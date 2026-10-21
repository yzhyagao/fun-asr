#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ASR Gateway —— FunASR 原生 WebSocket 服务 -> 流式转写接口（v2.2：全链路调试日志）

本版相对 v2.1 只增强日志，协议与行为不变。目的是回答一个问题：
    「PCM 音频块明明收到了，为什么没有变成文本？」

上游（FunASR 原生协议，默认 ws://fun-asr-nano:10096）
    1) 首帧必须是 JSON 握手:
       {mode, chunk_size, chunk_interval, encoder_chunk_look_back,
        decoder_chunk_look_back, audio_fs, wav_name, is_speaking, hotwords, itn}
    2) 之后持续发送二进制 PCM: 16000Hz / 16bit / 单声道
    3) 一句话结束时发送 {"is_speaking": false}
    4) 服务端返回 {"mode", "text", "wav_name", "is_final"}

日志按数据在链路里的流向分成五段，任何一环断了都能立刻看出来
------------------------------------------------------------------
  ① [连接] 新会话建立：来源 IP / 路由 / 事件风格 / wav_name
  ② [收]   收到 PCM 帧 #N：多少字节、≈多少 ms 音频、本句已收多久、
           电平 rms/peak、转发给上游的累计字节、
           接收≈N 帧/秒 + 推断采样率≈X Hz
        └ rms 接近 0 → 送进来的是静音
        └ 推断采样率不是 ≈16000 → 采样率不对（或推得比实时快，两种都出不了字）
  ③ [识别] 上游真的吐出了文本（只有 text 非空才打印）
        └ 这一环最关键：没这行 = 模型侧没出字
  ④ [下发] 把文本发给调用方（含本句累计文本）
        └ 有这行而你那边还收不到 = 你项目的事件名/字段没对上
  ⑤ [句尾] 一句话结束：最终文本 / 来源 online|offline / 是否兜底合成

四条告警（上面五段不全时先看这些）
    ⚠ 接收速率推断为 XHz（应≈16000）    → 采样率不对，或推得比实时快
    ⚠ 本句电平过低 (rms=...)             → 静音或增益太小
    ⚠ 已收 X s 音频但上游仍未返回任何文本 → 格式不对 / 是噪音 / 语种不匹配
    ⚠ 本句未识别到任何文本               → 这句确实没出字

对外协议（与 v2.1 完全一致）
    /v1/realtime、/v1/audio/stream   OpenAI Realtime 风格事件（默认）
    /v1/asr                          精简事件（asr.ready / asr.partial / asr.final）
"""

from __future__ import annotations

import array
import asyncio
import base64
import json
import logging
import math
import os
import time
import uuid
from contextlib import suppress
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState

LOGGER = logging.getLogger("asr-gateway")


# --------------------------------------------------------------------------- #
# 配置（全部可用环境变量覆盖）
# --------------------------------------------------------------------------- #
def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return default if raw is None or raw.strip() == "" else raw.strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        LOGGER.warning("环境变量 %s=%r 不是整数，回退默认值 %s", name, raw, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "y")


def _env_chunk_size(name: str, default: Tuple[int, int, int]) -> Tuple[int, int, int]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = json.loads(raw)
        if isinstance(value, (list, tuple)) and len(value) == 3:
            return (int(value[0]), int(value[1]), int(value[2]))
    except Exception:  # noqa: BLE001 - 配置容错
        pass
    LOGGER.warning("环境变量 %s=%r 不是长度 3 的 JSON 数组，回退默认值 %s", name, raw, default)
    return default


def _env_json_dict(name: str) -> Dict[str, Any]:
    """解析 JSON 对象型环境变量，失败时返回空字典。"""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return {}
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            return value
    except Exception:  # noqa: BLE001 - 配置容错
        pass
    LOGGER.warning("环境变量 %s=%r 不是 JSON 对象，已忽略", name, raw)
    return {}


def _parse_hotwords(raw: Optional[str]) -> Optional[Dict[str, int]]:
    """热词支持两种写法: '{"机器人":20,"导览":15}' 或 '机器人:20,导览:15'。"""
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): int(v) for k, v in parsed.items()}
    except Exception:  # noqa: BLE001 - 继续尝试 k:v 写法
        pass
    result: Dict[str, int] = {}
    for item in raw.split(","):
        if ":" in item:
            key, _, val = item.partition(":")
            key = key.strip()
            if not key:
                continue
            try:
                result[key] = int(val.strip())
            except ValueError:
                result[key] = 20
    return result or None


ASR_WS_URL = _env_str("ASR_WS_URL", "ws://fun-asr-nano:10096")
ASR_CONNECT_TIMEOUT = _env_int("ASR_CONNECT_TIMEOUT", 10)
ASR_RECV_TIMEOUT = _env_int("ASR_RECV_TIMEOUT", 30)
# commit 后的兜底计时：到点还没等到上游句尾包，就用聚合的 online 文本合成句尾结果。
# 0 = 关闭兜底，一直等上游（整段文件转写可设 0，实时对话建议 2000~3000）。
ASR_FINALIZE_TIMEOUT_MS = _env_int("ASR_FINALIZE_TIMEOUT_MS", 2500)
HEALTH_TIMEOUT = _env_int("HEALTH_TIMEOUT", 3)
LOG_LEVEL = _env_str("LOG_LEVEL", "INFO").upper()
# 每 N 帧打印一条 [收] 汇总（1 = 每帧都打）
PCM_STATS_EVERY = _env_int("ASR_PCM_STATS_EVERY", 100)
# 收了多少秒音频还没出一个字就告警（0 = 不告警）
NO_TEXT_WARN_SEC = _env_int("ASR_NO_TEXT_WARN_SEC", 5)
# 是否打印上游返回的空包（默认关，免得刷屏）
LOG_EMPTY = _env_bool("ASR_LOG_EMPTY", False)

# 对外事件风格：realtime = OpenAI Realtime 风格；simple = asr.partial / asr.final
STYLE_REALTIME = "realtime"
STYLE_SIMPLE = "simple"
_event_style = _env_str("ASR_EVENT_STYLE", STYLE_REALTIME).lower()
ASR_EVENT_STYLE = STYLE_SIMPLE if _event_style == STYLE_SIMPLE else STYLE_REALTIME

# FunASR 运行时要求：音频必须是 16000Hz / 16bit / 单声道
SUPPORTED_AUDIO_FS = (16000,)
BYTES_PER_SAMPLE = 2

# 上游握手附加字段。不同 FunASR 镜像字段不同，例如 SenseVoiceSmall-onnx 需要
#   ASR_EXTRA_CONFIG='{"svs_lang":"auto","svs_itn":true,"wav_format":"pcm"}'
# 同名时以下面的显式配置（ASR_MODE / ASR_CHUNK_SIZE / ASR_ITN / ...）为准；
# 运行期也可用 session.update 传任意附加字段。
ASR_EXTRA_CONFIG: Dict[str, Any] = _env_json_dict("ASR_EXTRA_CONFIG")

DEFAULT_SESSION: Dict[str, Any] = {
    "mode": _env_str("ASR_MODE", "2pass"),          # 2pass = 流式 + 离线修正
    "chunk_size": _env_chunk_size("ASR_CHUNK_SIZE", (5, 10, 5)),
    "chunk_interval": _env_int("ASR_CHUNK_INTERVAL", 10),
    "encoder_chunk_look_back": _env_int("ASR_ENCODER_CHUNK_LOOK_BACK", 4),
    "decoder_chunk_look_back": _env_int("ASR_DECODER_CHUNK_LOOK_BACK", 1),
    "audio_fs": _env_int("ASR_AUDIO_FS", 16000),
    "itn": _env_bool("ASR_ITN", True),              # 逆文本标准化（数字转汉字等）
    "hotwords": _parse_hotwords(os.getenv("ASR_HOTWORDS")),
    "extras": dict(ASR_EXTRA_CONFIG),
}

# session.update 允许覆盖的标准字段（其余字段一律透传到上游握手）
PATCHABLE_KEYS = (
    "mode",
    "chunk_size",
    "chunk_interval",
    "encoder_chunk_look_back",
    "decoder_chunk_look_back",
    "audio_fs",
    "itn",
    "hotwords",
    "wav_name",
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


def _event_names(style: str) -> Dict[str, str]:
    if style == STYLE_SIMPLE:
        return {
            "ready": "asr.ready",
            "delta": "asr.partial",
            "completed": "asr.final",
            "updated": "asr.session_updated",
        }
    return {
        "ready": "session.created",
        "delta": "conversation.item.input_audio_transcription.delta",
        "completed": "conversation.item.input_audio_transcription.completed",
        "updated": "session.updated",
    }


# --------------------------------------------------------------------------- #
# 上游 FunASR 连接
# --------------------------------------------------------------------------- #
class UpstreamError(RuntimeError):
    """上游（FunASR 容器）连接或协议异常。"""


class FunASRUpstream:
    """一条客户端连接独享一条上游连接（模型侧按会话维护 VAD 状态，不能复用）。"""

    def __init__(self, url: str, session: Dict[str, Any], wav_name: str) -> None:
        self.url = url
        self.session = session
        self.wav_name = wav_name
        self._ws: Optional[Any] = None
        self.audio_sent = False
        self.closed = False
        self.sent_bytes = 0

    # -- 生命周期 ---------------------------------------------------------- #
    async def connect(self) -> None:
        try:
            self._ws = await websockets.connect(
                self.url,
                open_timeout=ASR_CONNECT_TIMEOUT,
                close_timeout=5,
                max_size=None,
                ping_interval=20,
                ping_timeout=20,
            )
        except Exception as exc:  # noqa: BLE001
            raise UpstreamError(f"无法连接 ASR 后端 {self.url}: {exc}") from exc
        await self._ws.send(json.dumps(self.handshake(), ensure_ascii=False))
        LOGGER.info("[上行] 已连上游 %s (wav_name=%s)", self.url, self.wav_name)

    def handshake(self) -> Dict[str, Any]:
        cfg = self.session
        extras = cfg.get("extras")
        # 先铺附加字段（不同镜像的差异字段），再覆盖标准字段
        payload: Dict[str, Any] = dict(extras) if isinstance(extras, dict) else {}
        payload.update(
            {
                "mode": cfg.get("mode", "2pass"),
                "chunk_size": list(cfg.get("chunk_size", (5, 10, 5))),
                "chunk_interval": cfg.get("chunk_interval", 10),
                "encoder_chunk_look_back": cfg.get("encoder_chunk_look_back", 4),
                "decoder_chunk_look_back": cfg.get("decoder_chunk_look_back", 1),
                "audio_fs": cfg.get("audio_fs", 16000),
                "wav_name": cfg.get("wav_name", self.wav_name),
                "is_speaking": True,
            }
        )
        itn = cfg.get("itn")
        if itn is not None:
            payload["itn"] = bool(itn)
        hotwords = cfg.get("hotwords")
        if hotwords:
            # FunASR 要求 hotwords 是一个 JSON 字符串
            payload["hotwords"] = json.dumps(hotwords, ensure_ascii=False)
        return payload

    # -- 发送 -------------------------------------------------------------- #
    async def send_audio(self, data: bytes) -> None:
        if not data:
            return
        if len(data) % BYTES_PER_SAMPLE != 0:
            raise UpstreamError(
                f"音频帧长度 {len(data)} 不是 {BYTES_PER_SAMPLE} 字节的整数倍，"
                "请发送 16bit PCM（小端）"
            )
        await self._ws.send(data)
        self.sent_bytes += len(data)
        self.audio_sent = True

    async def end_utterance(self) -> None:
        """结束当前一句，触发上游离线修正并返回 is_final 结果。"""
        await self._ws.send(json.dumps({"is_speaking": False}))

    # -- 接收 -------------------------------------------------------------- #
    async def recv(self) -> Optional[Dict[str, Any]]:
        """返回上游 JSON 消息；无消息（超时）时返回 None。"""
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=ASR_RECV_TIMEOUT)
        except asyncio.TimeoutError:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            LOGGER.warning("上游返回非 JSON 消息，已忽略: %r", raw[:200])
            return {}
        if not isinstance(message, dict):
            return {}
        return message

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self._ws is not None:
            with suppress(Exception):
                await self._ws.close()


# --------------------------------------------------------------------------- #
# FastAPI 应用
# --------------------------------------------------------------------------- #
app = FastAPI(
    title="ASR Gateway (FunASR)",
    version="2.2.0",
    description="把 FunASR 的原生 WebSocket 服务包装成流式转写接口（OpenAI Realtime 风格 / 精简风格）",
)


def _error(error_type: str, message: str) -> Dict[str, Any]:
    return {"type": "error", "error": {"type": error_type, "message": message}}


async def _safe_send(ws: WebSocket, payload: Dict[str, Any]) -> None:
    if ws.client_state != WebSocketState.CONNECTED:
        return
    with suppress(Exception):
        await ws.send_text(json.dumps(payload, ensure_ascii=False))


def _parse_ws_target(url: str) -> Tuple[str, int]:
    parsed = urlparse(url)
    return parsed.hostname or "127.0.0.1", parsed.port or 80


async def _tcp_check(host: str, port: int, timeout: int = HEALTH_TIMEOUT) -> Tuple[bool, str]:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()
        return True, "reachable"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _frame_metrics(data: bytes) -> Tuple[float, int, int]:
    """返回 (rms, peak, 采样点数)，用于判断送进来的到底是不是静音。"""
    usable = (len(data) // BYTES_PER_SAMPLE) * BYTES_PER_SAMPLE
    if usable <= 0:
        return 0.0, 0, 0
    buf = array.array("h")
    buf.frombytes(data[:usable])
    step = 1 if len(buf) <= 64 else len(buf) // 64
    total = 0
    count = 0
    peak = 0
    for value in buf[::step]:
        total += value * value
        count += 1
        amp = value if value >= 0 else -value
        if amp > peak:
            peak = amp
    if count == 0:
        return 0.0, 0, len(buf)
    return math.sqrt(total / count), peak, len(buf)


class PcmStats:
    """本句音频的时长/电平统计，用于静音自检。"""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.frames = 0
        self.samples = 0
        self.peak = 0
        self._sum_sq = 0.0
        self._sum_n = 0
        self._t0 = time.monotonic()

    def add(self, data: bytes) -> None:
        rms, peak, samples = _frame_metrics(data)
        self.frames += 1
        self.samples += samples
        if samples:
            self._sum_sq += rms * rms
            self._sum_n += 1
        if peak > self.peak:
            self.peak = peak

    @property
    def avg_rms(self) -> float:
        if self._sum_n == 0:
            return 0.0
        return math.sqrt(self._sum_sq / self._sum_n)

    @property
    def seconds(self) -> float:
        return self.samples / 16000.0

    @property
    def fps(self) -> float:
        elapsed = time.monotonic() - self._t0
        return self.frames / elapsed if elapsed > 0 else 0.0

    @property
    def ms_per_frame(self) -> float:
        """按 16k/s16le 折算：每帧代表多少毫秒音频。"""
        if self.frames == 0:
            return 0.0
        return (self.samples / self.frames) / 16.0

    @property
    def est_hz(self) -> float:
        """按接收节奏反推对方音频的采样率。

        16k 实时流必然满足 「帧/秒 × 每帧采样数 ≈ 16000」。
        送 48k 音频时会算出 ≈48000；把文件推得比实时快时也会偏高。
        """
        if self.frames == 0:
            return 0.0
        return self.fps * (self.samples / self.frames)


@app.get("/")
async def index() -> Dict[str, Any]:
    return {
        "service": "asr-gateway",
        "version": "2.2.0",
        "upstream": ASR_WS_URL,
        "endpoints": {
            "ws_realtime": ["/v1/realtime", "/v1/audio/stream"],
            "ws_simple": ["/v1/asr"],
            "http": ["/health", "/"],
        },
        "event_style": ASR_EVENT_STYLE,
        "finalize_timeout_ms": ASR_FINALIZE_TIMEOUT_MS,
        "default_session": DEFAULT_SESSION,
        "audio_format": "PCM 16000Hz / 16bit / mono (little-endian)",
    }


@app.get("/health")
async def health(deep: int = 0) -> JSONResponse:
    host, port = _parse_ws_target(ASR_WS_URL)
    ok, detail = await _tcp_check(host, port)
    upstream: Dict[str, Any] = {"url": ASR_WS_URL, "tcp": detail}
    if ok and deep:
        probe = FunASRUpstream(ASR_WS_URL, dict(DEFAULT_SESSION), "health-probe")
        try:
            await probe.connect()
            upstream["handshake"] = "ok"
        except UpstreamError as exc:
            ok, upstream["handshake"] = False, str(exc)
        finally:
            await probe.close()
    body = {
        "status": "ok" if ok else "degraded",
        "upstream": upstream,
        "finalize_timeout_ms": ASR_FINALIZE_TIMEOUT_MS,
        "session": DEFAULT_SESSION,
    }
    return JSONResponse(status_code=200 if ok else 503, content=body)


def _coerce_session_value(key: str, value: Any) -> Tuple[bool, Any]:
    """把 session.update 的原始值规整成内部类型；返回 (是否可用, 规整后的值)。"""
    if key == "hotwords":
        if isinstance(value, str):
            return True, _parse_hotwords(value)
        if isinstance(value, dict):
            return True, {str(k): int(v) for k, v in value.items()}
        return False, None
    if key == "chunk_size":
        if not isinstance(value, (list, tuple)) or len(value) != 3:
            return False, None
        return True, tuple(int(x) for x in value)
    if key in (
        "chunk_interval",
        "encoder_chunk_look_back",
        "decoder_chunk_look_back",
        "audio_fs",
    ):
        try:
            return True, int(value)
        except (TypeError, ValueError):
            return False, None
    if key == "itn":
        return True, bool(value)
    if key in ("mode", "wav_name"):
        return True, str(value)
    return True, value


def _apply_session_patch(current: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    """把 session.update 合并进当前会话配置。

    标准字段（PATCHABLE_KEYS）按类型规整；其余字段原样透传到上游握手（extras）。
    返回被采纳的键值。
    """
    applied: Dict[str, Any] = {}
    for key, value in patch.items():
        if key in ("type", "event"):
            continue
        if key in PATCHABLE_KEYS:
            ok, coerced = _coerce_session_value(key, value)
            if not ok:
                continue
            current[key] = coerced
            applied[key] = coerced
        else:
            extras = current.get("extras")
            if not isinstance(extras, dict):
                extras = {}
                current["extras"] = extras
            extras[str(key)] = value
            applied[str(key)] = value
    return applied


def accumulate_segment(segments: List[str], incoming: str) -> str:
    """把上游 online 片段拼成整句文本。

    上游每个片段可能是「同一段滚动窗口的最新版本」（替换），
    也可能是「新一段」（追加）。用前缀/后缀关系做尽量保守的判断。
    """
    text = incoming or ""
    if not text.strip():
        return "".join(segments)
    if segments:
        last = segments[-1]
        if text == last:
            return "".join(segments)              # 完全重复，忽略
        if text.startswith(last):                 # 同一个滚动窗口变长了
            segments[-1] = text
        elif last.endswith(text):                 # 上游回吐了更短的同尾片段
            segments[-1] = text
        else:
            segments.append(text)
    else:
        segments.append(text)
    return "".join(segments)


# --------------------------------------------------------------------------- #
# WebSocket 主逻辑（两种事件风格共用）
# --------------------------------------------------------------------------- #
async def _serve(ws: WebSocket, style: str) -> None:
    await ws.accept()
    names = _event_names(style)
    client = ws.client
    remote = f"{getattr(client, 'host', '')}:{getattr(client, 'port', '')}"

    wav_name = (
        ws.query_params.get("wav_name")
        or ws.query_params.get("session")
        or f"gw-{uuid.uuid4().hex[:8]}"
    )
    session: Dict[str, Any] = dict(DEFAULT_SESSION)
    session["extras"] = dict(ASR_EXTRA_CONFIG)
    session["wav_name"] = wav_name
    # ?style=simple 可在 URL 上直接切风格
    if ws.query_params.get("style", "").lower() == STYLE_SIMPLE:
        style, names = STYLE_SIMPLE, _event_names(STYLE_SIMPLE)

    # 允许通过 query 快速覆盖：?hotwords={"机器人":20}&itn=0&svs_lang=auto
    query_patch: Dict[str, Any] = {}
    for key, raw in ws.query_params.items():
        if key in ("wav_name", "session", "style") or raw == "":
            continue
        query_patch[key] = raw
    if query_patch:
        if "itn" in query_patch:
            query_patch["itn"] = query_patch["itn"] not in ("0", "false", "False")
        _apply_session_patch(session, query_patch)

    LOGGER.info(
        "[连接] 新会话 remote=%s route=%s style=%s wav=%s 音频要求=16k/16bit/单声道",
        remote,
        ws.url.path,
        style,
        wav_name,
    )

    upstream = FunASRUpstream(ASR_WS_URL, session, wav_name)
    try:
        await upstream.connect()
    except UpstreamError as exc:
        LOGGER.error("%s", exc)
        await _safe_send(ws, _error("upstream_unreachable", str(exc)))
        with suppress(Exception):
            await ws.close(code=1011)
        return

    await _safe_send(
        ws,
        {
            "type": names["ready"],
            "session": {
                "id": wav_name,
                "model": "fun-asr",
                "upstream": ASR_WS_URL,
                "finalize_timeout_ms": ASR_FINALIZE_TIMEOUT_MS,
                "extras": dict(session.get("extras") or {}),
                **{k: v for k, v in session.items() if k not in ("wav_name", "extras")},
            },
        },
    )

    stop = asyncio.Event()
    stats = PcmStats()
    state: Dict[str, Any] = {
        "item_index": 0,
        "segments": [],                 # 当前句的 online 片段
        "awaiting_final": False,        # 已 commit，等上游句尾包
        "suppress_empty_final": False,  # 已合成过句尾结果，丢弃上游迟到的空包
        "finalize_task": None,
        "warned_level": False,          # 本句是否已报过"电平过低"
        "warned_no_text": False,        # 本句是否已报过"迟迟不出字"
        "warned_rate": False,           # 本句是否已报过"采样率不对"
        "got_text": False,              # 本句上游有没有出过字
    }

    async def emit_completed(
        text: str,
        mode: str,
        resolved_from: str,
        synthetic: bool,
        segments: List[str],
        metrics: Optional[Tuple[float, float]] = None,
    ) -> None:
        # 指标必须在 clear_utterance() 之前抓，否则 stats 已被重置
        duration, rms = metrics if metrics is not None else (stats.seconds, stats.avg_rms)
        state["item_index"] += 1
        if text:
            LOGGER.info(
                '[句尾] 本句完成 text="%s"（来源=%s, 合成=%s, 共%d段, 音频%.1fs, rms=%.0f）',
                text,
                resolved_from,
                synthetic,
                len(segments),
                duration,
                rms,
            )
        else:
            LOGGER.warning(
                "⚠ 本句未识别到任何文本（音频 %.1fs, rms=%.0f, 来源=%s）",
                duration,
                rms,
                resolved_from,
            )
        await _safe_send(
            ws,
            {
                "type": names["completed"],
                "transcript": text,
                "text": text,
                "is_final": True,
                "item_index": state["item_index"],
                "mode": mode,
                "resolved_from": resolved_from,
                "synthetic": synthetic,
                "segments": list(segments),
                "wav_name": session.get("wav_name", wav_name),
            },
        )
        LOGGER.info('[下发] → 调用方 text="%s"（类型=%s）', text, names["completed"])

    def clear_utterance() -> None:
        state["segments"] = []
        state["awaiting_final"] = False
        state["warned_level"] = False
        state["warned_no_text"] = False
        state["warned_rate"] = False
        state["got_text"] = False
        stats.reset()
        task = state.get("finalize_task")
        if task is not None and not task.done():
            task.cancel()
        state["finalize_task"] = None

    async def fire_finalize_after(ms: int) -> None:
        """commit 后兜底：到点仍没有上游句尾包，就用聚合文本合成一个。"""
        try:
            await asyncio.sleep(ms / 1000.0)
        except asyncio.CancelledError:
            return
        if not state["awaiting_final"] or stop.is_set():
            return
        text = "".join(state["segments"])
        segments = list(state["segments"])
        metrics = (stats.seconds, stats.avg_rms)
        clear_utterance()
        state["suppress_empty_final"] = True
        LOGGER.info("句尾兜底触发（%dms 内上游没给句尾包）", ms)
        await emit_completed(
            text, "gateway-fallback", "online" if text else "none", True, segments, metrics
        )

    def schedule_finalize(ms: int) -> None:
        state["awaiting_final"] = True
        state["suppress_empty_final"] = False
        old = state.get("finalize_task")
        if old is not None and not old.done():
            old.cancel()
        if ms <= 0:
            state["finalize_task"] = None
            return
        state["finalize_task"] = asyncio.create_task(fire_finalize_after(ms))

    # ---- 上游 -> 客户端 --------------------------------------------------- #
    async def emit_upstream_events() -> None:
        nonlocal upstream
        current = upstream
        while not stop.is_set():
            try:
                message = await current.recv()
            except Exception as exc:  # noqa: BLE001 - 上游断开
                if stop.is_set():
                    return
                if current is not upstream:  # 被 session.update 换掉了，重挂新连接
                    current = upstream
                    continue
                LOGGER.warning("上游连接中断: %s", exc)
                await _safe_send(ws, _error("upstream_closed", f"上游连接中断: {exc}"))
                stop.set()
                return
            if message is None:
                if stop.is_set():
                    return
                if current is not upstream:
                    current = upstream
                    continue
                # recv 超时：不主动断开，继续等下一条（长会话里允许静默）
                continue
            if not message:
                continue

            text = message.get("text", "") or ""
            mode = message.get("mode", "") or ""
            is_final = bool(message.get("is_final"))
            wav = message.get("wav_name", session.get("wav_name", wav_name))

            # 空包默认不打印（上游 2pass-offline 的句尾包 text 恒为空，很容易刷屏）
            if not text.strip():
                if LOG_EMPTY:
                    LOGGER.info("[上游] 空包 (mode=%s, is_final=%s)", mode, is_final)
            else:
                state["got_text"] = True
                state["warned_no_text"] = False
                LOGGER.info(
                    '[识别] 识别成功 text="%s"（mode=%s, is_final=%s, wav=%s）',
                    text,
                    mode,
                    is_final,
                    wav,
                )

            if is_final:
                offline_text = text.strip()
                segments = list(state["segments"])
                accumulated = "".join(segments)
                # 已合成过句尾结果：上游迟到的空包直接丢弃，避免重复 completed
                if state["suppress_empty_final"] and not offline_text:
                    LOGGER.info("丢弃上游迟到的空句尾包 (mode=%s)", mode)
                    clear_utterance()
                    state["suppress_empty_final"] = False
                    continue
                final_text = offline_text or accumulated
                resolved = "offline" if offline_text else ("online" if accumulated else "none")
                metrics = (stats.seconds, stats.avg_rms)
                clear_utterance()
                state["suppress_empty_final"] = False
                await emit_completed(final_text, mode, resolved, False, segments, metrics)
                continue

            utterance_text = accumulate_segment(state["segments"], text)
            if text.strip():
                await _safe_send(
                    ws,
                    {
                        "type": names["delta"],
                        "delta": text,
                        "text": utterance_text,
                        "utterance_text": utterance_text,
                        "is_final": False,
                        "item_index": state["item_index"] + 1,
                        "mode": mode,
                        "wav_name": wav,
                    },
                )
                LOGGER.info(
                    '[下发] → 调用方 本句累计="%s"（类型=%s）',
                    utterance_text,
                    names["delta"],
                )

    # ---- 客户端 -> 上游 --------------------------------------------------- #
    async def handle_audio(data: bytes) -> None:
        stats.add(data)
        # 先转发再打印，这样"上游累计字节"是准的
        try:
            await upstream.send_audio(data)
        except UpstreamError as exc:
            LOGGER.warning("转发 PCM 失败：%s", exc)
            await _safe_send(ws, _error("invalid_audio", str(exc)))
            return
        if stats.frames == 1 or stats.frames % PCM_STATS_EVERY == 0:
            cadence = (
                f"，接收≈{stats.fps:.0f}帧/秒，推断采样率≈{stats.est_hz:.0f}Hz"
                if stats.frames >= 25
                else ""
            )
            LOGGER.info(
                "[收] 收到 PCM 帧 #%d：%d 字节（≈%.0fms 音频/帧%s）"
                "，本句已收 %.2fs，电平 rms=%.0f peak=%.0f → [转发] 上游累计 %d 字节",
                stats.frames,
                len(data),
                stats.ms_per_frame,
                cadence,
                stats.seconds,
                stats.avg_rms,
                stats.peak,
                upstream.sent_bytes,
            )
        if stats.frames == 50:
            if stats.avg_rms < 8 and not state["warned_level"]:
                state["warned_level"] = True
                LOGGER.warning(
                    "⚠ 本句电平过低（rms=%.0f）：请确认是 16k/16bit/单声道 PCM，且不是静音",
                    stats.avg_rms,
                )
            if not state["got_text"] and NO_TEXT_WARN_SEC > 0:
                state["warned_no_text"] = True
                LOGGER.warning(
                    "⚠ 已收 %.1fs 音频但上游仍未返回任何文本："
                    "请确认 16000Hz / 单声道 / 小端 16bit（当前 rms=%.0f）",
                    stats.seconds,
                    stats.avg_rms,
                )
            # 采样率自检：16k 实时流应满足 帧/秒 × 每帧采样数 ≈ 16000
            if stats.est_hz > 0 and abs(stats.est_hz - 16000) > 1600 and not state["warned_rate"]:
                state["warned_rate"] = True
                LOGGER.warning(
                    "⚠ 接收速率推断为 %dHz（16k 实时流应≈16000）："
                    "要么音频采样率不是 16000，要么你在把音频推得比实时快 —— "
                    "两者都会让模型吐不出字，请先把音频重采样到 16k 再按实时节奏发送",
                    stats.est_hz,
                )

    async def handle_text_frame(raw_text: str) -> None:
        nonlocal upstream
        text = raw_text.strip()
        if text.upper() in ("EOS", "END"):
            await upstream.end_utterance()
            schedule_finalize(ASR_FINALIZE_TIMEOUT_MS)
            return
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            await _safe_send(ws, _error("invalid_message", "文本帧既不是 EOS 也不是合法 JSON"))
            return
        if not isinstance(data, dict):
            await _safe_send(ws, _error("invalid_message", "JSON 文本帧必须是对象"))
            return

        mtype = data.get("type") or data.get("event")

        if mtype == "input_audio_buffer.append":
            audio_b64 = data.get("audio")
            if not audio_b64:
                await _safe_send(ws, _error("invalid_message", "input_audio_buffer.append 缺少 audio"))
                return
            try:
                audio = base64.b64decode(audio_b64)
            except Exception as exc:  # noqa: BLE001
                await _safe_send(ws, _error("invalid_message", f"audio base64 解码失败: {exc}"))
                return
            await handle_audio(audio)
            return

        if mtype in ("input_audio_buffer.commit", "eos", "input_audio_buffer.end"):
            await upstream.end_utterance()
            try:
                timeout_ms = int(data.get("finalize_timeout_ms", ASR_FINALIZE_TIMEOUT_MS))
            except (TypeError, ValueError):
                timeout_ms = ASR_FINALIZE_TIMEOUT_MS
            LOGGER.info(
                "[收] 收到 commit：本句 %d 帧 / %.2fs / rms=%.0f / peak=%.0f，等上游 %dms",
                stats.frames,
                stats.seconds,
                stats.avg_rms,
                stats.peak,
                timeout_ms,
            )
            if not state["got_text"]:
                LOGGER.warning(
                    "⚠ 这一句上游一个字都没返回（音频 %.1fs, rms=%.0f）",
                    stats.seconds,
                    stats.avg_rms,
                )
            schedule_finalize(timeout_ms)
            return

        if mtype == "session.update":
            patch = data.get("session")
            if not isinstance(patch, dict):
                patch = {k: v for k, v in data.items() if k not in ("type", "event")}
            if upstream.audio_sent:
                await _safe_send(
                    ws,
                    _error("session_locked", "session.update 必须在发送音频之前调用；本次已忽略"),
                )
                return
            applied = _apply_session_patch(session, patch)
            new_fs = session.get("audio_fs")
            if new_fs not in SUPPORTED_AUDIO_FS:
                LOGGER.warning("audio_fs=%s 不是 FunASR 支持的 %s", new_fs, SUPPORTED_AUDIO_FS)
            # 重新握手：关旧连接，按新配置建一条
            old = upstream
            new_upstream = FunASRUpstream(ASR_WS_URL, session, session.get("wav_name", wav_name))
            try:
                await new_upstream.connect()
            except UpstreamError as exc:
                await _safe_send(ws, _error("upstream_unreachable", str(exc)))
                return
            upstream = new_upstream
            await old.close()
            await _safe_send(ws, {"type": names["updated"], "session": applied})
            return

        if mtype in ("session.close", "input_audio_buffer.close"):
            stop.set()
            return

        await _safe_send(ws, _error("unsupported_type", f"不支持的消息类型: {mtype!r}"))

    async def pump_client() -> None:
        try:
            while not stop.is_set():
                message = await ws.receive()
                if message.get("type") == "websocket.disconnect":
                    break
                payload_bytes = message.get("bytes")
                payload_text = message.get("text")
                if payload_bytes is not None:
                    await handle_audio(payload_bytes)
                elif payload_text is not None:
                    await handle_text_frame(payload_text)
        except Exception as exc:  # noqa: BLE001 - 客户端异常断开
            LOGGER.info("客户端读循环结束: %s", exc)
        finally:
            stop.set()

    tasks = [asyncio.create_task(emit_upstream_events()), asyncio.create_task(pump_client())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop.set()
        task = state.get("finalize_task")
        if task is not None and not task.done():
            task.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await upstream.close()
        if ws.client_state == WebSocketState.CONNECTED:
            with suppress(Exception):
                await ws.close()
        LOGGER.info("会话结束: %s（共完成 %d 句）", wav_name, state["item_index"])


@app.websocket("/v1/realtime")
@app.websocket("/v1/audio/stream")
async def realtime(ws: WebSocket) -> None:
    await _serve(ws, ASR_EVENT_STYLE)


@app.websocket("/v1/asr")
async def simple(ws: WebSocket) -> None:
    await _serve(ws, STYLE_SIMPLE)


if __name__ == "__main__":  # 便于本地直跑: python app.py
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=_env_int("PORT", 8000),
        log_level=LOG_LEVEL.lower(),
    )

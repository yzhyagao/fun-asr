#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ASR Gateway 测试客户端：把一个 wav 文件推给网关，打印 partial / final 文本。

用法（在 WSL 里执行）：
    pip install websockets
    python ws_test_client.py --wav test.wav
    python ws_test_client.py --wav test.wav --url ws://127.0.0.1:10097/v1/realtime --realtime
    python ws_test_client.py --wav test.wav --hotwords '{"机器人":20}' --text-file out.txt

说明：
    · wav 会被自动转成 16kHz / 16bit / 单声道（优先用 audioop，缺失时用纯 Python 回退）
    · --realtime 会按音频真实时长发送（模拟真人说话）；不加则尽快灌完，用于压测
"""

from __future__ import annotations

import argparse
import array
import asyncio
import json
import os
import sys
import time
import wave
from typing import Any, Dict, List, Optional, Tuple

import websockets

try:
    import audioop  # Python <= 3.12
except ImportError:  # Python 3.13 起 audioop 被移除
    audioop = None

TARGET_FS = 16000
SAMPLE_WIDTH = 2


def _convert_fallback(raw: bytes, channels: int, rate: int) -> bytes:
    """纯 Python 回退：多声道求均值下混 + 线性插值重采样（x86 上 array('h') 即小端）。"""
    samples = array.array("h")
    samples.frombytes(raw[: len(raw) // SAMPLE_WIDTH * SAMPLE_WIDTH])

    if channels > 1:
        frames = len(samples) // channels
        mono = array.array("h", bytes(SAMPLE_WIDTH * frames))
        for i in range(frames):
            window = samples[i * channels : (i + 1) * channels]
            mono[i] = int(sum(window) / channels)
        samples = mono

    if rate != TARGET_FS:
        ratio = TARGET_FS / float(rate)
        out_len = int(len(samples) * ratio)
        resampled = array.array("h", bytes(SAMPLE_WIDTH * out_len))
        last = len(samples) - 1
        for i in range(out_len):
            pos = i / ratio
            i0 = int(pos)
            frac = pos - i0
            i1 = i0 + 1 if i0 < last else last
            resampled[i] = int(samples[i0] * (1 - frac) + samples[i1] * frac)
        samples = resampled

    return samples.tobytes()


def load_pcm16k_mono(path: str) -> Tuple[bytes, float]:
    """读 wav 并转成 16kHz / 16bit / 单声道 小端 PCM，返回 (数据, 秒数)。"""
    with wave.open(path, "rb") as fp:
        channels = fp.getnchannels()
        width = fp.getsampwidth()
        rate = fp.getframerate()
        raw = fp.readframes(fp.getnframes())

    if width != SAMPLE_WIDTH:
        raise SystemExit(f"只支持 16bit PCM wav，当前为 {width * 8}bit，请先用 ffmpeg 转换")

    if channels == 2 and audioop is not None:
        raw = audioop.tomono(raw, width, 0.5, 0.5)
    elif rate != TARGET_FS and audioop is not None:
        raw, _ = audioop.ratecv(raw, width, 1, rate, TARGET_FS, None)
    elif channels != 1 or rate != TARGET_FS:
        raw = _convert_fallback(raw, channels, rate)
    elif channels != 1:
        raise SystemExit(f"不支持 {channels} 声道音频")

    seconds = len(raw) / float(SAMPLE_WIDTH * TARGET_FS)
    return raw, seconds


class Transcript:
    def __init__(self) -> None:
        self.partial = ""
        self.finals: List[str] = []

    @property
    def full_text(self) -> str:
        return "".join(self.finals) or self.partial


def _suppress_close(exc_type, exc, tb) -> bool:
    return True


async def run(args: argparse.Namespace) -> int:
    pcm, seconds = load_pcm16k_mono(args.wav)
    chunk_bytes = max(SAMPLE_WIDTH, int(TARGET_FS * SAMPLE_WIDTH * args.chunk_ms / 1000))
    total = len(pcm)
    print(f"[client] {args.wav}: {seconds:.2f}s, {total} bytes, 分块 {args.chunk_ms}ms/{chunk_bytes}B")

    transcript = Transcript()
    done = asyncio.Event()

    url = args.url
    for key, value in (("hotwords", args.hotwords), ("itn", None if args.itn is None else (1 if args.itn else 0))):
        if value is None:
            continue
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{key}={value}"

    async with websockets.connect(url, max_size=None, ping_interval=None) as ws:

        async def reader() -> None:
            try:
                async for raw in ws:
                    if isinstance(raw, bytes):
                        continue
                    print(f"[event] {raw}")
                    try:
                        msg: Dict[str, Any] = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    mtype = msg.get("type") or ""
                    if mtype.endswith("transcription.delta"):
                        transcript.partial = msg.get("text", "")
                    elif mtype.endswith("transcription.completed"):
                        text = msg.get("text") or msg.get("transcript", "")
                        transcript.finals.append(text)
                        transcript.partial = ""
                        if not args.keep_alive:
                            done.set()
                    elif mtype == "error":
                        print(f"[client] 服务端错误: {msg.get('error')}", file=sys.stderr)
                        done.set()
            except Exception as exc:  # noqa: BLE001
                if not done.is_set():
                    print(f"[client] 连接结束: {exc}", file=sys.stderr)
                done.set()

        reader_task = asyncio.create_task(reader())
        await asyncio.sleep(0.05)  # 等 session.created 先到

        started = time.monotonic()
        for offset in range(0, total, chunk_bytes):
            await ws.send(pcm[offset : offset + chunk_bytes])
            if args.realtime:
                target = (offset + chunk_bytes) / float(TARGET_FS * SAMPLE_WIDTH)
                delay = target - (time.monotonic() - started)
                if delay > 0:
                    await asyncio.sleep(delay)
        print(f"[client] 音频发送完毕，耗时 {time.monotonic() - started:.2f}s，发送 commit")

        await ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
        try:
            await asyncio.wait_for(done.wait(), timeout=args.timeout)
        except asyncio.TimeoutError:
            print("[client] 等待最终结果超时", file=sys.stderr)

        try:
            await ws.send(json.dumps({"type": "session.close"}))
        except Exception:  # noqa: BLE001
            pass
        reader_task.cancel()
        await asyncio.gather(reader_task, return_exceptions=True)

    print("-" * 60)
    print(f"[结果] {transcript.full_text}")
    if args.text_file and transcript.full_text:
        with open(args.text_file, "w", encoding="utf-8") as fp:
            fp.write(transcript.full_text)
        print(f"[client] 已写入 {args.text_file}")
    return 0 if transcript.full_text else 2


def main() -> None:
    parser = argparse.ArgumentParser(description="ASR Gateway 流式转写测试客户端")
    parser.add_argument("--wav", required=True, help="输入 wav 文件路径")
    parser.add_argument(
        "--url",
        default=os.getenv("ASR_GATEWAY_URL", "ws://127.0.0.1:10097/v1/realtime"),
        help="网关 WebSocket 地址，默认 ws://127.0.0.1:10097/v1/realtime",
    )
    parser.add_argument("--chunk-ms", type=int, default=100, help="每帧音频时长（毫秒），默认 100")
    parser.add_argument("--realtime", action="store_true", help="按音频真实时长发送，模拟真人说话")
    parser.add_argument("--timeout", type=float, default=15.0, help="等待最终结果秒数，默认 15")
    parser.add_argument("--hotwords", default=None, help='热词，如 \'{"机器人":20,"导览":15}\'')
    parser.add_argument("--itn", dest="itn", action="store_true", default=None, help="开启逆文本标准化")
    parser.add_argument("--no-itn", dest="itn", action="store_false", help="关闭逆文本标准化")
    parser.add_argument("--keep-alive", action="store_true", help="收到第一句最终结果后不退出，继续录")
    parser.add_argument("--text-file", default=None, help="把识别结果写入文件")
    args = parser.parse_args()

    if not os.path.isfile(args.wav):
        raise SystemExit(f"找不到音频文件: {args.wav}")
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()

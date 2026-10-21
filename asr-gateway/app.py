"""Thin gateway: Orin PCM in, FunASR text out.

Orin connects to /v1/realtime and waits for session.created before it sends
audio. Binary frames are 16 kHz mono s16le PCM. A text frame
{"type":"input_audio_buffer.commit"} closes the current utterance.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any
from uuid import uuid4

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
from websockets.exceptions import ConnectionClosed

LOGGER = logging.getLogger("asr-gateway")

ASR_WS_URL = os.environ.get("ASR_WS_URL", "ws://127.0.0.1:10096").strip()
ASR_MODE = os.environ.get("ASR_MODE", "2pass").strip() or "2pass"
ASR_AUDIO_FS = int(os.environ.get("ASR_AUDIO_FS", "16000"))
ASR_ITN = os.environ.get("ASR_ITN", "true").strip().lower() in {"1", "true", "yes"}
FINALIZE_TIMEOUT_S = int(os.environ.get("ASR_FINALIZE_TIMEOUT_MS", "2500")) / 1000
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def _chunk_size() -> list[int]:
    raw = os.environ.get("ASR_CHUNK_SIZE", "[5,10,5]").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return [5, 10, 5]
    if (
        isinstance(parsed, list)
        and len(parsed) == 3
        and all(isinstance(item, int) for item in parsed)
    ):
        return parsed
    return [5, 10, 5]


CHUNK_SIZE = _chunk_size()
CHUNK_INTERVAL = 10
# FunASR online stride: 60ms * chunk_size[1] / chunk_interval.
# [5,10,5] at 16 kHz s16le is 1920 bytes. 10ms frames make the online pass hallucinate.
STRIDE = int(60 * CHUNK_SIZE[1] / CHUNK_INTERVAL / 1000 * ASR_AUDIO_FS * 2)
STRIDE -= STRIDE % 2
_TAG = re.compile(r"<\|[^|]+\|>")


def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _handshake(wav_name: str, *, speaking: bool) -> str:
    return json.dumps(
        {
            "mode": ASR_MODE,
            "chunk_size": CHUNK_SIZE,
            "chunk_interval": 10,
            "encoder_chunk_look_back": 4,
            "decoder_chunk_look_back": 0,
            "audio_fs": ASR_AUDIO_FS,
            "wav_name": wav_name,
            "wav_format": "pcm",
            "is_speaking": speaking,
            "itn": ASR_ITN,
            "svs_lang": "auto",
            "svs_itn": ASR_ITN,
        },
        ensure_ascii=False,
    )


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        value = "" if value is None else str(value)
    return _TAG.sub("", value).strip()


def _classify(message: dict[str, Any]) -> tuple[str, str, bool]:
    """Return text, mode, and whether this message ends the utterance.

    An online message with is_final and no text is only an end flag. Closing
    the socket on it drops the offline correction that follows.
    """
    mode = str(message.get("mode") or "")
    text = _clean_text(message.get("text"))
    offline = mode.endswith("offline")
    ended = offline or (bool(message.get("is_final")) and bool(text) and not mode.endswith("online"))
    return text, mode, ended


class FunAsrUpstream:
    """One FunASR socket. A new utterance is a new socket."""

    def __init__(self, wav_name: str) -> None:
        self.wav_name = wav_name
        self.ws: Any = None
        self.generation = 0

    async def open_utterance(self) -> None:
        await self.close()
        self.generation += 1
        self.ws = await websockets.connect(
            ASR_WS_URL,
            subprotocols=["binary"],
            max_size=None,
            ping_interval=20,
            open_timeout=10,
        )
        await self.ws.send(_handshake(self.wav_name, speaking=True))

    async def send_pcm(self, pcm: bytes) -> None:
        if self.ws is None:
            raise ConnectionClosed(None, None) from None
        if len(pcm) % 2 != 0:
            pcm = pcm[: len(pcm) - 1]
        if not pcm:
            return
        await self.ws.send(pcm)

    async def finish_utterance(self) -> None:
        if self.ws is None:
            return
        await self.ws.send(json.dumps({"is_speaking": False}))

    async def recv(self) -> dict[str, Any] | None:
        if self.ws is None:
            raise ConnectionClosed(None, None)
        raw = await self.ws.recv()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            LOGGER.warning("upstream non-json chars=%s", len(raw))
            return None
        if not isinstance(message, dict):
            return None
        return message

    async def close(self) -> None:
        ws = self.ws
        self.ws = None
        if ws is None:
            return
        try:
            await ws.close()
        except Exception:
            LOGGER.debug("upstream close failed", exc_info=True)


app = FastAPI(title="ASR Gateway")


@app.get("/health")
async def health(deep: int = 0) -> JSONResponse:
    if not deep:
        return JSONResponse({"ok": True, "upstream": ASR_WS_URL})
    probe = FunAsrUpstream(f"health-{uuid4().hex[:8]}")
    try:
        await probe.open_utterance()
    except Exception as exc:
        LOGGER.warning("health deep failed: %s", exc)
        return JSONResponse(
            {"ok": False, "upstream": ASR_WS_URL, "error": type(exc).__name__},
            status_code=503,
        )
    finally:
        await probe.close()
    return JSONResponse({"ok": True, "upstream": ASR_WS_URL, "deep": True})


@app.websocket("/v1/realtime")
async def realtime(ws: WebSocket) -> None:
    await ws.accept()
    client = ws.client
    session_id = f"gw-{uuid4().hex[:8]}"
    LOGGER.info(
        "client connected host=%s port=%s session=%s",
        getattr(client, "host", ""),
        getattr(client, "port", ""),
        session_id,
    )
    upstream = FunAsrUpstream(session_id)
    try:
        await upstream.open_utterance()
    except Exception as exc:
        LOGGER.warning("upstream connect failed: %s", exc)
        await ws.send_json(
            {
                "type": "error",
                "error": {"type": "upstream_unreachable", "message": str(exc)},
            }
        )
        await ws.close(code=1011)
        return

    await ws.send_json(
        {
            "type": "session.created",
            "session": {
                "id": session_id,
                "model": "fun-asr-nano",
                "upstream": ASR_WS_URL,
            },
        }
    )
    LOGGER.info(
        "session.created sent session=%s upstream=%s stride=%s",
        session_id,
        ASR_WS_URL,
        STRIDE,
    )

    stop = asyncio.Event()
    audio_lock = asyncio.Lock()
    item_index = 1
    pcm_frames = 0
    pcm_bytes = 0
    # PCM waiting to fill one FunASR stride, including audio that arrived
    # while the upstream utterance was being reopened.
    pcm_buf = bytearray()
    last_partial = ""
    utterance_open = True

    async def emit(payload: dict[str, Any]) -> None:
        await ws.send_json(payload)

    async def reopen() -> None:
        nonlocal utterance_open
        try:
            await upstream.open_utterance()
        except Exception as exc:
            LOGGER.warning("upstream reopen failed session=%s error=%s", session_id, exc)
            stop.set()
            return
        async with audio_lock:
            utterance_open = True
        await flush_stride(force=False)

    async def read_upstream() -> None:
        nonlocal item_index, utterance_open, last_partial
        while not stop.is_set():
            try:
                message = await upstream.recv()
            except ConnectionClosed:
                if stop.is_set():
                    return
                LOGGER.info(
                    "upstream closed session=%s open=%s",
                    session_id,
                    utterance_open,
                )
                utterance_open = False
                await reopen()
                continue
            except Exception as exc:
                if stop.is_set():
                    return
                LOGGER.warning("upstream recv failed session=%s error=%s", session_id, exc)
                utterance_open = False
                await reopen()
                continue
            if not message:
                continue
            text, mode, ended = _classify(message)
            if ended:
                utterance_open = False
                if text.strip():
                    LOGGER.info(
                        "final session=%s item=%s text=%s",
                        session_id,
                        item_index,
                        text.strip(),
                    )
                    await emit(
                        {
                            "type": "conversation.item.input_audio_transcription.completed",
                            "text": text.strip(),
                            "transcript": text.strip(),
                            "item_index": item_index,
                        }
                    )
                else:
                    LOGGER.info(
                        "final empty session=%s item=%s mode=%s is_final=%s",
                        session_id,
                        item_index,
                        mode,
                        message.get("is_final"),
                    )
                last_partial = ""
                item_index += 1
                await upstream.close()
                if not stop.is_set():
                    await reopen()
                continue
            if text.strip():
                last_partial = text.strip()
                LOGGER.info(
                    "partial session=%s item=%s mode=%s text=%s",
                    session_id,
                    item_index,
                    mode,
                    last_partial,
                )
                await emit(
                    {
                        "type": "conversation.item.input_audio_transcription.delta",
                        "text": text.strip(),
                        "item_index": item_index,
                    }
                )

    async def flush_stride(*, force: bool) -> None:
        nonlocal utterance_open
        while not stop.is_set():
            async with audio_lock:
                if not utterance_open or upstream.ws is None:
                    return
                ready = len(pcm_buf) >= STRIDE
                if not ready and not force:
                    return
                if not pcm_buf:
                    return
                take = STRIDE if ready else len(pcm_buf)
                chunk = bytes(pcm_buf[:take])
                del pcm_buf[:take]
            try:
                await upstream.send_pcm(chunk)
            except Exception:
                LOGGER.warning("pcm forward failed session=%s", session_id)
                async with audio_lock:
                    pcm_buf[:0] = chunk
                    utterance_open = False
                await upstream.close()
                return
            if take < STRIDE:
                return

    async def read_client() -> None:
        nonlocal pcm_frames, pcm_bytes, utterance_open
        while not stop.is_set():
            message = await ws.receive()
            if message.get("type") == "websocket.disconnect":
                break
            payload = message.get("bytes")
            text = message.get("text")
            if payload is not None:
                pcm_frames += 1
                pcm_bytes += len(payload)
                if pcm_frames <= 3 or pcm_frames % 25 == 0:
                    LOGGER.info(
                        "pcm session=%s frame=%s bytes=%s total=%s",
                        session_id,
                        pcm_frames,
                        len(payload),
                        pcm_bytes,
                    )
                async with audio_lock:
                    pcm_buf.extend(payload)
                await flush_stride(force=False)
                continue
            if text and "input_audio_buffer.commit" in text:
                LOGGER.info(
                    "commit session=%s frames=%s total_bytes=%s",
                    session_id,
                    pcm_frames,
                    pcm_bytes,
                )
                if utterance_open and upstream.ws is not None:
                    generation = upstream.generation
                    await flush_stride(force=True)
                    try:
                        await upstream.finish_utterance()
                    except Exception:
                        LOGGER.warning("commit send failed session=%s", session_id)
                        utterance_open = False
                        await upstream.close()
                        continue

                    async def commit_timeout(expected: int) -> None:
                        await asyncio.sleep(FINALIZE_TIMEOUT_S)
                        if stop.is_set() or upstream.generation != expected:
                            return
                        LOGGER.warning(
                            "commit timeout session=%s item=%s",
                            session_id,
                            item_index,
                        )
                        await upstream.close()

                    asyncio.create_task(commit_timeout(generation))
                continue

    reader = asyncio.create_task(read_upstream())
    try:
        await read_client()
    except Exception as exc:
        LOGGER.info("client loop ended session=%s error=%s", session_id, exc)
    finally:
        stop.set()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        await upstream.close()
        LOGGER.info(
            "session closed session=%s frames=%s total_bytes=%s",
            session_id,
            pcm_frames,
            pcm_bytes,
        )


_configure_logging()

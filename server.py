#!/usr/bin/env python3
"""Qwen3.8-Omni-Flash-Realtime 网页语音服务。

浏览器 <-> 本服务 (WebSocket) <-> DashScope Realtime (WebSocket)

API Key 只保存在服务端，浏览器只负责采集/播放 PCM。
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("omni-voice")

# 运行期计数，方便排查「模型到底有没有下发音频」
STATS = {
    "sessions": 0,
    "audioPackets": 0,
    "audioBytes": 0,
    "speechStarted": 0,
    "upstreamErrors": 0,
    "lastError": "",
}
STARTED_AT = time.monotonic()


def load_dotenv(path: Path) -> None:
    """极简 .env 读取，避免额外依赖。已存在的环境变量优先。"""
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_dotenv(BASE_DIR / ".env")

API_KEY = os.getenv("DASHSCOPE_API_KEY", "").strip()
WORKSPACE_ID = os.getenv("DASHSCOPE_WORKSPACE_ID", "").strip()
REGION = os.getenv("DASHSCOPE_REGION", "beijing").strip().lower()
MODEL = os.getenv("OMNI_MODEL", "qwen3.8-omni-flash-realtime").strip()
VOICE = os.getenv("OMNI_VOICE", "Tina").strip()
INSTRUCTIONS = os.getenv(
    "OMNI_INSTRUCTIONS",
    "你是个人助理小云，正在通过实时语音和用户聊天。"
    "你的回答会被合成为语音播放，所以直接说话，不要说自己只能打字或不能语音。"
    "回答简短、自然、口语化，不要用 Markdown、列表或表情符号。",
).strip()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        log.warning("环境变量 %s=%r 不是整数，使用默认值 %s", name, raw, default)
        return default


SILENCE_MS = env_int("OMNI_SILENCE_MS", 500)
VAD_TYPE = os.getenv("OMNI_VAD", "server_vad").strip()

# 识别 .env.example 里的占位符，避免“看起来配置了”其实连不上
API_KEY_CONFIGURED = bool(API_KEY) and not API_KEY.startswith("sk-xxxx")
WORKSPACE_CONFIGURED = bool(WORKSPACE_ID) and WORKSPACE_ID != "xxxxxxxxxxxx"

REGION_HOSTS = {
    "beijing": "cn-beijing.maas.aliyuncs.com",
    "singapore": "ap-southeast-1.maas.aliyuncs.com",
}

app = FastAPI(title="Qwen Omni Voice", docs_url=None, redoc_url=None)


def upstream_url() -> str:
    """构造 DashScope WebSocket 地址。qwen3.8 必须使用业务空间专属域名。"""
    override = os.getenv("DASHSCOPE_WS_URL", "").strip()
    if override:
        base = override
    else:
        if not WORKSPACE_CONFIGURED:
            raise RuntimeError(
                "缺少 DASHSCOPE_WORKSPACE_ID（或在 .env 里设置完整的 DASHSCOPE_WS_URL）",
            )
        host = REGION_HOSTS.get(REGION)
        if not host:
            raise RuntimeError(
                f"未知地域 DASHSCOPE_REGION={REGION!r}，可选 beijing / singapore",
            )
        base = f"wss://{WORKSPACE_ID}.{host}/api-ws/v1/realtime"
    sep = "&" if "?" in base else "?"
    if "model=" in base:
        return base
    return f"{base}{sep}model={MODEL}"


def session_update() -> dict:
    """首轮会话配置。输出固定 24k PCM，输入固定 16k PCM。"""
    return {
        "type": "session.update",
        "session": {
            "modalities": ["text", "audio"],
            "voice": VOICE,
            "instructions": INSTRUCTIONS,
            "input_audio_transcription": {"model": "qwen3-asr-flash-realtime"},
            "turn_detection": {
                "type": VAD_TYPE,
                "threshold": 0.5,
                "prefix_padding_ms": 300,
                "silence_duration_ms": SILENCE_MS,
            },
            "audio": {
                "input": {"format": {"type": "pcm", "sample_rate": 16000}},
                "output": {
                    "format": {"type": "pcm", "sample_rate": 24000},
                    "voice": VOICE,
                },
            },
        },
    }


async def connect_upstream(url: str):
    headers = {"Authorization": f"Bearer {API_KEY}"}
    kwargs = {
        "max_size": 32 * 1024 * 1024,
        "ping_interval": 20,
        "ping_timeout": 20,
        "close_timeout": 5,
    }
    # websockets >= 14 用 additional_headers，旧版本用 extra_headers
    if "additional_headers" in inspect.signature(websockets.connect).parameters:
        kwargs["additional_headers"] = headers
    else:
        kwargs["extra_headers"] = headers
    return await websockets.connect(url, **kwargs)


async def wait_for_event(upstream, wanted: str, timeout: float = 10) -> dict:
    """读取上游事件，直到出现 wanted；error 事件直接抛出。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f"等待 {wanted} 超时（检查模型名、地域和 WorkspaceId）")
        raw = await asyncio.wait_for(upstream.recv(), remaining)
        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        kind = event.get("type")
        if kind == wanted:
            return event
        if kind == "error":
            detail = event.get("error") or event
            if isinstance(detail, dict):
                detail = detail.get("message") or json.dumps(detail, ensure_ascii=False)
            raise RuntimeError(str(detail))


async def handshake(upstream, client: WebSocket) -> None:
    """session.created -> session.update -> session.updated -> 通知浏览器开麦。"""
    await wait_for_event(upstream, "session.created")
    await upstream.send(json.dumps(session_update(), ensure_ascii=False))
    updated = await wait_for_event(upstream, "session.updated")
    await client.send_json(
        {"type": "server.ready", "session": updated.get("session", {})},
    )


async def pump(client: WebSocket, upstream) -> None:
    # 跟踪当前是否有进行中的 response，文本插话时先取消再新建
    session_state = {"active": False, "done": asyncio.Event()}

    async def browser_to_upstream() -> None:
        while True:
            message = await client.receive()
            if message.get("type") == "websocket.disconnect":
                return

            audio = message.get("bytes")
            if audio:
                await upstream.send(
                    json.dumps(
                        {
                            "type": "input_audio_buffer.append",
                            "audio": base64.b64encode(audio).decode("ascii"),
                        },
                    ),
                )
                continue

            text = message.get("text")
            if not text:
                continue
            try:
                event = json.loads(text)
            except json.JSONDecodeError:
                continue

            kind = event.get("type")
            if kind == "audio":
                raw = base64.b64decode(event.get("data") or "")
                if raw:
                    await upstream.send(
                        json.dumps(
                            {
                                "type": "input_audio_buffer.append",
                                "audio": base64.b64encode(raw).decode("ascii"),
                            },
                        ),
                    )
            elif kind == "text":
                content = (event.get("text") or "").strip()
                if not content:
                    continue
                if session_state["active"]:
                    session_state["done"].clear()
                    await upstream.send(json.dumps({"type": "response.cancel"}))
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(session_state["done"].wait(), 2)
                await upstream.send(
                    json.dumps(
                        {
                            "type": "conversation.item.create",
                            "item": {
                                "type": "message",
                                "role": "user",
                                "content": [{"type": "input_text", "text": content}],
                            },
                        },
                        ensure_ascii=False,
                    ),
                )
                await upstream.send(json.dumps({"type": "response.create"}))
            elif kind == "cancel":
                await upstream.send(json.dumps({"type": "response.cancel"}))
            elif kind == "stop":
                return

    async def upstream_to_browser() -> None:
        async for raw in upstream:
            if isinstance(raw, (bytes, bytearray)):
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue

            kind = event.get("type")
            if kind == "response.created":
                session_state["active"] = True
                session_state["done"].clear()
            elif kind == "response.done":
                session_state["active"] = False
                session_state["done"].set()

            # 音频直接以二进制下发，省掉浏览器的 base64 解码
            if kind == "response.audio.delta":
                pcm = base64.b64decode(event.get("delta") or "")
                if pcm:
                    STATS["audioPackets"] += 1
                    STATS["audioBytes"] += len(pcm)
                    await client.send_bytes(pcm)
                continue

            if kind == "input_audio_buffer.speech_started":
                STATS["speechStarted"] += 1
            elif kind == "error":
                STATS["upstreamErrors"] += 1
                detail = event.get("error") or event
                if isinstance(detail, dict):
                    detail = detail.get("message") or json.dumps(detail, ensure_ascii=False)
                STATS["lastError"] = str(detail)

            await client.send_text(raw)

    tasks = {
        asyncio.create_task(browser_to_upstream()),
        asyncio.create_task(upstream_to_browser()),
    }
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        with suppress(asyncio.CancelledError):
            error = task.exception()
            if error:
                raise error


@app.get("/api/config")
async def api_config():
    return {
        "model": MODEL,
        "voice": VOICE,
        "region": REGION,
        "vad": VAD_TYPE,
        "silenceMs": SILENCE_MS,
        "apiKeyConfigured": API_KEY_CONFIGURED,
        "workspaceConfigured": bool(
            WORKSPACE_CONFIGURED or os.getenv("DASHSCOPE_WS_URL", "").strip(),
        ),
    }


@app.get("/api/health")
@app.get("/health")
async def api_health():
    return {"ok": True}


@app.get("/api/stats")
async def api_stats():
    return {**STATS, "uptimeSec": round(time.monotonic() - STARTED_AT, 1)}


@app.websocket("/ws")
async def ws_bridge(client: WebSocket) -> None:
    await client.accept()

    if not API_KEY_CONFIGURED:
        await client.send_json(
            {
                "type": "server.error",
                "error": {
                    "message": "服务端未配置 DASHSCOPE_API_KEY（或仍是示例占位符），请编辑 .env 后重启。",
                },
            },
        )
        await client.close(code=1011)
        return

    try:
        url = upstream_url()
    except RuntimeError as exc:
        await client.send_json(
            {"type": "server.error", "error": {"message": str(exc)}},
        )
        await client.close(code=1011)
        return

    try:
        upstream = await connect_upstream(url)
    except Exception as exc:  # noqa: BLE001
        log.warning("connect upstream failed: %r", exc)
        await client.send_json(
            {"type": "server.error", "error": {"message": f"连接 DashScope 失败：{exc}"}},
        )
        await client.close(code=1011)
        return

    log.info("browser connected")
    STATS["sessions"] += 1
    try:
        await handshake(upstream, client)
        await pump(client, upstream)
    except Exception as exc:  # noqa: BLE001
        log.warning("session ended with error: %r", exc)
        detail = str(exc) or exc.__class__.__name__
        STATS["upstreamErrors"] += 1
        STATS["lastError"] = detail
        with suppress(Exception):
            await client.send_json(
                {"type": "server.error", "error": {"message": f"会话异常：{detail}"}},
            )
    finally:
        with suppress(Exception):
            await upstream.close()
        with suppress(Exception):
            await client.close()
        log.info("browser disconnected")


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )

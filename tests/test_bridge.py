#!/usr/bin/env python3
"""不依赖真实 DashScope 的桥接集成测试。

启动一个假的 Realtime 上游 + 真实的 server.py，验证：
  1. session.update -> session.updated -> server.ready
  2. 浏览器二进制 PCM -> input_audio_buffer.append(base64)
  3. response.audio.delta(base64) -> 浏览器二进制 PCM
  4. 转写事件透传
  5. 文本输入 -> conversation.item.create + response.create
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parents[1]
MOCK_PORT = 8765
APP_PORT = 8100


async def mock_handler(ws):
    await ws.send(json.dumps({"type": "session.created", "session": {"id": "sess_mock"}}))
    async for raw in ws:
        event = json.loads(raw)
        kind = event.get("type")

        if kind == "session.update":
            await ws.send(
                json.dumps(
                    {
                        "type": "session.updated",
                        "session": {"id": "sess_mock", "voice": "Tina"},
                    },
                ),
            )
        elif kind == "input_audio_buffer.append":
            size = len(base64.b64decode(event["audio"]))
            await ws.send(
                json.dumps(
                    {
                        "type": "conversation.item.input_audio_transcription.completed",
                        "item_id": "item_1",
                        "transcript": f"收到 {size} 字节",
                    },
                    ensure_ascii=False,
                ),
            )
            await ws.send(json.dumps({"type": "response.created", "response": {"id": "resp_1"}}))
            await ws.send(
                json.dumps(
                    {
                        "type": "response.audio_transcript.delta",
                        "response_id": "resp_1",
                        "delta": "这是测试回复",
                    },
                    ensure_ascii=False,
                ),
            )
            pcm = b"\x01\x00" * 2400  # 100 ms @ 24 kHz
            await ws.send(
                json.dumps(
                    {
                        "type": "response.audio.delta",
                        "response_id": "resp_1",
                        "delta": base64.b64encode(pcm).decode(),
                    },
                ),
            )
            await ws.send(
                json.dumps(
                    {
                        "type": "response.audio_transcript.done",
                        "response_id": "resp_1",
                        "transcript": "这是测试回复",
                    },
                    ensure_ascii=False,
                ),
            )
            await ws.send(json.dumps({"type": "response.done", "response": {"id": "resp_1"}}))
        elif kind == "conversation.item.create":
            await ws.send(json.dumps({"type": "conversation.item.created", "item": event["item"]}))
        elif kind == "response.create":
            await ws.send(json.dumps({"type": "response.created", "response": {"id": "resp_text"}}))
            await ws.send(
                json.dumps(
                    {
                        "type": "response.audio_transcript.done",
                        "response_id": "resp_text",
                        "transcript": "收到文字",
                    },
                    ensure_ascii=False,
                ),
            )
            await ws.send(json.dumps({"type": "response.done", "response": {"id": "resp_text"}}))


async def wait_port(port: int, timeout: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return
        except OSError:
            await asyncio.sleep(0.1)
    raise TimeoutError(f"port {port} not ready")


async def run_client() -> None:
    async with websockets.connect(f"ws://127.0.0.1:{APP_PORT}/ws") as ws:
        ready = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert ready["type"] == "server.ready", ready

        await ws.send(b"\x00\x01" * 1600)  # 100 ms @ 16 kHz

        user_text = None
        ai_text = None
        audio = b""
        got_done = False

        while not got_done:
            message = await asyncio.wait_for(ws.recv(), 5)
            if isinstance(message, bytes):
                audio += message
                continue
            event = json.loads(message)
            if event["type"] == "conversation.item.input_audio_transcription.completed":
                user_text = event["transcript"]
            elif event["type"] == "response.audio_transcript.done":
                ai_text = event["transcript"]
            elif event["type"] == "response.done" and event["response"]["id"] == "resp_1":
                got_done = True

        assert user_text == "收到 3200 字节", user_text
        assert ai_text == "这是测试回复", ai_text
        assert len(audio) == 4800, len(audio)

        await ws.send(json.dumps({"type": "text", "text": "你好"}, ensure_ascii=False))
        text_reply = None
        while text_reply is None:
            message = await asyncio.wait_for(ws.recv(), 5)
            if isinstance(message, bytes):
                continue
            event = json.loads(message)
            if event["type"] == "response.audio_transcript.done":
                text_reply = event["transcript"]
        assert text_reply == "收到文字", text_reply

    print("bridge test OK")


async def main() -> None:
    async with websockets.serve(mock_handler, "127.0.0.1", MOCK_PORT):
        env = os.environ.copy()
        env.update(
            {
                "DASHSCOPE_API_KEY": "test-key",
                "DASHSCOPE_WS_URL": f"ws://127.0.0.1:{MOCK_PORT}/api-ws/v1/realtime",
                "HOST": "127.0.0.1",
                "PORT": str(APP_PORT),
            },
        )
        proc = subprocess.Popen(
            [sys.executable, "server.py"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            await wait_port(APP_PORT)
            await run_client()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    asyncio.run(main())

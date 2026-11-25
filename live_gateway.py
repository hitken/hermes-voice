"""OpenAI Live 兼容的 WebRTC 网关（M1）。

Hermes Desktop 的「实时对话」走 OpenAI Live API over WebRTC：

    Electron ── WebRTC(Opus) ──> 这里 ── WebSocket ──> DashScope Realtime

本模块只负责传输与协议翻译：
- 接住 SDP offer，用 aiortc 回 answer
- 麦克风音频 Opus → PCM16 16k → input_audio_buffer.append
- DashScope 24k PCM → 48k → Opus 回给浏览器
- oai-events 数据通道：转写、错误、session.closed

Hermes 请求 client delegation 时进入 M3 模式：用户说完只发
session.delegation.created，Hermes 用 commentary.append 把答案送回来，
qwen 只负责把文本念出来。不请求委派时是 M1 的纯语音对话。
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
import uuid
from collections import deque
from fractions import Fraction
from typing import Any, Awaitable, Callable

import av
from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import AudioStreamTrack, MediaStreamError

log = logging.getLogger("omni-live")

# aiortc/aioice 默认把每一条 ICE candidate pair 都打出来，正常运行时太吵
logging.getLogger("aioice").setLevel(logging.WARNING)
logging.getLogger("aiortc.rtcicetransport").setLevel(logging.WARNING)

# 48 kHz、20 ms 一帧，Opus 的标准打包大小
OUT_RATE = 48000
OUT_SAMPLES = 960
OUT_BYTES = OUT_SAMPLES * 2

IN_RATE = 16000
DASHSCOPE_OUT_RATE = 24000

# 这些短句不需要 Hermes 跑工具，直接让 qwen 用语音回答，省掉几十秒延迟。
SMALL_TALK_RE = re.compile(
    r"(你好|您好|嗨|喂|在吗|在不在|能听到|听得到|听的到|听到我|你好吗|"
    r"谢谢|多谢|辛苦了|再见|拜拜|晚安|早上好|中午好|晚上好|哈哈|呵呵|"
    r"好的|好吧|没事|开玩笑|你是谁|你叫什么|谁呀|干嘛呢|在干吗|"
    r"今天怎么样|最近怎么样|回答我|说话呀|你在哪)"
)


def is_small_talk(text: str) -> bool:
    text = text.strip()
    return len(text) <= 24 and bool(SMALL_TALK_RE.search(text))


class OutboundTrack(AudioStreamTrack):
    """把 DashScope 的 24k PCM 变成浏览器能播的 48k 音频轨。

    aiortc 的发送端按 20ms 节奏调用 recv()，没有数据时补静音，
    这样 RTP 时间轴不会因为模型思考而断掉。
    """

    kind = "audio"

    def __init__(self) -> None:
        super().__init__()
        self._frames: deque[av.AudioFrame] = deque(maxlen=500)
        self._buffer = bytearray()
        self._resampler = av.AudioResampler(format="s16", layout="mono", rate=OUT_RATE)
        self._timestamp = 0
        self._started_at: float | None = None

    def push_pcm(self, data: bytes) -> None:
        """data: 24 kHz / 16 bit / 单声道小端 PCM。"""
        if not data:
            return
        samples = len(data) // 2
        if samples == 0:
            return
        frame = av.AudioFrame(format="s16", layout="mono", samples=samples)
        frame.sample_rate = DASHSCOPE_OUT_RATE
        frame.planes[0].update(data[: samples * 2])

        for out in self._resampler.resample(frame):
            self._buffer.extend(bytes(out.planes[0]))

        while len(self._buffer) >= OUT_BYTES:
            chunk = bytes(self._buffer[:OUT_BYTES])
            del self._buffer[:OUT_BYTES]
            ready = av.AudioFrame(format="s16", layout="mono", samples=OUT_SAMPLES)
            ready.sample_rate = OUT_RATE
            ready.planes[0].update(chunk)
            self._frames.append(ready)

    def flush(self) -> None:
        """用户插话时丢掉还没播的部分。"""
        self._frames.clear()
        self._buffer.clear()
        self._resampler = av.AudioResampler(format="s16", layout="mono", rate=OUT_RATE)

    async def recv(self) -> av.AudioFrame:
        if self.readyState != "live":
            raise MediaStreamError

        if self._started_at is None:
            self._started_at = time.monotonic()
            self._timestamp = 0
        else:
            self._timestamp += OUT_SAMPLES
            wait = self._started_at + self._timestamp / OUT_RATE - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)

        if self._frames:
            frame = self._frames.popleft()
        else:
            frame = av.AudioFrame(format="s16", layout="mono", samples=OUT_SAMPLES)
            frame.sample_rate = OUT_RATE
            frame.planes[0].update(bytes(OUT_BYTES))

        frame.pts = self._timestamp
        frame.time_base = Fraction(1, OUT_RATE)
        return frame


class LivePeer:
    """一条 Hermes 实时会话 = 一个 WebRTC peer + 一条 DashScope 连接。"""

    def __init__(
        self,
        *,
        connect_upstream: Callable[[], Awaitable[Any]],
        session_update: Callable[[], dict[str, Any]],
        stats: dict[str, Any] | None = None,
        on_closed: Callable[[str], None] | None = None,
        delegation: bool = False,
    ) -> None:
        self.id = f"sess_{uuid.uuid4().hex[:12]}"
        self._connect_upstream = connect_upstream
        self._session_update = session_update
        self._stats = stats
        self._on_closed = on_closed
        self.delegation = delegation
        self._delegation_id: str | None = None
        self._delegation_pending = False
        self._answer_received = False
        self._pending_turns: list[str] = []
        self._direct_texts: list[str] = []
        self._direct_timer: asyncio.Task | None = None
        self._extra_instructions: list[str] = []
        self._speak_queue: asyncio.Queue[str] = asyncio.Queue()
        self._response_done = asyncio.Event()

        self.pc = RTCPeerConnection()
        self.track = OutboundTrack()
        self.channel = None
        self.upstream = None
        self.muted = False
        self.closed = False
        self._started_at = time.monotonic()
        self._tasks: set[asyncio.Task] = set()

        self.pc.addTrack(self.track)

        @self.pc.on("datachannel")
        def on_datachannel(channel) -> None:
            self.channel = channel
            log.info("%s datachannel %s", self.id, channel.label)

            @channel.on("message")
            def on_message(message) -> None:
                if isinstance(message, (bytes, bytearray)):
                    return
                try:
                    event = json.loads(message)
                except (json.JSONDecodeError, TypeError):
                    return
                self._on_client_event(event)

            @channel.on("close")
            def on_close() -> None:
                self._spawn(self.close("datachannel_closed"))

        @self.pc.on("connectionstatechange")
        def on_state() -> None:
            log.info("%s peer state %s", self.id, self.pc.connectionState)
            if self.pc.connectionState in ("failed", "closed", "disconnected"):
                self._spawn(self.close("connection_lost"))

        @self.pc.on("track")
        def on_track(track) -> None:
            if track.kind != "audio":
                return
            self._spawn(self._pump_mic(track))

    # ---------- 生命周期 ----------

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def start(self, offer_sdp: str) -> str:
        """接住 offer，连上游，返回 answer SDP。"""
        await self.pc.setRemoteDescription(
            RTCSessionDescription(sdp=offer_sdp, type="offer"),
        )
        await self._open_upstream()
        answer = await self.pc.createAnswer()
        await self.pc.setLocalDescription(answer)
        return self.pc.localDescription.sdp

    async def _open_upstream(self) -> None:
        self.upstream = await self._connect_upstream()
        await self._await_event("session.created")
        await self.upstream.send(
            json.dumps(self._session_update(self.delegation), ensure_ascii=False),
        )
        await self._await_event("session.updated")

        self._send(
            {
                "type": "session.started",
                "session": {"id": self.id},
            },
        )
        self._spawn(self._pump_upstream())
        if self.delegation:
            self._spawn(self._speak_worker())
            log.info("%s delegation mode", self.id)
        if self._stats is not None:
            self._stats["liveSessions"] = self._stats.get("liveSessions", 0) + 1
        log.info("%s upstream ready", self.id)

    async def _await_event(self, wanted: str, timeout: float = 15) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(f"等待 {wanted} 超时")
            raw = await asyncio.wait_for(self.upstream.recv(), remaining)
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

    async def close(self, reason: str = "closed") -> None:
        if self.closed:
            return
        self.closed = True

        self._send({"type": "session.closed", "reason": reason})
        current = asyncio.current_task()
        for task in list(self._tasks):
            if task is not current:
                task.cancel()
        self._tasks.clear()

        if self.upstream is not None:
            try:
                await self.upstream.close()
            except Exception:  # noqa: BLE001
                pass
            self.upstream = None
        try:
            await self.pc.close()
        except Exception:  # noqa: BLE001
            pass
        if self._on_closed is not None:
            self._on_closed(self.id)
        log.info("%s closed (%s)", self.id, reason)

    # ---------- 数据通道 ----------

    def _send(self, event: dict[str, Any]) -> None:
        channel = self.channel
        if channel is None or channel.readyState != "open":
            return
        try:
            channel.send(json.dumps(event, ensure_ascii=False))
        except Exception:  # noqa: BLE001
            pass

    def _on_client_event(self, event: dict[str, Any]) -> None:
        kind = event.get("type")
        if kind in (
            "session.commentary.append",
            "session.thinking.append",
            "session.instructions.append",
            "session.close",
        ):
            detail = str(event.get("content") or event.get("reason") or "")[:80]
            log.info("%s <- client %s %s", self.id, kind, detail)
        if kind == "session.close":
            self._spawn(self.close("client_closed"))
        elif kind == "session.input_audio.mute":
            self.muted = True
        elif kind == "session.input_audio.unmute":
            self.muted = False
        elif kind == "session.instructions.append":
            content = str(event.get("content") or "").strip()
            if content and self.upstream is not None:
                self._extra_instructions.append(content)
                update = self._session_update(self.delegation)
                session = update.get("session") or {}
                extra = "\n".join(self._extra_instructions)
                session["instructions"] = f"{session.get('instructions', '')}\n{extra}".strip()
                update["session"] = session
                self._spawn(self._send_upstream(update))
        elif kind == "session.commentary.append":
            content = str(event.get("content") or "").strip()
            if content and self.delegation:
                self._answer_received = True
                self._speak_queue.put_nowait(content)
        elif kind == "session.thinking.append":
            # 进度提示不影响音频，记一笔即可
            log.debug("%s thinking: %s", self.id, str(event.get("content") or "")[:120])

    async def _send_upstream(self, payload: dict[str, Any]) -> None:
        if self.upstream is None:
            return
        try:
            await self.upstream.send(json.dumps(payload, ensure_ascii=False))
        except Exception:  # noqa: BLE001
            pass

    # ---------- 委派朗读 ----------

    def _queue_direct(self, text: str) -> None:
        """短句常被 VAD 切成两半，攒一下再回答一次。"""
        self._direct_texts.append(text)
        if self._direct_timer is not None:
            self._direct_timer.cancel()
        self._direct_timer = self._spawn(self._flush_direct())

    async def _flush_direct(self) -> None:
        await asyncio.sleep(2.0)
        text = max(self._direct_texts, key=len)
        self._direct_texts.clear()
        if text:
            await self._answer_direct(text)

    async def _answer_direct(self, text: str) -> None:
        """短问候直接让 qwen 语音回答，不走 Hermes 的工具链。"""
        if self.upstream is None:
            return
        log.info("%s direct answer: %s", self.id, text[:40])
        await self._send_upstream(
            {
                "type": "response.create",
                "response": {
                    "instructions": (
                        f"用户问：{text}\n请直接用中文口语回答这个问题本身，"
                        "只说答案，不要重复问题，不要念提示。"
                    ),
                },
            },
        )

    def _start_delegation(self, text: str) -> None:
        self._delegation_id = f"deleg_{uuid.uuid4().hex[:10]}"
        self._delegation_pending = True
        self._answer_received = False
        self._send(
            {
                "type": "session.delegation.created",
                "delegation": {
                    "id": self._delegation_id,
                    "type": "client",
                    "target": "hermes",
                },
            },
        )
        log.info("%s delegated %s: %s", self.id, self._delegation_id, text[:40])
        self._spawn(self._delegation_wait(self._delegation_id))

    async def _delegation_wait(self, delegation_id: str) -> None:
        """Hermes 慢的时候先给一句反馈，别让用户以为断了。"""
        await asyncio.sleep(10)
        if self.closed or self._delegation_id != delegation_id:
            return
        if self._answer_received or self._delegation_pending is False:
            return
        self._speak_queue.put_nowait("收到，Hermes 正在处理，请稍等。")

    def _maybe_next_delegation(self) -> None:
        """一条答案念完后，再处理排队的后续提问。"""
        if not self._answer_received or not self._pending_turns:
            return
        self._delegation_pending = False
        self._answer_received = False
        joined = "；".join(self._pending_turns)
        self._pending_turns.clear()
        self._start_delegation(joined)

    async def _speak_worker(self) -> None:
        """把 Hermes 的 commentary 文本依次交给 qwen 念出来。

        Hermes 会把长回复切成多块，先攒一小会儿再合并，避免一句一块地念。
        """
        while not self.closed:
            first = await self._speak_queue.get()
            await asyncio.sleep(0.25)
            parts = [first]
            while not self._speak_queue.empty():
                parts.append(self._speak_queue.get_nowait())
            text = "\n".join(parts).strip()
            if not text or self.upstream is None:
                continue

            self._response_done.clear()
            # 用 response.instructions 直接指定本次回答内容，比塞一条 user 消息
            # 可靠得多：后者偶尔会被模型当成指令，只回一句「好的，我这就开始」。
            await self._send_upstream(
                {
                    "type": "response.create",
                    "response": {
                        "instructions": f"请逐字念出下面这段话，不要说任何额外的话：{text}",
                    },
                },
            )
            try:
                await asyncio.wait_for(self._response_done.wait(), 30)
            except asyncio.TimeoutError:
                log.warning("%s speak timed out", self.id)
            self._maybe_next_delegation()

    # ---------- 音频 ----------

    async def _pump_mic(self, track) -> None:
        """浏览器麦克风 → DashScope。"""
        resampler = av.AudioResampler(format="s16", layout="mono", rate=IN_RATE)
        logged = False
        try:
            while not self.closed:
                frame = await track.recv()
                if self.muted or self.upstream is None:
                    continue
                if not logged:
                    logged = True
                    log.debug(
                        "%s mic in %s/%s @%s",
                        self.id, frame.format.name, frame.layout.name, frame.sample_rate,
                    )
                for out in resampler.resample(frame):
                    pcm = bytes(out.planes[0])
                    if not pcm:
                        continue
                    await self.upstream.send(
                        json.dumps(
                            {
                                "type": "input_audio_buffer.append",
                                "audio": base64.b64encode(pcm).decode("ascii"),
                            },
                        ),
                    )
        except (MediaStreamError, asyncio.CancelledError):
            return
        except Exception as exc:  # noqa: BLE001
            log.warning("%s mic pump stopped: %r", self.id, exc)

    async def _pump_upstream(self) -> None:
        """DashScope → 音频轨 + 数据通道事件。"""
        try:
            async for raw in self.upstream:
                if isinstance(raw, (bytes, bytearray)):
                    continue
                try:
                    event = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                self._handle_upstream_event(event)
                if event.get("type") in ("response.done", "response.cancel"):
                    self._response_done.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("%s upstream ended: %r", self.id, exc)
            self._send(
                {"type": "error", "error": {"code": "upstream_closed", "message": str(exc)}},
            )
            await self.close("upstream_closed")

    def _handle_upstream_event(self, event: dict[str, Any]) -> None:
        kind = event.get("type")
        now_ms = int((time.monotonic() - self._started_at) * 1000)

        if kind == "response.audio.delta":
            pcm = base64.b64decode(event.get("delta") or "")
            if pcm:
                self.track.push_pcm(pcm)
                if self._stats is not None:
                    self._stats["audioPackets"] = self._stats.get("audioPackets", 0) + 1
                    self._stats["audioBytes"] = self._stats.get("audioBytes", 0) + len(pcm)
            return

        if kind == "input_audio_buffer.speech_started":
            self.track.flush()
            if self.delegation:
                self._spawn(self._send_upstream({"type": "response.cancel"}))
            if self._stats is not None:
                self._stats["speechStarted"] = self._stats.get("speechStarted", 0) + 1
            return

        if kind == "conversation.item.input_audio_transcription.delta":
            self._send(
                {
                    "type": "session.input_transcript.delta",
                    "delta": event.get("text") or event.get("delta") or "",
                    "start_ms": now_ms,
                    "end_ms": now_ms,
                },
            )
            return

        if kind == "conversation.item.input_audio_transcription.completed":
            transcript = event.get("transcript") or ""
            self._send(
                {
                    "type": "session.input_transcript.delta",
                    "delta": transcript,
                    "start_ms": now_ms,
                    "end_ms": now_ms,
                },
            )
            if self.delegation and str(transcript).strip():
                text = str(transcript).strip()
                if is_small_talk(text):
                    self._queue_direct(text)
                elif self._delegation_pending:
                    # 上一轮还在跑：排队，不重复委派，否则 Hermes 会中断上一轮
                    # 并把刚生成的回复标成「已念过」，我们就永远拿不到答案。
                    self._pending_turns.append(text)
                    log.info("%s queued turn (%d pending)", self.id, len(self._pending_turns))
                else:
                    self._start_delegation(text)
            return

        if kind == "response.audio_transcript.delta":
            self._send(
                {
                    "type": "session.output_transcript.delta",
                    "delta": event.get("delta") or "",
                    "start_ms": now_ms,
                    "end_ms": now_ms,
                },
            )
            return

        if kind == "error":
            detail = event.get("error") or event
            if isinstance(detail, dict):
                detail = detail.get("message") or json.dumps(detail, ensure_ascii=False)
            if self._stats is not None:
                self._stats["upstreamErrors"] = self._stats.get("upstreamErrors", 0) + 1
                self._stats["lastError"] = str(detail)
            self._send(
                {"type": "error", "error": {"code": "upstream_error", "message": str(detail)}},
            )


class LiveGateway:
    """`POST /v1/live/sessions` 的实现。"""

    def __init__(
        self,
        *,
        connect_upstream: Callable[[], Awaitable[Any]],
        session_update: Callable[[], dict[str, Any]],
        stats: dict[str, Any] | None = None,
    ) -> None:
        self._connect_upstream = connect_upstream
        self._session_update = session_update
        self._stats = stats
        self._peers: dict[str, LivePeer] = {}

    async def handle_offer(
        self,
        offer_sdp: str,
        delegation: bool = False,
    ) -> tuple[str, str]:
        peer = LivePeer(
            connect_upstream=self._connect_upstream,
            session_update=self._session_update,
            stats=self._stats,
            on_closed=lambda peer_id: self._peers.pop(peer_id, None),
            delegation=delegation,
        )
        self._peers[peer.id] = peer
        try:
            answer = await peer.start(offer_sdp)
        except Exception:
            self._peers.pop(peer.id, None)
            await peer.close("handshake_failed")
            raise
        return peer.id, answer

    async def shutdown(self) -> None:
        for peer in list(self._peers.values()):
            await peer.close("server_shutdown")
        self._peers.clear()

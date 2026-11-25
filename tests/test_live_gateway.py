#!/usr/bin/env python3
"""不依赖浏览器的 WebRTC 网关单测：重采样、事件映射、打断与统计。"""

import asyncio
import base64
import json
import math
import struct
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from live_gateway import (  # noqa: E402
    IN_RATE, OUT_RATE, OUT_SAMPLES, LivePeer, OutboundTrack, is_small_talk,
)


class FakeUpstream:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(payload))


class FakeChannel:
    def __init__(self):
        self.readyState = "open"
        self.messages = []

    def send(self, payload):
        self.messages.append(json.loads(payload))


def s24(samples: int) -> bytes:
    values = (
        int(9000 * math.sin(2 * math.pi * 440 * i / 24000)) for i in range(samples)
    )
    return struct.pack("<%dh" % samples, *values)


def make_peer(stats, delegation=False):
    peer = LivePeer.__new__(LivePeer)
    peer.id = "sess_test"
    peer.channel = FakeChannel()
    peer.track = OutboundTrack()
    peer._stats = stats
    peer._started_at = asyncio.get_running_loop().time()
    peer.delegation = delegation
    peer._delegation_id = None
    peer._delegation_pending = False
    peer._answer_received = False
    peer._pending_turns = []
    peer._direct_texts = []
    peer._direct_timer = None
    peer._extra_instructions = []
    peer._session_update = lambda delegation=False: {
        "type": "session.update",
        "session": {"instructions": "base", "turn_detection": {}},
    }
    peer._tasks = set()
    peer._speak_queue = asyncio.Queue()
    peer._response_done = asyncio.Event()
    return peer


async def main():
    # 1) 24k -> 48k 重采样，输出仍是 20ms 一帧
    track = OutboundTrack()
    track.push_pcm(s24(24000))  # 1 秒
    first = await track.recv()
    assert first.sample_rate == OUT_RATE, first.sample_rate
    assert first.samples == OUT_SAMPLES, first.samples
    peak = max(abs(v) for v in struct.unpack("<%dh" % first.samples, bytes(first.planes[0])))
    assert peak > 1000, peak
    print("resample OK: 24k -> 48k, 20ms frame, peak", peak)

    stats = {}
    peer = make_peer(stats)

    # 2) 模型音频：base64 -> 播放队列 + 统计
    pcm = s24(24000)
    peer._handle_upstream_event(
        {"type": "response.audio.delta", "delta": base64.b64encode(pcm).decode()}
    )
    assert stats["audioPackets"] == 1
    assert stats["audioBytes"] == len(pcm)
    assert peer.track._frames, "no audio queued"

    # 3) 用户插话：清空待播队列
    peer._handle_upstream_event({"type": "input_audio_buffer.speech_started"})
    assert not peer.track._frames
    assert stats["speechStarted"] == 1
    print("barge-in OK: queued audio flushed")

    # 4) 字幕映射
    peer._handle_upstream_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "你好",
        }
    )
    peer._handle_upstream_event(
        {"type": "response.audio_transcript.delta", "delta": "你也好"}
    )
    kinds = [m["type"] for m in peer.channel.messages]
    assert "session.input_transcript.delta" in kinds, kinds
    assert "session.output_transcript.delta" in kinds, kinds
    assert peer.channel.messages[-1]["delta"] == "你也好"
    print("transcript mapping OK:", kinds)

    # 5) 错误映射
    peer._handle_upstream_event(
        {"type": "error", "error": {"message": "上游断开"}}
    )
    assert stats["upstreamErrors"] == 1
    assert stats["lastError"] == "上游断开"
    assert peer.channel.messages[-1]["type"] == "error"
    print("error mapping OK")

    # 6) 委派模式：识别完成后发 delegation.created，commentary 进朗读队列
    import server

    upd = server.session_update(delegation=True)
    assert upd["session"]["turn_detection"]["create_response"] is False
    assert "只负责朗读" in upd["session"]["instructions"]

    dpeer = make_peer({}, delegation=True)
    dpeer._handle_upstream_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "帮我查一下",
        }
    )
    delegations = [
        m for m in dpeer.channel.messages if m["type"] == "session.delegation.created"
    ]
    assert len(delegations) == 1 and delegations[0]["delegation"]["id"]
    assert dpeer._delegation_id == delegations[0]["delegation"]["id"]

    dpeer._on_client_event(
        {
            "type": "session.commentary.append",
            "content": "查到了",
            "delegation_id": dpeer._delegation_id,
            "event_id": "evt_1",
        }
    )
    assert dpeer._speak_queue.qsize() == 1
    assert dpeer._speak_queue.get_nowait() == "查到了"

    dpeer.upstream = FakeUpstream()
    dpeer._on_client_event({"type": "session.instructions.append", "content": "第一条"})
    dpeer._on_client_event({"type": "session.instructions.append", "content": "第二条"})
    await asyncio.sleep(0)
    text = dpeer.upstream.sent[-1]["session"]["instructions"]
    assert "第一条" in text and "第二条" in text, text
    print("instructions accumulate OK")

    # 非委派模式不应主动发委派事件
    apeers = make_peer({})
    apeers._handle_upstream_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "你好",
        }
    )
    assert not any(
        m["type"] == "session.delegation.created" for m in apeers.channel.messages
    )
    print("delegation OK")

    # 7) 短问候走直接回答，不进 Hermes；上一轮未完成时新话排队
    assert is_small_talk("你好，能听到我说话吗？")
    assert is_small_talk("回答我呢")
    assert not is_small_talk("帮我查一下当前目录的文件")

    dpeer.upstream = FakeUpstream()
    await dpeer._answer_direct("能听到你说话")
    sent = dpeer.upstream.sent[-1]
    assert sent["type"] == "response.create"
    assert "能听到你说话" in sent["response"]["instructions"]
    assert "不要重复问题" in sent["response"]["instructions"]

    dpeer._delegation_pending = True
    dpeer._pending_turns.clear()
    dpeer.channel.messages.clear()
    dpeer._handle_upstream_event(
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "再帮我看看另一个文件",
        }
    )
    assert not any(
        m["type"] == "session.delegation.created" for m in dpeer.channel.messages
    )
    assert dpeer._pending_turns == ["再帮我看看另一个文件"]
    print("small talk + pending queue OK")

    assert IN_RATE == 16000 and OUT_RATE == 48000
    print("gateway test OK")


asyncio.run(main())

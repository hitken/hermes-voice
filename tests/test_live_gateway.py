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

from live_gateway import IN_RATE, OUT_RATE, OUT_SAMPLES, LivePeer, OutboundTrack  # noqa: E402


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


def make_peer(stats):
    peer = LivePeer.__new__(LivePeer)
    peer.id = "sess_test"
    peer.channel = FakeChannel()
    peer.track = OutboundTrack()
    peer._stats = stats
    peer._started_at = asyncio.get_running_loop().time()
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

    assert IN_RATE == 16000 and OUT_RATE == 48000
    print("gateway test OK")


asyncio.run(main())

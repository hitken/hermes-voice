# 需求：把 Hermes Desktop 的「实时对话」接到本服务

状态：**M1/M2/M3 已完成**；M4（稳定性、文档、回归测试）进行中
日期：2026-11-25

> M1：`live_gateway.py` 实现 SDP 握手、音频双向、字幕/错误/关闭事件；
> `POST /v1/live/sessions` 已接入 `server.py`。实测 WebRTC 连通、模型音频 36 包 /
> 549120 字节、声卡有声 15.7s、字幕完整、无 JS 报错、关闭后无残留。
> M2：Hermes 配置已写入（`voice_chat_mode=gpt-live`、
> `voice.gpt_live.base_url=http://127.0.0.1:8000/v1`、占位 Key，
> `resolve_gpt_live_status().available=true`）。用 Hermes 自己的
> `create_webrtc_session()` 生成真实 SDP 并完成握手，WebRTC 连通、字幕和声音正常。
> M3：委派模式已实现。实测 qwen 不抢答，发出 `session.delegation.created`，
> 收到 `session.commentary.append` 后准确念出委派答案；A 模式无回归。
> 朗读用 `response.create` 的 `response.instructions` 指定内容（不用 user 消息），
> 否则模型偶尔只回「好的，我这就开始」而不念正文。
> 单测 `tests/test_live_gateway.py`、`tests/test_bridge.py` 均通过。

## 1. 背景

本仓库已经是一个可用的本地实时语音服务：

```
浏览器 ── WebSocket ──> FastAPI ── WebSocket ──> DashScope Realtime
        16k PCM 上行                  24k PCM 下行
```

模型 `qwen3.8-omni-flash-realtime`，浏览器端已经验证：语音进、语音出、实时字幕、打断、断线重连。

Hermes Desktop 自带一个「实时对话」入口，默认走 OpenAI 的 GPT-Live（`gpt-live-1`）。希望把这个入口接到本服务，用 DashScope 的语音模型替代 OpenAI，省掉 OpenAI 的按分钟计费。

## 2. 调研结论（基于 `~/.hermes/hermes-agent` 源码）

### 2.1 Hermes 侧的实际情况

| 项 | 值 |
| --- | --- |
| 模式开关 | `voice.voice_chat_mode`：`chained`（默认）/ `gpt-live` |
| 实时模型 | `voice.gpt_live.model`，默认 `gpt-live-1` |
| 音色 | `voice.gpt_live.voice`，默认 `marin` |
| 传输 | **WebRTC**，不是 WebSocket |
| 数据通道 | `oai-events` |
| 会话创建 | 渲染进程 → `POST /api/audio/voice-live/session`（Hermes 后端）→ `POST {base_url}/live/sessions`（OpenAI） |
| 凭据 | `voice.gpt_live.api_key`，回退 `OPENAI_API_KEY`；Key 只留在后端，不下发前端 |
| base URL | `voice.gpt_live.base_url`，默认 `https://api.openai.com/v1`，**可配置** |

关键代码：

- `tools/voice_live.py` —— 读取配置、拼 session 配置、把 SDP offer 转发给上游
- `apps/desktop/src/lib/voice-live.ts` —— `RTCPeerConnection`、`oai-events` 数据通道、事件处理
- `hermes_cli/web_routers/audio.py` —— `/api/audio/voice-live/session`、`/status`

### 2.2 上游握手协议

```http
POST {base_url}/live/sessions
Authorization: Bearer <key>
Content-Type: application/json

{
  "session": {
    "model": "gpt-live-1",
    "instructions": "...",
    "audio": { "output": { "voice": "marin" } },
    "delegation": { "type": "client" },
    "input": [ ...历史消息，可选... ]
  },
  "transport": { "type": "webrtc", "sdp": "<offer>" }
}
```

期望返回：

```json
{
  "session": { "id": "sess_xxx" },
  "transport": { "type": "webrtc", "sdp": "<answer>" }
}
```

返回后，渲染进程 `setRemoteDescription(answer)`，WebRTC 建立。

### 2.3 数据通道事件

服务端 → 客户端（Hermes 会处理这些）：

| 事件 | 载荷 | 用途 |
| --- | --- | --- |
| `session.started` | `{session:{id}}` | 标记会话开始 |
| `session.input_transcript.delta` | `{delta, start_ms, end_ms}` | 用户字幕 |
| `session.output_transcript.delta` | `{delta, start_ms, end_ms}` | 助手字幕 |
| `session.delegation.created` | `{delegation:{id,type,target}}` | 请求 Hermes 干活（B 模式核心） |
| `error` | `{error:{code,message}}` | 非致命错误提示 |
| `session.closed` | `{reason, usage:{seconds}}` | 会话结束 |

客户端 → 服务端：

| 事件 | 载荷 | 用途 |
| --- | --- | --- |
| `session.commentary.append` | `{content, delegation_id, event_id}` | Hermes 的回答，要求语音念出 |
| `session.thinking.append` | `{content, delegation_id, event_id}` | 进度提示 |
| `session.instructions.append` | `{content}` | 中途改人设 |
| `session.input_audio.mute` / `.unmute` | `{}` | 麦克风开关 |
| `session.close` | `{}` | 主动结束 |

### 2.4 差距

本服务现在是：

- 传输：WebSocket，自定义消息（`server.ready` / 二进制 PCM / `text`）
- 上游：DashScope Realtime，OpenAI Realtime 风格的事件名
- 没有 WebRTC，没有 `oai-events` 数据通道，没有委派语义

所以要接，需要在本服务里新增一个 **OpenAI Live 兼容的 WebRTC 网关**，做协议和音频的双向翻译。

技术可行性已验证：`aiortc` 1.16 + `av` 19 在 Python 3.14 上有现成轮子，可直接安装。

## 3. 目标与非目标

### 目标

1. Hermes 的「实时对话」按钮可以选用本服务，不再依赖 OpenAI。
2. 语音全双工，可打断，字幕可用。
3. 关闭会话后，服务端释放 DashScope 连接，无残留。
4. 不改 Hermes 上游代码，只通过配置接入。

### 非目标

- 不改 Hermes 的 UI 和交互。
- 不追求公网可用；第一版只支持本机（`127.0.0.1`）。
- 不实现 OpenAI Live 的完整事件集，只实现 Hermes 实际用到的子集。
- 不替代现有的浏览器页面，两条入口并存。

## 4. 方案

### 方案 A：纯语音对话（MVP）

qwen 直接听、直接答。Hermes 只当壳：界面显示字幕，但 Hermes 的模型、工具、记忆都不参与。

```
Electron ── WebRTC ──> 本服务 /live/sessions ── WebSocket ──> DashScope
```

实现要点：

1. `POST /live/sessions`：用 `aiortc` 接住 SDP offer，返回 answer。
2. 入向音频：Opus → PCM16 48k → 重采样 16k 单声道 → `input_audio_buffer.append`。
3. 出向音频：DashScope 24k PCM → 重采样 48k → `AudioFrame` → RTP。
4. 数据通道：
   - `conversation.item.input_audio_transcription.delta` → `session.input_transcript.delta`
   - `response.audio_transcript.delta` → `session.output_transcript.delta`
   - `input_audio_buffer.speech_started` → 本地清空待播音频（打断）
   - 会话结束 → `session.closed`
5. `session.close` → 关上游、关 peer。

### 方案 B：委派模式（完整）

qwen 只当耳朵和嘴，Hermes 当大脑：

```
你说完
  → 本服务发 session.delegation.created
  → Hermes 用自己的模型和工具干活
  → 回 session.commentary.append
  → 本服务把文本交给 qwen，让它念出来
```

实现要点：

1. DashScope 会话设 `turn_detection.create_response = false`，避免 qwen 自己抢答。
2. `input_audio_buffer.speech_stopped` + 转写完成后，发 `session.delegation.created`（带最近 5 分钟上下文）。
3. 收到 `session.commentary.append` 后，把内容作为用户消息交给 qwen，再 `response.create`，由 qwen 口语化念出。
4. `session.thinking.append` 可以映射成 qwen 的一句短提示（可选）。
5. 打断：`speech_started` 时清空播放队列并取消当前 response。

### 建议顺序

先 A 后 B。A 用来验证 WebRTC、ICE、音频重采样这些底层是否稳定；B 只增加事件层，复用 A 的传输。

## 5. 配置改动（Hermes 侧）

```bash
hermes config set voice.voice_chat_mode gpt-live
hermes config set voice.gpt_live.base_url http://127.0.0.1:8000/v1
hermes config set voice.gpt_live.api_key local --force   # 任意非空占位，仅为通过可用性检查
```

改完需要重启 Hermes Desktop。

## 6. 验收标准

### 方案 A

- [ ] Hermes 设置里语音引擎能选 `gpt-live`，不再提示缺 Key。
- [ ] 点实时对话 → 授权麦克风 → 说「你好」→ 3 秒内听到 qwen 的语音回答。
- [ ] 说话时能打断正在播放的回答。
- [ ] 用户/助手字幕能出现（允许只显示片段）。
- [ ] 点关闭 → WebRTC 断开，DashScope 会话释放，`/api/stats` 无残留会话。
- [ ] 整个过程中浏览器控制台无 JS 报错。

### 方案 B（在 A 之上）

- [ ] 说「帮我看看当前目录有哪些文件」→ Hermes 执行工具 → 语音念出真实结果。
- [ ] 委派期间有「正在处理」的提示音或短句。
- [ ] 多轮对话上下文正确（最近 5 分钟）。
- [ ] 工具调用失败时，语音能说明失败原因，而不是编造结果。

## 7. 风险

| 风险 | 说明 | 应对 |
| --- | --- | --- |
| ICE/DTLS 兼容 | Electron 155 与 aiortc 的 WebRTC 实现 | 本机 host candidate 直连；先做最小 SDP 往返测试 |
| 音频重采样 | 48k ↔ 16k/24k 的连续流 | 用 `av.AudioResampler`，保留帧边界，不按块硬切 |
| 会话生命周期 | DashScope 连接、WebRTC 连接、数据通道三方状态 | 任一方关闭都统一走 cleanup |
| 委派语义 | qwen 可能不等 Hermes 就自己回答 | `create_response=false`，并在提示词里明确只念不答 |
| 依赖体积 | `aiortc` 引入 `av`、`pylibsrtp` | 只装在服务端；现有浏览器链路不受影响 |

## 8. 里程碑

| 阶段 | 内容 | 产出 |
| --- | --- | --- |
| M1 ✅ | WebRTC 网关骨架：`/live/sessions`、SDP 往返、音频双向 | 独立测试页能听到 qwen |
| M2 ✅ | 事件映射 + A 模式接入 Hermes | Hermes 后端用自身代码完成握手，语音对话正常 |
| M3 ✅ | 委派模式 B | `delegation.created` + `commentary.append` 链路通，qwen 只念不答 |
| M4 | 稳定性、错误处理、文档 | README 更新，回归测试 |

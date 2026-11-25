# 小云 · Qwen3.8 Omni 实时语音网页

打开网页就能和 `qwen3.8-omni-flash-realtime` 语音对话。浏览器负责采集/播放，服务端负责转发并保管 API Key。

```
浏览器                         本服务                        DashScope
┌──────────────┐   PCM 16k   ┌──────────────┐   WebSocket   ┌──────────────┐
│ getUserMedia │ ──────────▶ │ FastAPI /ws  │ ────────────▶ │ Realtime API │
│ AudioWorklet │             │  (Python)    │               │              │
│ AudioContext │ ◀────────── │  二进制 PCM  │ ◀──────────── │ 24k 音频/事件 │
└──────────────┘   PCM 24k   └──────────────┘               └──────────────┘
```

## 快速开始

### 1. 准备两样东西

在[百炼控制台](https://bailian.console.aliyun.com/)拿到：

- **API Key**（`sk-...`）
- **业务空间 ID（WorkspaceId）**

> qwen3.8 这代必须走业务空间专属域名，不能用老的 `dashscope.aliyuncs.com`。
> 北京和新加坡的 Key 不通用，别拿错地域。

### 2. 配置

```bash
cd /home/ken/Work/omni-voice-web
cp .env.example .env
$EDITOR .env
```

至少填：

```dotenv
DASHSCOPE_API_KEY=sk-xxxx
DASHSCOPE_WORKSPACE_ID=xxxx
DASHSCOPE_REGION=beijing
```

### 3. 启动

```bash
./run.sh
```

首次会自动建 `.venv` 并装依赖。然后打开：

**http://127.0.0.1:8000**

点麦克风按钮，允许麦克风权限即可开始说话。

## 使用说明

- 点圆形麦克风开始；对话中再点一次是**静音/取消静音**；「结束」断开会话。
- 底部「试听」会播两声音，用来确认浏览器和系统输出没问题；和模型无关。
- 右下角会显示 `首包 xxx ms · 音频 3.4s`：有「音频」数字说明声音已经在播，听不到就是输出设备的问题。
- 用户一开口，本地会立刻丢掉还没播完的助手音频，实现打断。
- 底部输入框可以直接打字，走同一套会话上下文。
- **建议戴耳机**。外放时扬声器的声音会进麦克风，VAD 会把它当成用户插话。
- 页面已经开启浏览器回声消除（`echoCancellation`），但外放音量太大仍然会自打断。

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DASHSCOPE_API_KEY` | 必填 | 百炼 API Key |
| `DASHSCOPE_WORKSPACE_ID` | 必填 | 业务空间 ID |
| `DASHSCOPE_REGION` | `beijing` | `beijing` / `singapore` |
| `DASHSCOPE_WS_URL` | 空 | 完整地址，设置后优先于上面两项 |
| `OMNI_MODEL` | `qwen3.8-omni-flash-realtime` | 模型名 |
| `OMNI_VOICE` | `Tina` | 音色 |
| `OMNI_INSTRUCTIONS` | 小云人设 | 系统提示词 |
| `OMNI_SILENCE_MS` | `500` | 静音判定时长，越小越快、越容易误触发 |
| `OMNI_VAD` | `server_vad` | 可换 `semantic_vad` / `semantic_vad_ec` |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | 监听地址 |

想换音色或人设，改 `.env` 后重启即可；也可以直接在 `server.py` 的 `session_update()` 里加 `tools`、`enable_search` 等参数。

## 延迟怎么调

端到端延迟大致是：

```
网络 RTT + 静音判定(OMNI_SILENCE_MS) + 首包推理 + 播放缓冲(~20ms)
```

- 把 `OMNI_SILENCE_MS` 从 500 降到 350~400，响应会明显更跟手，但说话中间停顿容易被截断。
- 不要为了“更实时”把音频块改小到 20ms，包太碎反而增加开销；100ms 是稳妥值。
- 服务端已经在等 `session.updated` 之后才让浏览器开麦，避免首句配置没生效。

## 常见问题

**401 / invalid api key**
Key 和地域不匹配。北京空间用北京 Key，新加坡用新加坡 Key。

**等待 session.updated 超时 / 404**
`WorkspaceId` 或地域不对；也可能模型没开通。先用 `DASHSCOPE_WS_URL` 直接填控制台给的完整地址排除变量。

**页面提示“服务端还缺配置”**
`.env` 没填或没重启。`server.py` 只在启动时读一次环境变量。

**有转写、没声音**
- 先点页面上的「试听」。响了说明浏览器和系统输出正常，问题在上游；不响就是输出设备/静音。
- 看右下角有没有 `音频 x.xs`。有数字＝音频已经排进播放队列，听不到就去查系统音量、输出设备、标签页是否静音。
- 服务端计数：`curl -s http://127.0.0.1:8000/api/stats`。`audioPackets` 为 0 说明模型没下发音频，或上游连接断了。
- 浏览器地址必须是 `http://127.0.0.1:8000` 或 `http://localhost:8000`。用局域网 IP 走 HTTP 时，浏览器会禁用麦克风（非安全上下文）。
- 需要局域网/手机访问时，用 Caddy、mkcert 或 Cloudflare Tunnel 套一层 HTTPS。
- 检查系统输出设备，以及页面是否被静音。

**一直自打断**
戴耳机；或把 `OMNI_VAD` 改成 `semantic_vad` / `semantic_vad_ec`，并把 `OMNI_SILENCE_MS` 调大一点。

**麦克风没反应**
Chrome/Edge 地址栏左侧确认权限；Linux 下确认 `pactl list short sources` 有输入设备且未静音。

## 自测（不需要真实 Key）

```bash
.venv/bin/python tests/test_bridge.py
```

它会启动一个假的上游 WebSocket 和真实的 `server.py`，验证握手、二进制音频转发、base64 音频下行、转写透传和文本输入。改了桥接逻辑后先跑它。

## 目录结构
```
omni-voice-web/
├── server.py              # FastAPI + DashScope 桥接
├── static/
│   ├── index.html
│   ├── style.css
│   ├── app.js             # 采集、重采样、播放、打断、转写渲染
│   └── capture-worklet.js # 麦克风采集 AudioWorklet
├── requirements.txt
├── .env.example
└── run.sh
```

## 想跑成开机服务

```bash
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/omni-voice.service <<'EOF'
[Unit]
Description=Qwen Omni Voice Web
After=network-online.target

[Service]
WorkingDirectory=%h/Work/omni-voice-web
ExecStart=%h/Work/omni-voice-web/.venv/bin/python server.py
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now omni-voice.service
```

## 下一步可以加

- **接进 Hermes Desktop**：把它的「实时对话」按钮指到本服务，替代 OpenAI GPT-Live。方案与接口调研见 [`docs/hermes-live-integration.md`](docs/hermes-live-integration.md)。
- **Function Call / MCP**：命中工具时模型只回参数，不出音频；服务端执行后把 `function_call_output` 写回会话再 `response.create`。
- **视频**：浏览器定时抽帧，`conv.append_video()` / `input_image_buffer.append` 推给模型，配合 `representation_compact=normal` 省 token。
- **多通道**：3.8 支持 1/2/4 声道 16k PCM；需要绕过 SDK 直接发 `session.audio.input.format` 的 `channels` / `channel_layout`。
- **鉴权**：目前默认只监听 `127.0.0.1`。如果要暴露到公网，至少加一层访问口令或反向代理 Basic Auth。

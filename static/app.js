/* 小云 · 实时语音前端
 * 浏览器 <-> /ws <-> DashScope Realtime
 * 上行：16 kHz / 16 bit / 单声道 PCM，每 100 ms 一包
 * 下行：24 kHz / 16 bit / 单声道 PCM（服务端已从 base64 转成二进制）
 */

const $ = (id) => document.getElementById(id);

const CHUNK_SAMPLES = 1600; // 100 ms @ 16 kHz

const state = {
  ws: null,
  running: false,
  ready: false,
  muted: false,
  attempts: 0,

  playCtx: null,
  nextTime: 0,
  sources: new Set(),

  captureCtx: null,
  micStream: null,
  micSource: null,
  worklet: null,
  sink: null,
  micStarting: false,

  sendQueue: [],
  sendCount: 0,
  resampleBuf: new Float32Array(0),
  resamplePos: 0,

  userBubbles: new Map(),
  aiBubbles: new Map(),
  currentAi: null,
  pendingResponseAt: 0,

  firstByteMs: 0,
  audioPackets: 0,
  audioSeconds: 0,
};

const els = {
  status: $("status"),
  statusText: $("statusText"),
  subtitle: $("subtitle"),
  transcript: $("transcript"),
  empty: $("emptyState"),
  banner: $("configBanner"),
  micBtn: $("micBtn"),
  testBtn: $("testBtn"),
  muteBtn: $("muteBtn"),
  stopBtn: $("stopBtn"),
  textForm: $("textForm"),
  textInput: $("textInput"),
  meta: $("meta"),
  toast: $("toast"),
};

/* ---------- 基础 UI ---------- */

let toastTimer = null;
function toast(message) {
  els.toast.textContent = message;
  els.toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => els.toast.classList.remove("show"), 5000);
}

function setStatus(kind, text) {
  els.status.className = `status ${kind}`;
  els.statusText.textContent = text;
}

function setBusy(busy) {
  els.micBtn.classList.toggle("connecting", busy);
  els.micBtn.disabled = busy;
  els.muteBtn.disabled = !state.ready;
  els.stopBtn.disabled = !state.running;
}

function updateMicUi() {
  els.micBtn.classList.toggle("live", state.ready);
  els.micBtn.classList.toggle("muted", state.muted);
  const label = !state.running
    ? "开始对话"
    : !state.ready
      ? "连接中…"
      : state.muted
        ? "取消静音"
        : "静音";
  els.micBtn.setAttribute("aria-label", label);
  els.micBtn.title = label;
  els.muteBtn.textContent = state.muted ? "取消静音" : "静音";
}

function showBanner(html) {
  els.banner.innerHTML = html;
  els.banner.classList.remove("hidden");
}

function scrollDown() {
  els.transcript.scrollTop = els.transcript.scrollHeight;
}

function setLevel(value) {
  document.documentElement.style.setProperty("--level", value.toFixed(3));
}

/** 角落里的实时信息：首包延迟 + 已播音频时长，用来判断「有没有声音」 */
function updateMeta() {
  const parts = [];
  if (state.firstByteMs) parts.push(`首包 ${state.firstByteMs} ms`);
  if (state.audioPackets) {
    parts.push(`音频 ${state.audioSeconds.toFixed(1)}s`);
  }
  els.meta.textContent = parts.join(" · ");
}

/* ---------- 配置 ---------- */

async function loadConfig() {
  try {
    const response = await fetch("/api/config");
    const cfg = await response.json();
    els.subtitle.textContent = `${cfg.model} · ${cfg.voice}`;

    const missing = [];
    if (!cfg.apiKeyConfigured) missing.push("DASHSCOPE_API_KEY");
    if (!cfg.workspaceConfigured) missing.push("DASHSCOPE_WORKSPACE_ID");

    if (missing.length) {
      showBanner(
        `服务端还缺配置：${missing
          .map((item) => `<code>${item}</code>`)
          .join("、")}。编辑项目里的 <code>.env</code> 后重启服务。`,
      );
      els.micBtn.disabled = true;
    }
  } catch (error) {
    showBanner("无法读取服务端配置，请确认服务已启动。");
  }
}

/* ---------- 启动 / 停止 ---------- */

async function start() {
  if (state.running) return;
  state.running = true;
  state.attempts = 0;
  els.empty?.classList.add("hidden");
  setBusy(true);
  updateMicUi();

  try {
    if (!state.playCtx) {
      state.playCtx = new AudioContext({ latencyHint: "interactive" });
    }
    await state.playCtx.resume();
  } catch (error) {
    state.running = false;
    setBusy(false);
    updateMicUi();
    toast(`浏览器无法初始化音频：${error.message}`);
    return;
  }

  if (state.playCtx.state !== "running") {
    toast("浏览器挂起了音频输出，点一下页面空白处再试");
  }

  if (!state.running) return;
  connect();
}

function connect() {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${protocol}//${location.host}/ws`);
  ws.binaryType = "arraybuffer";
  state.ws = ws;

  setStatus("connecting", "连接中…");
  ws.onopen = () => setStatus("connecting", "建立会话…");
  ws.onmessage = onMessage;
  ws.onclose = onClose;
  ws.onerror = () => {};
}

function onClose(event) {
  if (event && event.target !== state.ws) return;

  state.ready = false;
  stopMic();
  flushAudio();
  updateMicUi();

  if (!state.running) {
    if (!els.status.classList.contains("error")) setStatus("idle", "未连接");
    return;
  }

  if (state.attempts < 3) {
    state.attempts += 1;
    setStatus("connecting", `连接断开，重试 ${state.attempts}/3…`);
    setTimeout(() => {
      if (state.running) connect();
    }, 800 * state.attempts);
  } else {
    state.running = false;
    setStatus("error", "连接已断开");
    setBusy(false);
    toast("连接已断开，请点击麦克风重新开始");
  }
}

function stop() {
  state.running = false;
  state.ready = false;
  state.attempts = 0;

  if (state.ws) {
    try {
      state.ws.close();
    } catch (_) {
      /* ignore */
    }
    state.ws = null;
  }

  stopMic();
  flushAudio();
  setStatus("idle", "未连接");
  setBusy(false);
  if (!els.transcript.querySelector(".msg")) {
    els.empty?.classList.remove("hidden");
  }
  els.meta.textContent = "";
  state.firstByteMs = 0;
  state.audioPackets = 0;
  state.audioSeconds = 0;
  state.muted = false;
  updateMicUi();
}

/* ---------- 麦克风采集 ---------- */

async function startMic() {
  if (state.micStream || state.micStarting) return;
  state.micStarting = true;

  let stream = null;
  let ctx = null;

  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      },
    });
    if (!state.running) return;

    ctx = new AudioContext({ latencyHint: "interactive" });
    await ctx.audioWorklet.addModule("/capture-worklet.js");
    await ctx.resume();
    if (!state.running) return;

    const source = ctx.createMediaStreamSource(stream);
    const worklet = new AudioWorkletNode(ctx, "capture-processor");
    const sink = ctx.createGain();
    sink.gain.value = 0; // 必须接到 destination，工作节点才会被驱动

    worklet.port.onmessage = (event) => handleFrames(event.data);
    source.connect(worklet);
    worklet.connect(sink);
    sink.connect(ctx.destination);

    state.captureCtx = ctx;
    state.micStream = stream;
    state.micSource = source;
    state.worklet = worklet;
    state.sink = sink;

    stream = null; // 所有权已交给 state
    ctx = null;
  } finally {
    state.micStarting = false;
    if (stream) stream.getTracks().forEach((track) => track.stop());
    if (ctx) ctx.close().catch(() => {});
  }
}

function stopMic() {
  if (state.worklet) {
    state.worklet.port.onmessage = null;
    try {
      state.worklet.disconnect();
    } catch (_) {
      /* ignore */
    }
  }
  if (state.micSource) {
    try {
      state.micSource.disconnect();
    } catch (_) {
      /* ignore */
    }
  }
  if (state.sink) {
    try {
      state.sink.disconnect();
    } catch (_) {
      /* ignore */
    }
  }
  if (state.micStream) {
    state.micStream.getTracks().forEach((track) => track.stop());
  }
  if (state.captureCtx) {
    state.captureCtx.close().catch(() => {});
  }

  state.worklet = null;
  state.micSource = null;
  state.sink = null;
  state.micStream = null;
  state.captureCtx = null;

  state.sendQueue = [];
  state.sendCount = 0;
  state.resampleBuf = new Float32Array(0);
  state.resamplePos = 0;
  setLevel(0);
}

function handleFrames(frames) {
  if (!state.ready || state.muted || !state.captureCtx) return;
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;

  const pcm = resampleToPcm16(frames, state.captureCtx.sampleRate);
  enqueuePcm(pcm);
  setLevel(rmsToLevel(frames));
}

/** 流式线性重采样到 16 kHz，并转成 Int16。 */
function resampleToPcm16(input, inputRate) {
  const outputRate = 16000;
  const merged = new Float32Array(state.resampleBuf.length + input.length);
  merged.set(state.resampleBuf, 0);
  merged.set(input, state.resampleBuf.length);

  const step = inputRate / outputRate;
  const out = [];
  let pos = state.resamplePos;

  while (pos + 1 < merged.length) {
    const index = Math.floor(pos);
    const frac = pos - index;
    out.push(merged[index] * (1 - frac) + merged[index + 1] * frac);
    pos += step;
  }

  const consumed = Math.floor(pos);
  state.resampleBuf = merged.slice(consumed);
  state.resamplePos = pos - consumed;

  const floats = Float32Array.from(out);
  const pcm = new Int16Array(floats.length);
  for (let i = 0; i < floats.length; i += 1) {
    const sample = Math.max(-1, Math.min(1, floats[i]));
    pcm[i] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
  }
  return pcm;
}

/** 攒满 1600 个采样（100 ms）再发，避免 WebSocket 包过碎。 */
function enqueuePcm(pcm) {
  if (!pcm.length) return;
  state.sendQueue.push(pcm);
  state.sendCount += pcm.length;

  while (state.sendCount >= CHUNK_SAMPLES) {
    const chunk = new Int16Array(CHUNK_SAMPLES);
    let offset = 0;

    while (offset < CHUNK_SAMPLES) {
      const head = state.sendQueue[0];
      const take = Math.min(head.length, CHUNK_SAMPLES - offset);
      chunk.set(head.subarray(0, take), offset);
      offset += take;
      if (take === head.length) state.sendQueue.shift();
      else state.sendQueue[0] = head.subarray(take);
    }

    state.sendCount -= CHUNK_SAMPLES;
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(chunk.buffer);
    }
  }
}

function rmsToLevel(frames) {
  let sum = 0;
  for (let i = 0; i < frames.length; i += 1) sum += frames[i] * frames[i];
  const rms = Math.sqrt(sum / frames.length);
  return Math.min(1, rms * 4.5);
}

/* ---------- 播放 ---------- */

function playPcm(buffer) {
  const ctx = state.playCtx;
  if (!ctx || buffer.byteLength < 2) return;

  const usable = buffer.byteLength - (buffer.byteLength % 2);
  const pcm = new Int16Array(buffer, 0, usable / 2);
  const floats = new Float32Array(pcm.length);
  for (let i = 0; i < pcm.length; i += 1) floats[i] = pcm[i] / 32768;

  const audioBuffer = ctx.createBuffer(1, floats.length, 24000);
  audioBuffer.copyToChannel(floats, 0);

  const source = ctx.createBufferSource();
  source.buffer = audioBuffer;
  source.connect(ctx.destination);

  const now = ctx.currentTime;
  if (state.nextTime < now + 0.02) state.nextTime = now + 0.02;

  source.start(state.nextTime);
  state.nextTime += audioBuffer.duration;

  state.sources.add(source);
  source.onended = () => state.sources.delete(source);

  state.audioPackets += 1;
  state.audioSeconds += audioBuffer.duration;
  updateMeta();
}

/** 播放一段两音提示音：听到 = 浏览器和系统输出都正常 */
async function playTestTone() {
  if (!state.playCtx) {
    state.playCtx = new AudioContext({ latencyHint: "interactive" });
  }
  try {
    await state.playCtx.resume();
  } catch (_) {
    /* ignore */
  }

  const ctx = state.playCtx;
  if (ctx.state !== "running") {
    toast("音频输出被浏览器挂起了，请点一下页面再点试听");
    return;
  }

  const start = ctx.currentTime + 0.03;
  [660, 880].forEach((freq, index) => {
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    const at = start + index * 0.2;
    osc.type = "sine";
    osc.frequency.value = freq;
    gain.gain.setValueAtTime(0.0001, at);
    gain.gain.exponentialRampToValueAtTime(0.18, at + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, at + 0.16);
    osc.connect(gain).connect(ctx.destination);
    osc.start(at);
    osc.stop(at + 0.18);
  });
}

/** 用户一开口就丢掉本地还没播完的音频（服务端已取消本轮回复）。 */
function flushAudio() {
  for (const source of state.sources) {
    try {
      source.stop();
    } catch (_) {
      /* ignore */
    }
  }
  state.sources.clear();
  state.nextTime = 0;
}

/* ---------- 消息处理 ---------- */

function onMessage(event) {
  if (event.data instanceof ArrayBuffer) {
    if (state.pendingResponseAt) {
      state.firstByteMs = Math.round(performance.now() - state.pendingResponseAt);
      state.pendingResponseAt = 0;
      updateMeta();
    }
    playPcm(event.data);
    return;
  }

  let message;
  try {
    message = JSON.parse(event.data);
  } catch (_) {
    return;
  }

  switch (message.type) {
    case "server.ready":
      state.ready = true;
      state.attempts = 0;
      setStatus("live", "对话中");
      updateMicUi();
      els.micBtn.disabled = false;
      setBusy(false);
      startMic().catch((error) => {
        toast(`麦克风不可用：${error.message}`);
      });
      break;

    case "server.error":
      toast(
        (message.error && message.error.message) || message.message || "未知错误",
      );
      // 会话还没建立就出错，多半是配置/地域问题，重试没有意义
      if (!state.ready && state.running) {
        state.running = false;
        setBusy(false);
        setStatus("error", "连接失败");
        updateMicUi();
      }
      break;

    case "error": {
      const detail =
        (message.error && message.error.message) || message.message || "未知错误";
      toast(detail);
      break;
    }

    case "input_audio_buffer.speech_started":
      flushAudio();
      break;

    case "conversation.item.input_audio_transcription.delta":
      updateUserBubble(
        message.item_id,
        `${message.text || ""}${message.stash || ""}`,
        false,
      );
      break;

    case "conversation.item.input_audio_transcription.completed":
      updateUserBubble(message.item_id, message.transcript || "", true);
      break;

    case "response.created":
      state.pendingResponseAt = performance.now();
      createAiBubble(message.response?.id || "response");
      break;

    case "response.audio_transcript.delta":
      appendAiText(message.response_id, message.delta || "", true);
      break;

    case "response.text.delta":
      appendAiText(message.response_id, message.delta || "", false);
      break;

    case "response.audio_transcript.done":
      finalizeAiBubble(message.response_id, message.transcript);
      break;

    case "response.text.done":
      finalizeAiBubble(message.response_id, message.text);
      break;

    case "response.done":
      finalizeAiBubble(message.response?.id, extractTranscript(message.response));
      break;

    default:
      break;
  }
}

/* ---------- 气泡渲染 ---------- */

function updateUserBubble(itemId, text, done) {
  const key = itemId || "pending-user";
  let bubble = state.userBubbles.get(key);

  if (!bubble) {
    const element = document.createElement("div");
    element.className = "msg user";
    element.innerHTML =
      '<div class="who">你</div><div class="bubble"><span class="text"></span></div>';
    els.transcript.appendChild(element);
    bubble = { element, textElement: element.querySelector(".text") };
    state.userBubbles.set(key, bubble);
  }

  bubble.textElement.textContent = text;
  if (done) bubble.element.classList.add("done");
  scrollDown();
}

function addUserText(text) {
  updateUserBubble(`text-${Date.now()}`, text, true);
}

function createAiBubble(responseId) {
  if (state.aiBubbles.has(responseId)) return state.aiBubbles.get(responseId);

  const element = document.createElement("div");
  element.className = "msg ai";
  element.innerHTML =
    '<div class="who">小云</div><div class="bubble"><span class="text"></span>' +
    '<span class="typing"><i></i><i></i><i></i></span></div>';
  els.transcript.appendChild(element);

  const bubble = {
    element,
    textElement: element.querySelector(".text"),
    text: "",
    source: null,
  };
  state.aiBubbles.set(responseId, bubble);
  state.currentAi = responseId;
  scrollDown();
  return bubble;
}

function appendAiText(responseId, delta, isAudioTranscript) {
  const id = responseId || state.currentAi || "response";
  const bubble = state.aiBubbles.get(id) || createAiBubble(id);

  const source = isAudioTranscript ? "audio" : "text";
  if (bubble.source && bubble.source !== source) {
    // 音频转写优先；如果之前先到了 text.delta，就丢掉换成音频转写，避免重复
    if (source === "text") return;
    bubble.text = "";
  }
  bubble.source = source;

  bubble.text += delta;
  bubble.textElement.textContent = bubble.text;
  scrollDown();
}

/** response.done 里也带完整文本，作为转写事件缺失时的兜底。 */
function extractTranscript(response) {
  if (!response || !Array.isArray(response.output)) return null;
  for (const item of response.output) {
    for (const part of item.content || []) {
      if (part.type === "audio" && part.transcript) return part.transcript;
      if (part.type === "text" && part.text) return part.text;
    }
  }
  return null;
}

function finalizeAiBubble(responseId, transcript) {
  const id = responseId || state.currentAi;
  if (!id) return;

  const bubble = state.aiBubbles.get(id);
  if (!bubble) return;

  if (transcript) {
    bubble.text = transcript;
    bubble.textElement.textContent = transcript;
  }

  if (!bubble.text) {
    // 工具调用等没有文本的 response，不留下空气泡
    bubble.element.remove();
    state.aiBubbles.delete(id);
    return;
  }

  bubble.element.querySelector(".typing")?.remove();
  scrollDown();
}

/* ---------- 交互 ---------- */

els.micBtn.addEventListener("click", () => {
  if (!state.running) {
    start();
    return;
  }
  if (!state.ready) return;

  state.muted = !state.muted;
  updateMicUi();
  setLevel(0);
});

els.muteBtn.addEventListener("click", () => {
  if (state.ready) els.micBtn.click();
});

els.stopBtn.addEventListener("click", stop);

els.testBtn.addEventListener("click", () => {
  playTestTone().catch((error) => toast(`试听失败：${error.message}`));
});

// 浏览器可能在任何时候挂起音频输出，任何一次交互都顺手恢复
["pointerdown", "keydown"].forEach((type) => {
  window.addEventListener(
    type,
    () => {
      if (state.playCtx && state.playCtx.state === "suspended") {
        state.playCtx.resume().catch(() => {});
      }
    },
    { passive: true },
  );
});

els.textForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = els.textInput.value.trim();
  if (!text || !state.ready || !state.ws || state.ws.readyState !== WebSocket.OPEN) {
    return;
  }

  flushAudio();
  state.ws.send(JSON.stringify({ type: "text", text }));
  addUserText(text);
  els.textInput.value = "";
});

window.addEventListener("beforeunload", () => {
  if (state.ws) state.ws.close();
});

loadConfig();

/**
 * 采集节点：把麦克风的 128 帧小块拼成 2048 帧再发给主线程，
 * 降低 postMessage 频率（约 23 次/秒 @ 48 kHz）。
 */
class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.size = 2048;
    this.buffer = new Float32Array(this.size);
    this.filled = 0;
  }

  process(inputs) {
    const channel = inputs[0] && inputs[0][0];
    if (!channel) return true;

    let offset = 0;
    while (offset < channel.length) {
      const take = Math.min(this.size - this.filled, channel.length - offset);
      this.buffer.set(channel.subarray(offset, offset + take), this.filled);
      this.filled += take;
      offset += take;

      if (this.filled === this.size) {
        const chunk = this.buffer.slice(0);
        this.port.postMessage(chunk, [chunk.buffer]);
        this.filled = 0;
      }
    }
    return true;
  }
}

registerProcessor("capture-processor", CaptureProcessor);

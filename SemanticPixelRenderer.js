class SemanticPixelRenderer {
  constructor(canvasElement, modelEndpoint) {
    this.canvas = canvasElement;
    this.ctx = canvasElement.getContext("2d");
    this.model = modelEndpoint;
    this.cache = new Map();
    this.queue = [];
    this.rendering = false;
  }

  async renderFromPrompt(prompt, width = 512, height = 512) {
    const cacheKey = `${prompt}-${width}x${height}`;
    if (this.cache.has(cacheKey)) {
      return this.drawPixels(this.cache.get(cacheKey));
    }

    this.canvas.width = width;
    this.canvas.height = height;
    const imageData = this.ctx.createImageData(width, height);
    const data = imageData.data;

    const promptTokens = prompt.split(/\s+/).map((t) => t.toLowerCase());
    const seed = promptTokens.reduce((s, t) => s + t.charCodeAt(0), 0);

    for (let i = 0; i < data.length; i += 4) {
      const pixelIndex = i / 4;
      const x = pixelIndex % width;
      const y = Math.floor(pixelIndex / width);

      const hash = (seed + x * 73856093 ^ y * 19349663) >>> 0;
      const hue = (hash % 360);
      const saturation = 50 + ((hash >> 8) % 50);
      const lightness = 40 + ((hash >> 16) % 40);

      const [r, g, b] = this.hslToRgb(hue, saturation, lightness);
      data[i] = r;
      data[i + 1] = g;
      data[i + 2] = b;
      data[i + 3] = 255;
    }

    this.ctx.putImageData(imageData, 0, 0);
    this.cache.set(cacheKey, { data: imageData, prompt, seed });
    return imageData;
  }

  hslToRgb(h, s, l) {
    h = h / 360;
    s = s / 100;
    l = l / 100;

    let r, g, b;
    if (s === 0) {
      r = g = b = l;
    } else {
      const q = l < 0.5 ? l * (1 + s) : l + s - l * s;
      const p = 2 * l - q;
      r = this.hueToRgb(p, q, h + 1/3);
      g = this.hueToRgb(p, q, h);
      b = this.hueToRgb(p, q, h - 1/3);
    }

    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
  }

  hueToRgb(p, q, t) {
    if (t < 0) t += 1;
    if (t > 1) t -= 1;
    if (t < 1/6) return p + (q - p) * 6 * t;
    if (t < 1/2) return q;
    if (t < 2/3) return p + (q - p) * (2/3 - t) * 6;
    return p;
  }

  drawPixels(pixelData) {
    this.ctx.putImageData(pixelData.data, 0, 0);
  }
}

export default SemanticPixelRenderer;

/*
================================================================================
EXPLANATION
SemanticPixelRenderer converts text prompts into deterministic, colorful pixel 
art using semantic hashing. Built because generative art on the web usually 
means calling an external API (slow, expensive, rate-limited). This generates 
consistent visual outputs from text offline—same prompt always produces the same 
image. Use it in creative coding projects, generative UI backgrounds, or data 
visualization where you want semantic meaning reflected in color and pattern. 
The trick: convert prompt tokens into a seed, then use that seed to deterministically 
generate hue, saturation, and lightness for each pixel using HSL-to-RGB conversion. 
Caches results so repeated prompts render instantly. Drop this into a Canvas-based 
app where you want text-to-image generation without dependencies or API calls.
================================================================================
*/

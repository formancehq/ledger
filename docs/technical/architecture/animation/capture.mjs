// Capture frames of the architecture animation using headless Chromium.
//
// Usage:
//   node capture.mjs [--fps=15] [--duration=12] [--width=1200] [--height=720] [--out=frames]
//
// Requires: puppeteer (`npm i puppeteer` or run via the capture.sh wrapper).
import puppeteer from "puppeteer";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ""), true];
  })
);
const fps = Number(args.fps ?? 15);
const durationSec = Number(args.duration ?? 12);
const width = Number(args.width ?? 1200);
const height = Number(args.height ?? 720);
const outDir = args.out ?? "frames";

const here = path.dirname(fileURLToPath(import.meta.url));
const indexHtml = "file://" + path.join(here, "index.html");
const framesDir = path.join(here, outDir);

if (existsSync(framesDir)) await rm(framesDir, { recursive: true });
await mkdir(framesDir, { recursive: true });

const browser = await puppeteer.launch({
  headless: "new",
  defaultViewport: { width, height, deviceScaleFactor: 2 },
});
const page = await browser.newPage();
await page.goto(indexHtml, { waitUntil: "load" });

// Wait a moment for animation to start
await new Promise((r) => setTimeout(r, 600));

const totalFrames = Math.round(fps * durationSec);
const interval = 1000 / fps;

console.log(`Capturing ${totalFrames} frames @ ${fps}fps (${durationSec}s)...`);
const t0 = Date.now();
for (let i = 0; i < totalFrames; i++) {
  const target = t0 + i * interval;
  const wait = target - Date.now();
  if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  const file = path.join(framesDir, `frame_${String(i).padStart(4, "0")}.png`);
  await page.screenshot({ path: file, omitBackground: false });
  process.stdout.write(`\r  frame ${i + 1}/${totalFrames}`);
}
process.stdout.write("\n");

await browser.close();

await writeFile(
  path.join(here, "frames.meta.json"),
  JSON.stringify({ fps, durationSec, width, height, frames: totalFrames }, null, 2)
);

console.log(`Done. Frames in ${framesDir}.`);
console.log(`Assemble GIF with:`);
console.log(`  ffmpeg -y -framerate ${fps} -i ${outDir}/frame_%04d.png -vf "fps=${fps},split[s0][s1];[s0]palettegen=stats_mode=full[p];[s1][p]paletteuse=dither=sierra2_4a" architecture.gif`);

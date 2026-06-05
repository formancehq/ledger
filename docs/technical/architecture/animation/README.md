# Architecture animation

Dynamic visualization of the order flow through the system: **gRPC → Routed Controller → Admission → Raft (3 nodes, WAL side-persistence) → FSM → {Cache, Pebble} → Index Builder**. Style inspired by [raft.github.io](https://raft.github.io/).

## Files

| File | Purpose |
|------|---------|
| `index.html` | Self-contained SVG/JS animation. Open directly in any browser. |
| `capture.mjs` | Headless Chromium frame capture (Puppeteer). |
| `capture.sh` | Wrapper: installs puppeteer if needed, captures frames, assembles `architecture.gif` via `ffmpeg`. |
| `architecture.gif` | Generated GIF (after running `capture.sh`). |

## View

```bash
open docs/technical/architecture/animation/index.html
```

The animation loops continuously and each step is narrated in an HTML banner above the SVG — a short title plus a richer description that names the actual code paths and Raft mechanisms involved (CheckCache, MirrorPreload, Ready loop, MsgApp, WAL fsync, …).

The leader sits at the **bottom** of the Raft column so the read-only `Admission ↔ Cache/Pebble` consultation arcs don't have to traverse the whole diagram. WAL is extracted as a **separate red-bordered box** below each Raft node — same structural principle as `Cache` / `Pebble` sitting below `FSM` — and is offset to the right (x=650) so the `Leader → Followers` replication channel at x=580 stays clear.

### Controls

A toolbar above the diagram lets you walk through the flow:

| Button | Behaviour |
|--------|-----------|
| `⏸ Pause` / `▶ Play` | Toggle continuous playback. When paused, the indicator highlights the current step in yellow. |
| `⏭ Next step` | Available only while paused: runs the next step and stops again. |
| `⟲ Restart` | Clear all in-flight dots and jump back to step ①. |

The right-hand indicator (`Step n / N`) shows the step about to run (or currently running). Steps are intentionally fine-grained (13 total) so `Next` advances one box transition (or one sub-action) at a time — every traversal between two boxes is its own step (`Client → gRPC`, `gRPC → Ctrl`, `Ctrl → Admission`, …), the preload sub-cycle has separate stops for the `Admission ↔ Cache` and `Admission ↔ Pebble` round-trips, and the apply path has separate stops for `Raft → FSM`, `payload → Cache`, and the `Cache + Pebble` fan-out.

## Regenerate the GIF

Requires `node >= 18` and `ffmpeg`.

```bash
cd docs/technical/architecture/animation
./capture.sh
```

Tunables (env vars): `FPS` (default 15), `DURATION` (default 12s), `WIDTH`/`HEIGHT`.

```bash
FPS=20 DURATION=14 ./capture.sh
```

## Editing the scene

The animation is plain SVG plus a small JS scheduler at the bottom of `index.html`. Coordinates live **only** in the SVG `d=` attributes — there are no JS-side anchor tables to keep in sync:

- **Static edges** — every `<path>` between two boxes has a unique `id="e-…"`. Move a box → update only the path's `d` attribute.
- **Dot animation** — `animateOnPaths(dot, [{ id, reverse? }, …], duration)` walks the rendered geometry via `getPointAtLength`. Dots literally ride the static paths, so they cannot drift away from arcs by construction.
- **Steps** — the `steps` array describes each sub-step as `{ title, desc, color, action }`. Each `action` calls `anim(...)` referencing one or more path IDs.
- **Banner** — `setStep(title, desc, color)` updates the HTML banner above the diagram.

No build step, no dependencies for the HTML itself.

# Architecture animation v2 (Svelte 5)

Component-based rewrite of the plain-JS animation in `../animation/`. Built with
Svelte 5 (runes) + Vite, produces a single self-contained `dist/index.html`.

## Files

| Path | Purpose |
|------|---------|
| `index.html`                 | Vite entry. |
| `src/main.js`                | Mounts `App.svelte` on `#app`. |
| `src/App.svelte`             | Root layout — wires the runtime bus to banner / highlights. |
| `src/components/*.svelte`    | One panel per file (Diagram, TxForm, TxControls, Inflight, Cache, Pebble, Payload, StepBanner). |
| `src/lib/state.svelte.js`    | Single reactive store (`$state`) — `raft`, `cache`, `pebble`, `inflight`, `history`, `paused`, … |
| `src/lib/cache.js`           | Two-generation cache model (no DOM). |
| `src/lib/pebble.js`          | Durable Map mirror. |
| `src/lib/locks.js`           | Lock factory with `batchGrants` for Next-click waves. |
| `src/lib/anim.js`            | `getPointAtLength` dot animation (imperative). |
| `src/lib/dots.js`            | Rest / blocked dot lifecycles. |
| `src/lib/payloadBuilders.js` | Pure HTML payload builders (paint`...`). |
| `src/lib/steps.js`           | The 13-step pipeline definition. |
| `src/lib/gates.js`           | `awaitResumeOrNext`, `raftLock`, `fsmLock`, cancel check. |
| `src/lib/runCycle.js`        | Async step loop (talks to UI via a small bus). |
| `src/lib/snapshot.js`        | Previous-button history (take/pop/apply). |
| `src/lib/controls.js`        | Send / Repeat / Pause / Next / Previous / Restart handlers. |
| `src/lib/geometry.js`        | All SVG anchors + step-rest / block positions. |
| `src/lib/colors.js`          | Stage colour tokens. |
| `src/lib/tx.js`              | `makeTx()` factory. |

## Run

```bash
cd docs/technical/architecture/animation-v2
npm install
npm run dev      # HMR dev server on http://localhost:5173
npm run build    # → dist/index.html (single file, drop-anywhere)
npm run preview  # serve the built artefact
```

## Design

- **Single reactive store** (`state.svelte.js`) keeps `raft`, `cache`, `pebble`,
  `inflight`, `history`, `paused`, etc. Components read from the same proxy and
  re-render automatically; mutations from `steps.js` / `controls.js` /
  `runCycle.js` go through that store.
- **No DOM in the lib/**. The runtime asks the host to do banner / payload
  writes via a tiny bus (`bindRunBus`). Live SVG dots stay imperative for
  performance (RAF + `setAttribute`), routed through `src/lib/anim.js`.
- **Previous** snapshots the full reactive state (`structuredClone` for Maps),
  cancels current loops via per-tx `tx.cancelled`, restores, then re-spawns
  fresh `runStepsLoop` instances that park at the resume gate.

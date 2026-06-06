import { app } from "./state.svelte.js";
import { anim, makeDot } from "./anim.js";

// Background log compaction. Every COMPACT_INTERVAL_MS the compactor goes
// to Pebble to read the durably-persisted lastAppliedIndex, then issues a
// wal.Truncate(<lastAppliedIndex) on the leader's WAL. The first index of
// the WAL moves up to lastAppliedIndex; lower entries can be garbage
// collected on disk in a follow-up pass.
//
// Wall-clock driven (setInterval), independent of app.paused — compaction
// runs even while the pipeline animation is stopped, mirroring the real
// server's background goroutine.
const COMPACT_INTERVAL_MS = 3000;
let timer = null;
let running = false;

export function startCompactor() {
  if (timer !== null) return;
  timer = setInterval(tick, COMPACT_INTERVAL_MS);
}

export function stopCompactor() {
  if (timer === null) return;
  clearInterval(timer);
  timer = null;
}

async function tick() {
  if (running) return;
  running = true;
  try {
    const dot = makeDot("#ff6b6b", 5, "compactor");
    // Round-trip to Pebble — the compactor has no idea whether there's
    // anything to compact until it reads lastAppliedIndex. Fires every
    // interval, even on an idle pipeline (mirrors the real background
    // goroutine that probes Pebble on a timer).
    await anim(dot, "e-compactor-pebble", 700);
    const applied = app.raft.leaderApplied;
    await anim(dot, [{ id: "e-compactor-pebble", reverse: true }], 700);
    // Only issue the truncate when the read value actually exceeds the
    // current floor — otherwise the round-trip ends right here and the
    // WAL bounds stay put. Mirrors the real wal.Truncate guard.
    if (applied > app.wal.leaderFirst) {
      await anim(dot, "e-compactor-wal", 500);
      // Re-check at apply time so a Restart during the animation can't
      // resurrect a stale truncation — Restart resets leaderApplied to 1,
      // the guard naturally short-circuits.
      if (app.raft.leaderApplied > app.wal.leaderFirst) {
        app.wal.leaderFirst = app.raft.leaderApplied;
      }
    }
    dot.parentNode?.removeChild(dot);
  } finally {
    running = false;
  }
}

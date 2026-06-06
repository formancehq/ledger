import { app, resumeWaiters, cache, pebble } from "./state.svelte.js";
import { raftLock, fsmLock, channelLock, consumeAndWake, fireReadyTick, drainReadyTick } from "./gates.js";
import { STAGGER_MS } from "./geometry.js";
import { runCycle } from "./runCycle.js";
import { makeTx } from "./tx.js";
import { clearTxDots } from "./dots.js";
import { clearAllDots } from "./anim.js";

// ── Pause / Resume ──────────────────────────────────────────────────────
export function togglePause() {
  if (app.inflight.length === 0) return;
  app.paused = !app.paused;
  if (!app.paused) {
    // Same per-tx visual stagger as stepNext — otherwise every awaiter
    // wakes in the same microtask and the kick-off wave looks like one
    // merged blob instead of N independent goroutines.
    const waiters = resumeWaiters.splice(0);
    waiters.forEach((item, i) => { item.tx.staggerMs = i * STAGGER_MS; });
    waiters.forEach(({ res }) => res());
    // Drain the Ready tick AFTER microtasks — let any tx already waiting
    // at joinReadyTick (or one that wakes from a resumeWaiter and pushes)
    // be in pendingProposals when the drain fires. Subsequent arrivals in
    // continuous mode are handled by the READY_TICK_WINDOW_MS timer.
    Promise.resolve().then(fireReadyTick);
  }
}

// ── Next ────────────────────────────────────────────────────────────────
// Strict step-by-step: only txs CURRENTLY parked at the pause gate
// (resumeWaiters) wake on this click. No global token grant — clicks during
// a mid-action anim become no-ops, so the tx always pauses again at its
// next gate even if the user clicks ahead.
export function stepNext() {
  if (!app.paused || app.inflight.length === 0) return;
  const waiters = resumeWaiters.splice(0);
  waiters.forEach((item, i) => { item.tx.staggerMs = i * STAGGER_MS; });
  waiters.forEach(consumeAndWake);
  // Drain the Ready tick AFTER microtasks — txs woken from resumeWaiters
  // might push to pendingProposals on this same tick. The microtask defer
  // lets them push before we drain, so the whole queue forms one batch.
  Promise.resolve().then(fireReadyTick);
}

// ── Restart ─────────────────────────────────────────────────────────────
export function restart() {
  app.cancelRequested = true;
  app.paused = false;
  resumeWaiters.splice(0).forEach(({ res }) => res());
  // Also wake anyone parked at the Ready tick gate (post-①f). Without this
  // they'd stay in pendingProposals as zombies — when a NEW tx is later
  // sent and joins the tick, the zombies wake too and try to drive a batch
  // through ②③④⑤ with stale state, which corrupts the cache.
  drainReadyTick().forEach(({ res }) => res());
  for (const t of app.inflight) { clearTxDots(t); t.cancelled = true; }
  app.inflight.length = 0;
  app.completed.length = 0;
  app.selectedTxId = null;
  app.activeActions = 0;
  raftLock.reset(); fsmLock.reset(); channelLock.reset();
  app.lastTxData = null;
  app.raft = {
    term: 1, leaderIdx: 0, f1Match: 0, f2Match: 0,
    leaderApplied: 0, f1Applied: 0, f2Applied: 0,
  };
  app.wal = { leaderFirst: 1, leaderLast: 0 };
  cache.reset();
  pebble.clear();
  clearAllDots();
  // Reset cancel flag after the current macrotask so already-running loops
  // have a chance to bail. New sends after this point start fresh.
  setTimeout(() => { app.cancelRequested = false; }, 0);
}

// ── Send / Repeat ───────────────────────────────────────────────────────
export function sendTx(form) {
  app.txSeq += 1;
  const tx = makeTx({ form, id: app.txSeq });
  runCycle(tx);
}
export function repeatLast() {
  if (!app.lastTxData) return;
  sendTx({ ...app.lastTxData });
}

// ── Tx selection (for the payload panel) ────────────────────────────────
export function selectTx(id) { app.selectedTxId = id; }

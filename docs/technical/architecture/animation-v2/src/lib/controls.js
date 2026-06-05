import { cache, pebble } from "./state.svelte.js";
import { app, resumeWaiters } from "./state.svelte.js";
import { raftLock, fsmLock, consumeAndWake } from "./gates.js";
import { STAGGER_MS } from "./geometry.js";
import { runCycle } from "./runCycle.js";
import { makeTx } from "./tx.js";
import { clearTxDots } from "./dots.js";
import { clearAllDots } from "./anim.js";
import { takeCheckpoint, popCheckpoint, clearHistory } from "./snapshot.js";

// ── Pause / Resume ──────────────────────────────────────────────────────
export function togglePause() {
  if (app.inflight.length === 0) return;
  app.paused = !app.paused;
  if (!app.paused) {
    // Resuming continuous playback: drop any leftover Next credits so the
    // first wave of txs doesn't accidentally double-step.
    for (const t of app.inflight) t.nextTokens = 0;
    resumeWaiters.splice(0).forEach(({ res }) => res());
  }
}

// ── Next ────────────────────────────────────────────────────────────────
// One click ⇒ each in-flight tx earns one step credit, and the pause gate
// (resumeWaiters) is drained so waiters at awaitResumeOrNext can proceed.
//
// We DON'T touch raftLock / fsmLock here. Those are real serializers — at any
// given time only one tx can hold each. Pre-arming batchGrants used to let a
// whole wave cross the mutex on a single click, which faithfully broke the
// Raft semantics: a second tx arriving at ①f must wait for the first to have
// WAL-written its batch (the leader is single-threaded across wal.Append),
// not race through alongside it. Letting the queue stay queued is the right
// behaviour — when the holder calls release() inside step ②'s action, the
// queued tx wakes naturally.
export function stepNext() {
  if (!app.paused || app.inflight.length === 0) return;
  for (const t of app.inflight) t.nextTokens += 1;
  const waiters = resumeWaiters.splice(0);
  waiters.forEach((item, i) => { item.tx.staggerMs = i * STAGGER_MS; });
  waiters.forEach(consumeAndWake);
}

// ── Previous ────────────────────────────────────────────────────────────
export function stepPrevious() { popCheckpoint(); }

// ── Restart ─────────────────────────────────────────────────────────────
export function restart() {
  app.cancelRequested = true;
  app.paused = false;
  resumeWaiters.splice(0).forEach(({ res }) => res());
  for (const t of app.inflight) { clearTxDots(t); t.cancelled = true; t.nextTokens = 0; }
  app.inflight.length = 0;
  app.completed.length = 0;
  app.selectedTxId = null;
  clearHistory();
  app.activeActions = 0;
  raftLock.reset(); fsmLock.reset();
  app.lastTxData = null;
  app.raft = {
    term: 1, leaderIdx: 1, f1Match: 1, f2Match: 1,
    leaderApplied: 1, f1Applied: 1, f2Applied: 1,
  };
  cache.reset();
  pebble.clear();
  clearAllDots();
  // Reset cancel flag after the current macrotask so already-running loops
  // have a chance to bail. New sends after this point start fresh.
  setTimeout(() => { app.cancelRequested = false; }, 0);
}

// ── Send / Repeat ───────────────────────────────────────────────────────
export function sendTx(form) {
  takeCheckpoint();
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

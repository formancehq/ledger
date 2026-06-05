import { app, resumeWaiters } from "./state.svelte.js";
import { makeLock } from "./locks.js";
import { BATCHED_STAGGER_MS } from "./geometry.js";
import { showBlockedDot, clearBlockedDot } from "./dots.js";

// Both pipeline locks live here so steps and the Next/Restart handlers share
// the same instances. The block/unblock hooks drive the red-pulse visualisation.
function onBlock(tx)   { tx.status = "blocked"; showBlockedDot(tx); }
function onUnblock(tx) { tx.status = "running"; clearBlockedDot(tx); }

// raftLock = admission→leader gate. The drain wave at this point IS the
// batching moment (N admission requests → 1 Raft proposal), so it uses zero
// stagger to leave admission as one tight group.
export const raftLock = makeLock({ onBlock, onUnblock, staggerMs: BATCHED_STAGGER_MS }); // ②③④
// fsmLock = leader→FSM gate. Apply is sequential one entry at a time, so the
// default per-tx stagger keeps the visual readable on the rare cases where
// more than one tx ends up queued.
export const fsmLock  = makeLock({ onBlock, onUnblock });   // ⑤a-⑤c

// Pause/Next gate. Per-tx tokens (granted 1 per Next click) ensure each tx
// always advances by exactly one step per click, regardless of whether the tx
// was already at the gate or still finishing an action at click time.
export function awaitResumeOrNext(tx) {
  if (tx.nextTokens > 0) {
    tx.nextTokens--;
    return Promise.resolve();
  }
  if (!app.paused) return Promise.resolve();
  return new Promise(res => resumeWaiters.push({ res, tx }));
}

// Single helper to wake any waiter list (lock queue or resume waiters) while
// consuming one token per woken tx. Used by Next-driven drains.
export function consumeAndWake(item) {
  if (item.tx.nextTokens > 0) item.tx.nextTokens--;
  item.res();
}

export function isCancelled(tx) {
  return app.cancelRequested || tx.cancelled;
}

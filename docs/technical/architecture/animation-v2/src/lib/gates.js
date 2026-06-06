import { app, resumeWaiters, proxyOf } from "./state.svelte.js";
import { makeLock } from "./locks.js";
import { BATCHED_STAGGER_MS, STAGGER_MS, READY_TICK_WINDOW_MS } from "./geometry.js";
import { showBlockedDot, clearBlockedDot } from "./dots.js";
import { COLORS } from "./colors.js";

// Both pipeline locks live here so steps and the Next/Restart handlers share
// the same instances. The block/unblock hooks drive the red-pulse visualisation.
function onBlock(tx)   { tx.status = "blocked"; showBlockedDot(tx); }
function onUnblock(tx) { tx.status = "running"; clearBlockedDot(tx); }

// formBatch is now driven by the Ready tick gate (joinReadyTick /
// fireReadyTick below), NOT by raftLock release. raftLock just serializes
// successive batches once they've already been formed at the tick boundary.
function formBatch(txs) {
  let resolveApplied;
  const applied = new Promise(res => { resolveApplied = res; });
  const batch = {
    leadId:        txs[0].id,
    members:       txs.slice(1),         // bare refs; lead drives the lifecycle
    applied,                             // resolves when lead's ⑤c is done
    resolveApplied,
  };
  // Set batch refs on BOTH the bare tx (consumed by runCycle's lock branches)
  // and the Svelte proxy in app.inflight (so the In-flight panel can tell
  // members from solo txs). Skipping the proxy means UI stays stale.
  for (const tx of txs) {
    tx.batch = batch;
    const proxy = proxyOf(tx);
    if (proxy) proxy.batch = batch;
  }
  txs[0].batchLead = true;
  const leadProxy = proxyOf(txs[0]);
  if (leadProxy) leadProxy.batchLead = true;
  // Drop a synthetic lifecycle event on each member so their right-panel
  // timeline doesn't go silent through ②③④⑤ — without this they'd jump
  // from ①e straight to ⑥ with no indication that they were folded into
  // a batched entry. Route via the Svelte proxy in app.inflight (bare ref
  // updates don't propagate to subscribers).
  for (let i = 1; i < txs.length; i++) {
    const member = txs[i];
    const proxy = proxyOf(member);
    if (!proxy) continue;
    proxy.timeline = [...proxy.timeline, {
      stepIndex: 5,
      color:     COLORS.raft,
      title:     `①f Absorbed into Node WAL + FSM-apply batch (lead tx#${txs[0].id})`,
      html: `<p>Queued at the admission→leader gate while a prior batch was being WAL-written. When the gate drained, ${txs.length} requests woke together — each will still be its own Raft entry with its own <code>commitIndex</code> (etcd/raft assigns indices per Propose), but they share the SAME Ready-loop tick: one <code>wal.Append([entries…])</code>, one <code>FSM.applyProposal</code> loop, one <code>pebble.Batch</code> over all N entries' writes.</p><p>This member skips ②③④⑤ in its own loop — the lead drives them on behalf of the batch. This tx re-enters at ⑥ to send its own ApplyResponse to its own client once the apply tail resolves.</p>`,
    }];
  }
}
// raftLock = serializer between successive Raft batches. Strict mutex: only
// one batch's lead is in ②③④ at a time; subsequent leads wait until the
// holder finishes step ②. Batching itself happens at joinReadyTick (above
// in semantic flow, below in this file).
export const raftLock = makeLock({ onBlock, onUnblock });   // ②③④
// channelLock = the leader→applier channel slot (make(chan applyWork, 1)
// in internal/infra/node/applier.go). Capacity 1: only one batch sits in
// the channel at a time. Subsequent submits block at the leader's right
// edge (red-pulse blockedDot) until the applier drains the slot. Held
// from the moment the lead enters ⑤a until the applier picks up at
// fsmLock acquire.
export const channelLock = makeLock({ onBlock, onUnblock });
// fsmLock = the applier itself. Capacity 1, but NO blockedDot — the lead
// holding the channel slot is the one waiting here, and its midpoint dot
// already shows that visually. A second blockedDot at leaderBlock would
// double-display the same wait.
export const fsmLock  = makeLock();                          // ⑤a-⑥a (applier)

// Pause/Next gate. Strict step-by-step: a Next click ONLY wakes txs that
// are currently parked at this gate (in resumeWaiters). Txs that are
// mid-action when the click happens are NOT pre-credited with a token —
// they'll have to wait for the next click after they reach the gate. This
// avoids the "I clicked once but the tx advanced two steps" surprise.
export function awaitResumeOrNext(tx) {
  if (!app.paused) return Promise.resolve();
  return new Promise(res => resumeWaiters.push({ res, tx }));
}

// Single helper to wake a queued waiter. Step-by-step mode dropped the
// token mechanism, so this is now just `item.res()`.
export function consumeAndWake(item) {
  item.res();
}

export function isCancelled(tx) {
  return app.cancelRequested || tx.cancelled;
}

// ── Ready tick gate ────────────────────────────────────────────────────────
// Mirror of the raft.Node Ready loop: txs that finished ①f independently
// (Propose() in real Go) accumulate at the leader. Each "tick" drains the
// whole pool and batches it into ONE entry — including the first arrival.
//
// Triggers:
//   - Paused mode: stepNext() calls fireReadyTick() after microtasks so any
//     tx waking from the resumeWaiter pause-gate has a chance to push to
//     pendingProposals on the same click before the drain.
//   - Continuous mode (post-Resume, app.paused=false): the first arrival
//     schedules pendingTickTimer (READY_TICK_WINDOW_MS). Other arrivals
//     during the window join the same tick. At expiry, fireReadyTick drains.
//   - Restart: drainReadyTick() wakes everyone without batching so
//     cancelled txs can bail out cleanly.
//
// Note: serialization between successive batches is still enforced by
// raftLock — the lead acquires it AFTER the Ready tick wake, releases at
// walDone in step ②'s action. So only one batch is in ②③④ at a time.
const pendingProposals = [];
let pendingTickTimer = null;

export function joinReadyTick(tx) {
  const promise = new Promise(res => pendingProposals.push({ res, tx }));
  if (!app.paused && pendingTickTimer === null) {
    pendingTickTimer = setTimeout(() => {
      pendingTickTimer = null;
      fireReadyTick();
    }, READY_TICK_WINDOW_MS);
  }
  return promise;
}

export function fireReadyTick() {
  if (pendingTickTimer !== null) {
    clearTimeout(pendingTickTimer);
    pendingTickTimer = null;
  }
  const drained = pendingProposals.splice(0);
  if (drained.length === 0) return;
  if (drained.length > 1) formBatch(drained.map(d => d.tx));
  drained.forEach((item, i) => { item.tx.staggerMs = i * STAGGER_MS; });
  drained.forEach(({ res }) => res());
}

// Drain + wake without batching — for Restart paths. Returns the drained
// items so callers can decide what to do with them (typically just wake to
// let bail() run on isCancelled).
export function drainReadyTick() {
  if (pendingTickTimer !== null) {
    clearTimeout(pendingTickTimer);
    pendingTickTimer = null;
  }
  return pendingProposals.splice(0);
}

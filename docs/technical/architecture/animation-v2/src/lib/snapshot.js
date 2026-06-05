import { app, cache, pebble, resumeWaiters } from "./state.svelte.js";
import { raftLock, fsmLock } from "./gates.js";
import { clearTxDots, placeRestDot } from "./dots.js";
import { clearAllDots } from "./anim.js";
import { runStepsLoop } from "./runCycle.js";

// Deep-clone helper that strips Svelte 5 reactive proxies. structuredClone
// throws DataCloneError on those because the proxy's internal slots aren't
// transferable. Walking the structure with property access goes through the
// proxy's [[Get]] traps and produces a plain mirror.
function plain(x) {
  if (x === null || typeof x !== "object") return x;
  if (Array.isArray(x)) return x.map(plain);
  const out = {};
  for (const k of Object.keys(x)) out[k] = plain(x[k]);
  return out;
}

// Per-tx snapshot — only the logical fields. DOM refs (restDot, blockedDot)
// are NOT captured; they're rebuilt from stepIndex on restore.
function snapshotTx(t) {
  return {
    id: t.id, color: t.color,
    source: t.source, destination: t.destination, asset: t.asset, amount: t.amount, ledger: t.ledger,
    referenceId: t.referenceId,
    stepIndex: t.stepIndex,
    timeline: t.timeline.map(e => ({ ...e })),
    proposalPlan: t.proposalPlan ? plain(t.proposalPlan) : null,
    proposalPlanStale: t.proposalPlanStale ? plain(t.proposalPlanStale) : null,
    preloadDecisions: t.preloadDecisions ? plain(t.preloadDecisions) : null,
    commitTerm: t.commitTerm, commitIndex: t.commitIndex,
    appliedLogId: t.appliedLogId,
    appliedTxId: t.appliedTxId,
    appliedPostingCount: t.appliedPostingCount,
  };
}

// Coarse identity used to dedup adjacent snapshots that capture the same
// logical state — a batched Next wave fires takeCheckpoint() once per tx but
// none of them have actually advanced yet, so we keep just one entry.
function snapshotKey(snap) {
  const ids = snap.txs.map(t => `${t.id}@${t.stepIndex}`).join("|");
  return `${ids}::${snap.raft.leaderIdx}/${snap.raft.leaderApplied}`;
}

export function takeCheckpoint() {
  const snap = {
    raft:    { ...app.raft },
    cache:   cache.snapshot(),
    pebble:  pebble.snapshot(),
    txs:     app.inflight.map(snapshotTx),
    // Completed list shrinks when Previous rolls back a tx that finished — we
    // snapshot the whole history list so the UI stays consistent.
    completed: app.completed.map(plain),
    lastTxData: app.lastTxData ? { ...app.lastTxData } : null,
    selectedTxId: app.selectedTxId,
    txSeq:        app.txSeq,
  };
  const prev = app.history.at(-1);
  if (prev && snapshotKey(prev) === snapshotKey(snap)) return;
  app.history.push(snap);
}

export function popCheckpoint() {
  if (app.history.length === 0 || app.activeActions > 0) return;
  const snap = app.history.pop();
  applyCheckpoint(snap);
}

export function applyCheckpoint(snap) {
  // 1. Cancel current loops, wake all gates so they can exit cleanly.
  for (const t of app.inflight) t.cancelled = true;
  raftLock.queue.splice(0).forEach(({ res }) => res());
  fsmLock.queue.splice(0).forEach(({ res }) => res());
  resumeWaiters.splice(0).forEach(({ res }) => res());
  raftLock.reset(); fsmLock.reset();

  // 2. Wipe the SVG dots layer (live + parked) and any step-driven highlights.
  for (const t of app.inflight) clearTxDots(t);
  clearAllDots();

  // 3. Restore the reactive state in one assignment per slot.
  app.raft = { ...snap.raft };
  cache.restore(snap.cache);
  pebble.restore(snap.pebble);
  app.txSeq         = snap.txSeq;
  app.selectedTxId  = snap.selectedTxId;
  app.lastTxData    = snap.lastTxData ? { ...snap.lastTxData } : null;
  app.completed.length = 0;
  for (const t of (snap.completed || [])) app.completed.push(plain(t));
  app.paused        = true;          // Previous always parks the simulation
  app.activeActions = 0;

  // 4. Replace inflight with FRESH tx objects (id preserved; everything mutable
  //    is reset to a clean lifecycle state).
  app.inflight.length = 0;
  for (const ts of snap.txs) {
    app.inflight.push({
      ...ts,
      status: "running", cancelled: false,
      nextTokens: 0, staggerMs: 0,
      restDot: null, blockedDot: null,
      timeline:     ts.timeline ? ts.timeline.map(e => ({ ...e })) : [],
      proposalPlan: ts.proposalPlan ? structuredClone(ts.proposalPlan) : null,
      proposalPlanStale: ts.proposalPlanStale ? structuredClone(ts.proposalPlanStale) : null,
      preloadDecisions: ts.preloadDecisions ? structuredClone(ts.preloadDecisions) : null,
    });
  }

  // 5. Park each restored tx at the rest position for the step it just finished.
  for (const tx of app.inflight) {
    if (tx.stepIndex > 0) placeRestDot(tx, tx.stepIndex - 1);
  }

  // 6. Spawn fresh loops — they immediately wait at awaitResumeOrNext (paused).
  for (const tx of app.inflight) runStepsLoop(tx);
}

export function clearHistory() {
  app.history.length = 0;
}

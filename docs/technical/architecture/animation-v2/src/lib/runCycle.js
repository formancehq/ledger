import { app, cache } from "./state.svelte.js";
import { steps } from "./steps.js";
import { raftLock, fsmLock, awaitResumeOrNext, isCancelled } from "./gates.js";
import { clearTxDots, placeRestDot, clearRestDot } from "./dots.js";
import { takeCheckpoint } from "./snapshot.js";
import { generationFor } from "./cache.js";
import { computePlan, eventGuardRebuild } from "./payloadBuilders.js";
import { COLORS } from "./colors.js";

// Single UI callback — banner update on step entry. Timeline mutations happen
// directly on tx.timeline; the Svelte proxy picks them up.
let bus = { onStep: () => {} };
export function bindRunBus(b) { bus = b; }

// Bail out helper — every cancel path needs to release whichever lock the tx is
// holding and clear its dots before returning.
function bail(tx) {
  if (tx.stepIndex === 5) raftLock.release();
  if (tx.stepIndex === 9) fsmLock.release();
  clearTxDots(tx);
}

function removeFromInflight(tx) {
  // Lookup by id rather than reference identity — Svelte 5 proxifies items as
  // they enter the $state array, so the bare tx ref we held never matches what
  // sits in app.inflight. indexOf would always return -1 and the tx would stay
  // stuck in the In-flight panel forever.
  const i = app.inflight.findIndex(t => t.id === tx.id);
  if (i >= 0) app.inflight.splice(i, 1);
  if (app.selectedTxId === tx.id) {
    app.selectedTxId = app.inflight.at(-1)?.id ?? null;
  }
}

// Entry point used by Send / Repeat. Sets up the inflight list state then hands
// off to runStepsLoop. applyCheckpoint() uses runStepsLoop() directly because
// restored txs are already in app.inflight.
export function runCycle(tx) {
  app.inflight.push(tx);
  app.selectedTxId = tx.id;
  runStepsLoop(tx);
}

// The lifecycle loop. Each iter:
//   1. consult step.skipIf — if true, jump to the next step WITHOUT consuming a
//      Next click (e.g. ①e when the cache served everything).
//   2. park the tx at the current step's "pre-action" rest position.
//   3. wait at the gate (paused / Next-token) — except step 0 (①a is "free").
//   4. checkpoint the pre-step state for Previous.
//   5. drop the rest dot, acquire the right lock at ①f / ⑤a, run the action.
//   6. emit step.event — appends a chronological entry to tx.timeline so the
//      right-side panel shows the full lifecycle of the tx (decisions made,
//      data captured, mutations applied).
//   7. increment stepIndex, auto-pause after ①a so the reader can inspect.
export async function runStepsLoop(tx) {
  while (tx.stepIndex < steps.length) {
    const s = steps[tx.stepIndex];

    // Skip-without-cost: this step is a logical no-op for this particular tx
    // (e.g. ①e when CheckCache covered everything).
    if (s.skipIf && s.skipIf(tx)) {
      tx.stepIndex++;
      continue;
    }

    // Park the dot at the entry edge of this step (= exit of the previous one).
    if (tx.stepIndex > 0) placeRestDot(tx, tx.stepIndex - 1);

    if (tx.stepIndex > 0) await awaitResumeOrNext(tx);
    if (isCancelled(tx)) { bail(tx); return; }

    // Snapshot AFTER the gate and BEFORE the action so Previous lands here.
    if (tx.stepIndex > 0) takeCheckpoint();

    clearRestDot(tx);

    if (tx.stepIndex === 5) {
      await raftLock.acquire(tx);
      if (isCancelled(tx)) { bail(tx); return; }
      // ProposalGuard rebuild check — preloader.go:209-236.
      // Under the tracker lock, recompute gen(nextIndex) and compare to what
      // BuildPreloads saw optimistically at ①d. If they disagree, the cache
      // has rotated since admission computed its plan — drop it and rebuild.
      if (tx.proposalPlan) {
        const currentFutureIdx = app.raft.leaderIdx + 1;
        const planGen = generationFor(tx.proposalPlan.futureIdx, cache.threshold);
        const currentGen = generationFor(currentFutureIdx, cache.threshold);
        if (planGen !== currentGen) {
          const stale = tx.proposalPlan;
          tx.proposalPlanStale = stale;
          tx.proposalPlan = null;             // force planFor to recompute
          const fresh = computePlan(tx, cache, app.raft);
          // Emit a warning event into the lifecycle timeline (proxy-routed).
          const entry = eventGuardRebuild(tx, stale, fresh);
          const proxy = app.inflight.find(t => t.id === tx.id);
          if (proxy) {
            proxy.timeline = [...proxy.timeline,
              { stepIndex: tx.stepIndex, color: COLORS.raft, ...entry }];
          }
        }
      }
    } else if (tx.stepIndex === 9) {
      // raftLock was already released at the end of the WAL anim inside
      // step ②'s action — the propose-side gate is only meant to serialize
      // the leader's WAL batches, not the whole apply pipeline.
      await fsmLock.acquire(tx);
      if (isCancelled(tx)) { bail(tx); return; }
    }

    bus.onStep(s, tx);

    if (tx.staggerMs > 0) {
      const delay = tx.staggerMs;
      tx.staggerMs = 0;
      await new Promise(r => setTimeout(r, delay));
      if (isCancelled(tx)) { bail(tx); return; }
    }

    app.activeActions++;
    try {
      await s.action(tx);
    } catch (e) {
      console.error("Step", tx.stepIndex, "of tx#" + tx.id, "failed:", e);
      raftLock.release(); fsmLock.release();
      clearTxDots(tx);
      app.activeActions--;
      removeFromInflight(tx);
      return;
    }
    app.activeActions--;

    if (isCancelled(tx)) { bail(tx); return; }

    // After the action runs, append an event to the tx's lifecycle timeline.
    // Steps that don't introduce new info (①b/①c, ②/③/④) leave `event` unset.
    // We append through the Svelte 5 proxy fetched from app.inflight (lookup
    // by id) and reassign the whole array — pushing through the bare `tx`
    // ref we still hold bypasses the proxy and Lifecycle subscribers never
    // see the mutation.
    if (s.event) {
      const entry = s.event(tx);
      if (entry) {
        const proxy = app.inflight.find(t => t.id === tx.id);
        if (proxy) {
          proxy.timeline = [...proxy.timeline, { stepIndex: tx.stepIndex, color: s.color, ...entry }];
        }
      }
    }

    tx.stepIndex++;
    if (tx.stepIndex === 1 && tx.stepIndex < steps.length) app.paused = true;
    if (tx.stepIndex === 12) fsmLock.release();
  }

  // Loop exit — tx has gone through ⑥. Archive a snapshot (timeline,
  // decisions, commit info) into app.completed before removing from the
  // in-flight list so the History panel can replay the full lifecycle.
  cache.ageBadges();
  app.lastTxData = {
    source: tx.source, destination: tx.destination,
    asset: tx.asset, amount: tx.amount, ledger: tx.ledger,
  };
  // Read timeline via the proxy — the events were pushed through the inflight
  // proxy (runCycle line ~116) so the bare `tx.timeline` we hold here is the
  // empty array from makeTx. Same proxy/bare-ref issue we hit with preloadDecisions.
  const proxy = app.inflight.find(t => t.id === tx.id);
  const liveTimeline = proxy ? proxy.timeline : tx.timeline;
  app.completed.push({
    id: tx.id, color: tx.color,
    source: tx.source, destination: tx.destination, asset: tx.asset, amount: tx.amount, ledger: tx.ledger,
    referenceId: tx.referenceId,
    stepIndex: steps.length,                                 // fully applied
    status: "done",
    timeline: liveTimeline.map(e => ({ ...e })),
    proposalPlan: tx.proposalPlan ? { ...tx.proposalPlan } : null,
    preloadDecisions: tx.preloadDecisions ? tx.preloadDecisions.map(d => ({ ...d })) : null,
    commitTerm: tx.commitTerm, commitIndex: tx.commitIndex,
    appliedLogId: tx.appliedLogId,
    appliedTxId: tx.appliedTxId,
    appliedPostingCount: tx.appliedPostingCount,
  });
  clearTxDots(tx);
  removeFromInflight(tx);
  tx.status = "done";
}

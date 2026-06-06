import { app, cache, proxyOf } from "./state.svelte.js";
import { steps } from "./steps.js";
import { awaitResumeOrNext, isCancelled, channelLock, fsmLock, raftLock } from "./gates.js";
import { clearTxDots, placeRestDotAt, clearRestDot } from "./dots.js";
import { clearTxAnimDots } from "./anim.js";
import { ANCHOR } from "./geometry.js";

// Single UI callback — banner update on step entry. Timeline mutations happen
// directly on tx.timeline; the Svelte proxy picks them up.
let bus = { onStep: () => {} };
export function bindRunBus(b) { bus = b; }

// Bail out helper — every cancel path needs to release whichever lock the tx
// is holding and clear its dots before returning. Locks are tracked per-tx
// via boolean flags (_heldRaftLock / _heldFsmLock / _heldChannelSlot); bail
// frees each held one via the bare module-scoped lock ref. Also resolves
// batch.applied if this tx is the lead of a batch — otherwise members would
// wait forever on the dead lead.
function bail(tx) {
  // Release every lock the tx is holding via the bare module-scoped refs.
  // Going through a stored proxy on tx would target a different object
  // than the one awaiters are queued on.
  if (tx._heldRaftLock)    { raftLock.release();    tx._heldRaftLock = false; }
  if (tx._heldFsmLock)     { fsmLock.release();     tx._heldFsmLock = false; }
  if (tx._heldChannelSlot) { channelLock.release(); tx._heldChannelSlot = false; }
  if (tx.batch && tx.batchLead) tx.batch.resolveApplied();
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

// Entry point used by Send / Repeat. Sets up the inflight list state then
// hands off to runStepsLoop on the Svelte 5 PROXY for the tx — not the bare
// ref. Mutating the bare object (e.g. tx.stepIndex++) doesn't propagate
// through the proxy's signals, so the In-flight panel, Lifecycle, and any
// other $derived/$effect reader stays frozen on initial values. Driving
// the loop with the proxy fixes the whole reactivity chain in one shot.
export function runCycle(tx) {
  app.inflight.push(tx);
  app.selectedTxId = tx.id;
  runStepsLoop(app.inflight.at(-1));
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

    // No more STEP_REST-based dot parking — anim() now leaves its dot at
    // the end of the last path it walked, so the previous step's leftover
    // dots ARE the rest dots (by construction, no maintenance). Special
    // case: a batched member that just landed at ⑥b (after skipping ⑥a
    // via skipIf) never ran ②③④⑤'s anims, so there's nothing for them
    // on the FSM side — park them explicitly at the FSM where the lead
    // applied on their behalf.
    if (tx.batch && !tx.batchLead && tx.stepIndex === 13) {
      placeRestDotAt(tx, ANCHOR.fsmIn);
    }

    if (tx.stepIndex > 0) await awaitResumeOrNext(tx);
    if (isCancelled(tx)) { bail(tx); return; }

    // Clear the previous step's leftover dots (incl. any restDot the
    // special case above placed) right before this step's action emits
    // new ones. Tagging dots by tx.id keeps other in-flight txs' dots intact.
    clearRestDot(tx);
    clearTxAnimDots(tx.id);

    // Declarative pre-action hook: steps that need extra logic before their
    // action runs (ProposalGuard rebuild at ①f, joinReadyTick + batched-
    // member short-circuit at ②) implement `beforeAction(tx)`. The hook may
    // return `{ jumpTo: <stepIndex> }` to skip the rest of this iter and
    // jump to a different step (used by the batched-member shortcut).
    if (s.beforeAction) {
      const r = await s.beforeAction(tx);
      if (isCancelled(tx)) { bail(tx); return; }
      if (r?.jumpTo != null) { tx.stepIndex = r.jumpTo; continue; }
    }

    // Declarative lock acquire: step ② declares `acquires: <lock>` and
    // the runner takes it just before the action. We map the acquired
    // lock to a boolean flag on tx — storing the lock OBJECT on the tx
    // proxy would proxify it, and a stored proxy ref doesn't compare ===
    // with the bare module-scoped lock that bail/release need to operate
    // on. (⑤a's fsmLock + channelLock are managed manually inside the
    // step's action for the same reason.)
    if (s.acquires) {
      await s.acquires.acquire(tx);
      if (s.acquires === raftLock)         tx._heldRaftLock = true;
      else if (s.acquires === fsmLock)     tx._heldFsmLock = true;
      else if (s.acquires === channelLock) tx._heldChannelSlot = true;
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
      if (tx._heldRaftLock)    { raftLock.release();    tx._heldRaftLock = false; }
      if (tx._heldFsmLock)     { fsmLock.release();     tx._heldFsmLock = false; }
      if (tx._heldChannelSlot) { channelLock.release(); tx._heldChannelSlot = false; }
      // If the failing tx was the lead of a batch, unblock the members so
      // they don't hang on a never-resolving applied promise.
      if (tx.batch && tx.batchLead) tx.batch.resolveApplied();
      clearTxDots(tx);
      app.activeActions--;
      removeFromInflight(tx);
      return;
    }
    app.activeActions--;

    if (isCancelled(tx)) { bail(tx); return; }

    // Declarative lock release: step ② declares `releases: <lock>` and
    // the runner frees it just after the action. Clear the matching flag
    // so bail() doesn't try to double-release.
    if (s.releases) {
      s.releases.release();
      if (s.releases === raftLock)         tx._heldRaftLock = false;
      else if (s.releases === fsmLock)     tx._heldFsmLock = false;
      else if (s.releases === channelLock) tx._heldChannelSlot = false;
    }

    // After the action runs, append an event to the tx's lifecycle timeline.
    // Steps that don't introduce new info (①b/①c, ②/③/④) leave `event` unset.
    // We append through the Svelte 5 proxy fetched from app.inflight (lookup
    // by id) and reassign the whole array — pushing through the bare `tx`
    // ref we still hold bypasses the proxy and Lifecycle subscribers never
    // see the mutation.
    if (s.event) {
      const entry = s.event(tx);
      if (entry) {
        const proxy = proxyOf(tx);
        if (proxy) {
          proxy.timeline = [...proxy.timeline, { stepIndex: tx.stepIndex, color: s.color, ...entry }];
        }
      }
    }

    tx.stepIndex++;
    if (tx.stepIndex === 1 && tx.stepIndex < steps.length) app.paused = true;
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
  const proxy = proxyOf(tx);
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

import { TX_PALETTE } from "./geometry.js";

// Build a fresh tx object from a submitted form. Everything that should be
// stable across the tx's lifetime is computed here (id, color, referenceId).
//   stepIndex / status / payload / proposalPlan / commit* are mutable lifecycle
//   fields owned by the runCycle.
export function makeTx({ form, id, lastLogId = 0 }) {
  return {
    id,
    color: TX_PALETTE[(id - 1) % TX_PALETTE.length],
    source: form.source,
    destination: form.destination,
    asset: form.asset,
    amount: form.amount,
    ledger: form.ledger,
    referenceId: `tx-2026-06-04-${String(id).padStart(3, "0")}`,
    stepIndex: 0,
    status: "running",                 // running | blocked | done
    cancelled: false,                  // set by Restart to stop the loop
    staggerMs: 0,                      // visual offset within a drained wave
    timeline: [],                      // chronological log of lifecycle events — see steps.js
    proposalPlan: null,                // CheckCache snapshot from ①d
    proposalPlanStale: null,           // populated when ProposalGuard rebuilt (G2)
    preloadDecisions: null,            // per-key MirrorPreload outcomes captured at ⑤b
    commitTerm: null,                  // snapshotted at step ② when leaderIdx bumps
    commitIndex: null,
    appliedLogId: null,                // snapshotted at ⑤c — survives later cache rotations
    appliedTxId: null,
    appliedPostingCount: null,
    // Batching (admission.NewCommand(orders…) semantics). When raftLock drains
    // a wave of N>1 txs, they form one batched Raft entry: the first becomes
    // the lead (batchLead=true), the rest are members. Members skip ②③④⑤ in
    // their own loop and resume at ⑥ once the lead's apply has resolved
    // batch.applied. Both null when the tx isn't in a batch.
    batch: null,
    batchLead: false,
    // Lock-hold flags — booleans (not lock refs) so Svelte 5's proxy on
    // the tx state doesn't proxify the lock object and break identity.
    _heldRaftLock:    false,           // raftLock held (step ②)
    _heldFsmLock:     false,           // fsmLock held (⑤a..⑥a start)
    _heldChannelSlot: false,           // channelLock held (⑤a beforeAction..action)
    restDot: null,                     // SVG circle parked between steps
    blockedDot: null,                  // SVG circle pulsing at a lock gate
  };
}

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
    cancelled: false,                  // set by Restart or Previous to stop the loop
    nextTokens: 0,                     // step credits granted by Next clicks
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
    restDot: null,                     // SVG circle parked between steps
    blockedDot: null,                  // SVG circle pulsing at a lock gate
  };
}

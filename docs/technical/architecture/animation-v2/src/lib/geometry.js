import { BOXES } from "./layout.js";

// Named anchor points used by step actions (rest positions, blocked-dot
// targets, path endpoints). Derived from BOXES so moving a box updates its
// anchors automatically — no manual coord duplication between layout.js,
// path d= strings, and rest-dot table.
//
// Most anchors are box-edge mid-points; a few use bottomAt(fraction) when
// two distinct points share the same edge (admission's Cache vs Pebble
// consult rails leaving from different x positions on the bottom).
export const ANCHOR = {
  client:           BOXES.client.rightMid(),       // (160, 315) — NB: e-client-grpc starts at (160, 315) from CLIENT, this is the in-anchor on gRPC side
  grpc:             BOXES.grpc.rightMid(),         // (380, 230)
  ctrl:             BOXES.ctrl.rightMid(),         // (380, 310)
  admConsultCache:  BOXES.adm.bottomAt(0.75),      // (340, 420) — Cache-consult departs from the right quarter of Admission's bottom
  admConsultPebble: BOXES.adm.bottomAt(0.25),      // (260, 420) — Pebble-consult departs from the left quarter
  admPropose:       BOXES.adm.rightMid(),          // (380, 390) — Admission → Leader
  admBlock:         BOXES.adm.rightMid(),          // same edge for the BLOCKED pulse
  leaderIn:         BOXES.leader.leftMid(),        // (470, 430) — Leader left edge
  leaderOut:        BOXES.leader.rightMid(),       // (690, 430) — Leader right edge (pivot for fan-outs)
  leaderBlock:      { x: BOXES.leader.x + BOXES.leader.w, y: BOXES.leader.y + 20 }, // (690, 400) — slightly above rightMid for the BLOCKED pulse visual
  // Endpoints of the e-leader-f1 / e-leader-f2 paths — where AppendEntries
  // dots actually land. Step ②'s rest dots sit on the follower boxes so the
  // visual reflects "entries arrived at the followers", not "still parked
  // on the leader".
  followerF1:       BOXES.followerF1.bottomMid(),  // (580, 330) — F1 bottom edge (end of e-leader-f1)
  followerF2:       BOXES.followerF2.leftMid(),    // (470, 160) — F2 left edge   (end of e-leader-f2)
  // Top edges of the per-node WAL boxes where the follower-fsync dots
  // actually land. Step ③ rests here so the entries visually sit on the
  // WALs that just persisted them.
  walF1Top:         BOXES.walF1.topMid(),          // (650, 345)
  walF2Top:         BOXES.walF2.topMid(),          // (650, 215)
  fsmIn:            BOXES.fsmL.leftMid(),          // (780, 401)
  cacheTop:         BOXES.cache.topMid(),          // (817.5, 438)
  pebbleTop:        BOXES.pebble.topMid(),         // (902.5, 438)
  notifier:         BOXES.notifier.leftMid(),      // (958, 315)
};

// Rest position the tx sits at AFTER the step at this index completed. Used to
// drop a static colored dot on the box-edge while the simulation is paused.
// Where a tx parks (red-pulsing) when blocked at a lock.
//   stepIndex 5 (①f) → blocked at the Admission → Leader gate
//   stepIndex 9 (⑤a) → blocked at the Leader   → FSM    gate
export function blockPosition(stepIndex) {
  if (stepIndex === 5) return ANCHOR.admBlock;     // legacy, raftLock no longer fires here
  if (stepIndex === 6) return ANCHOR.leaderIn;     // queued at the raft.Node Ready-tick gate (same spot as rest dot to avoid teleport)
  if (stepIndex === 9) return ANCHOR.leaderBlock;
  return null;
}

// Which boxes light up at each step (per-step pedagogical highlight).
export const HIGHLIGHTS = [
  ["box-grpc"],                                                         //  0  ①a
  ["box-ctrl"],                                                         //  1  ①b
  ["box-adm"],                                                          //  2  ①c
  ["box-cache"],                                                        //  3  ①d
  ["box-pebble"],                                                       //  4  ①e
  ["nodeL"],                                                            //  5  ①f
  ["walL", "nodeF1", "nodeF2"],                                         //  6  ②
  ["walF1", "walF2"],                                                   //  7  ③
  ["nodeL"],                                                            //  8  ④
  ["fsmL", "fsmF1", "fsmF2"],                                           //  9  ⑤a
  ["box-cache"],                                                        // 10  ⑤b
  ["box-cache", "box-pebble"],                                          // 11  ⑤c
  ["box-client", "box-notifier",
   "box-worker-index", "box-worker-sinks",
   "box-worker-archiver", "box-worker-sealer"],                         // 12  ⑥
];

// Visual offset applied to each dot when a queued WAVE of txs is drained from
// a gate. Two regimes co-exist:
//
//   STAGGER_MS           — default, "independent dots". Used by the pause gate
//                          (Next-click drain) and by fsmLock. Each tx travels
//                          on its own through gRPC → Ctrl → Admission → Cache
//                          → Pebble; visually offsetting them lets the reader
//                          follow each one separately.
//
//   BATCHED_STAGGER_MS=0 — only at raftLock drain. Admission aggregates the N
//                          requests currently queued at ①f into ONE Raft
//                          Proposal / one log entry / one pebble.Batch (see
//                          admission.go's NewCommand(orders…) call). Zero
//                          stagger makes the wave leave admission as a single
//                          tight group, faithful to "one Admit() call ≡ one
//                          entry on the wire".
//
// Use the In-flight / History panels to disambiguate individual txs when
// they overlap.
export const STAGGER_MS         = 140;
export const BATCHED_STAGGER_MS = 0;
// Continuous-mode Ready tick window. First tx arriving at the leader's
// pending-proposals queue schedules a tick this many ms in the future;
// other arrivals during the window join the same batch.
export const READY_TICK_WINDOW_MS = 500;

// Named anim durations — pacing tuned per-distance, not pure pixels/sec.
// The literals below cover the common cases; one-off durations (500ms
// follower hop, 750ms / 900ms fan-out variants, etc.) stay inline in
// steps.js because they're tuned for that specific visual.
//   DUR_QUICK  = short edges (grpc→ctrl, ctrl→adm, fsm→notifier, worker
//                fan-outs, WAL fsync hops)
//   DUR_HOP    = FSM↔Cache and FSM↔Pebble (uniform speed by design)
//   DUR_FORWARD= main forward edges (client→grpc, adm→leader, …)
//   DUR_RESPONSE = multi-segment leader → client response chain
export const DUR_QUICK    = 280;
export const DUR_HOP      = 380;
export const DUR_FORWARD  = 700;
export const DUR_RESPONSE = 1500;
export const TX_PALETTE = [
  "#82aaff", "#c3e88d", "#ffcb6b", "#c792ea",
  "#89ddff", "#addb67", "#ff9bd2", "#fbc02d",
];

// Generation rotation / cache badge constants.
export const ROTATION_EVERY = 5;   // raft indices per Gen0
export const BADGE_TTL      = 2;   // cycles a "NEW" / "TOUCHED" badge stays lit

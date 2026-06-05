// Canonical SVG coordinates for every named anchor. Centralising avoids the
// drift the plain-JS version suffered (same number repeated in the <path d=…>,
// in STEP_REST, in showBlockedDot, ...).

export const ANCHOR = {
  client:           { x: 220, y: 315 },
  grpc:             { x: 380, y: 230 },   // gRPC right edge
  ctrl:             { x: 380, y: 310 },   // Ctrl right edge
  admConsultCache:  { x: 340, y: 420 },   // Admission bottom-right (Cache departs)
  admConsultPebble: { x: 260, y: 420 },   // Admission bottom-left  (Pebble departs)
  admPropose:       { x: 380, y: 390 },   // Admission right edge (→ Leader)
  admBlock:         { x: 380, y: 390 },   // same edge for the BLOCKED pulse
  leaderIn:         { x: 470, y: 430 },   // Leader left edge
  leaderOut:        { x: 690, y: 430 },   // Leader right edge (pivot for fan-outs)
  leaderBlock:      { x: 690, y: 400 },   // Leader right edge for the BLOCKED pulse
  // Endpoints of the e-leader-f1 / e-leader-f2 paths — where AppendEntries
  // dots actually land. Step ②'s rest dots sit on the follower boxes so the
  // visual reflects "entries arrived at the followers", not "still parked
  // on the leader".
  followerF1:       { x: 580, y: 330 },   // F1 bottom edge (end of e-leader-f1)
  followerF2:       { x: 470, y: 160 },   // F2 left edge   (end of e-leader-f2)
  // Endpoints of the e-wal-f1 / e-wal-f2 paths — top edges of the per-node
  // WAL boxes where the follower-fsync dots actually land. Step ③ rests
  // here so the entries visually sit on the WALs that just persisted them.
  walF1Top:         { x: 650, y: 345 },   // WAL F1 top edge (end of e-wal-f1)
  walF2Top:         { x: 650, y: 215 },   // WAL F2 top edge (end of e-wal-f2)
  fsmIn:            { x: 780, y: 401 },   // FSM (Leader) left edge
  cacheTop:         { x: 820, y: 438 },
  pebbleTop:        { x: 902, y: 438 },
};

// Rest position the tx sits at AFTER the step at this index completed. Used to
// drop a static colored dot on the box-edge while the simulation is paused.
// null = tx has fully exited the pipeline.
// A STEP_REST entry can be a single anchor (one dot) or an array (one dot per
// anchor). The latter handles fan-out steps where the tx is conceptually
// present on multiple boxes at once — typically the leader→followers
// replication phases.
export const STEP_REST = [
  ANCHOR.grpc,                                       //  0  ①a → gRPC right edge
  ANCHOR.ctrl,                                       //  1  ①b → Ctrl right edge
  ANCHOR.admConsultCache,                            //  2  ①c → Admission (Cache-consult departure)
  ANCHOR.admConsultPebble,                           //  3  ①d → Admission (Pebble-consult departure)
  ANCHOR.admPropose,                                 //  4  ①e → Admission (Propose-to-Leader departure)
  ANCHOR.leaderIn,                                   //  5  ①f → Leader left edge
  [ANCHOR.followerF1, ANCHOR.followerF2],            //  6  ②   → entries landed on both followers
  [ANCHOR.walF1Top,   ANCHOR.walF2Top],              //  7  ③   → entries persisted on follower WALs
  ANCHOR.leaderOut,                                  //  8  ④   → acks back at the leader
  ANCHOR.fsmIn,                                      //  9  ⑤a → FSM left edge
  ANCHOR.cacheTop,                                   // 10  ⑤b → Cache top
  ANCHOR.pebbleTop,                                  // 11  ⑤c → Pebble top
  null,                                              // 12  ⑥   → done
];

// Where a tx parks (red-pulsing) when blocked at a lock.
//   stepIndex 5 (①f) → blocked at the Admission → Leader gate
//   stepIndex 9 (⑤a) → blocked at the Leader   → FSM    gate
export function blockPosition(stepIndex) {
  if (stepIndex === 5) return ANCHOR.admBlock;
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
export const TX_PALETTE = [
  "#82aaff", "#c3e88d", "#ffcb6b", "#c792ea",
  "#89ddff", "#addb67", "#ff9bd2", "#fbc02d",
];

// Generation rotation / cache badge constants.
export const ROTATION_EVERY = 5;   // raft indices per Gen0
export const BADGE_TTL      = 2;   // cycles a "NEW" / "TOUCHED" badge stays lit

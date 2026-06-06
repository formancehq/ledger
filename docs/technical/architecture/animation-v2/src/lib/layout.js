// Single source of truth for box geometry. Coords used by Diagram.svelte
// AND by geometry.js's ANCHOR table — keeps the two in sync by construction
// (move a box here, all its anchor edges follow).
//
// Each box has helper methods that return {x, y} points on its perimeter.
// Anchors elsewhere are derived from these.
function box(id, x, y, w, h, props = {}) {
  return {
    id, x, y, w, h, ...props,
    // Perimeter helpers — { x, y } points on the box's edges/corners.
    leftMid:    () => ({ x,             y: y + h / 2 }),
    rightMid:   () => ({ x: x + w,      y: y + h / 2 }),
    topMid:     () => ({ x: x + w / 2,  y }),
    bottomMid:  () => ({ x: x + w / 2,  y: y + h }),
    topLeft:    () => ({ x,             y }),
    topRight:   () => ({ x: x + w,      y }),
    bottomLeft: () => ({ x,             y: y + h }),
    bottomRight:() => ({ x: x + w,      y: y + h }),
    center:     () => ({ x: x + w / 2,  y: y + h / 2 }),
    // A point at a fraction across the bottom edge — used for the admission
    // box which exposes TWO consult-departure rails (Cache on the right
    // quarter, Pebble on the left quarter).
    bottomAt:   (fx) => ({ x: x + w * fx, y: y + h }),
  };
}

export const BOXES = {
  // ─── Client ────────────────────────────────────────────────────────
  client:    box("box-client",    40, 280, 120, 70, { title: "gRPC Client",      sub: "Apply(CreateLog)" }),

  // ─── Ingress ───────────────────────────────────────────────────────
  grpc:      box("box-grpc",     220, 200, 160, 60, { title: "gRPC Server",      sub: "BucketService :8888" }),
  ctrl:      box("box-ctrl",     220, 280, 160, 60, { title: "Routed Controller", sub: "forward to leader" }),
  adm:       box("box-adm",      220, 360, 160, 60, { title: "Admission",         sub: "preload volumes" }),

  // ─── Raft cluster (Leader at the bottom) ───────────────────────────
  followerF2:box("nodeF2",       470, 120, 220, 80),
  walF2:     box("walF2",        610, 215,  80, 20),
  followerF1:box("nodeF1",       470, 250, 220, 80),
  walF1:     box("walF1",        610, 345,  80, 20),
  leader:    box("nodeL",        470, 380, 220, 100),
  walL:      box("walL",         470, 495, 220, 42),
  // Background compactor in the consensus column — truncates the leader's
  // WAL once entries are safely applied + snapshotted. Behaviour wires up
  // in a follow-up; the box exists now so the layout is final.
  compactor: box("compactor",    470, 590, 220, 42, { title: "Compactor", sub: "wal.Truncate(<firstIdx)" }),

  // ─── FSM (one per node) + Cache + Pebble ───────────────────────────
  fsmF2:     box("fsmF2",        780, 120, 160, 40),
  fsmF1:     box("fsmF1",        780, 250, 160, 40),
  fsmL:      box("fsmL",         780, 380, 160, 42),
  cache:     box("box-cache",    780, 438,  75, 42, { title: "Cache",  sub: "in-memory", stroke: "#c792ea", titleFill: "#c792ea" }),
  pebble:    box("box-pebble",   865, 438,  75, 42, { title: "Pebble", sub: "durable" }),

  // ─── Notifier + subscribers ────────────────────────────────────────
  notifier:  box("box-notifier", 958, 200,  28, 230),
  workerIndex:    box("box-worker-index",    1000, 120, 180, 50, { title: "Index Builder",    sub: "tail log → ReadStore · batch ~1000" }),
  workerSinks:    box("box-worker-sinks",    1000, 200, 180, 50, { title: "Event Sinks",       sub: "Kafka · NATS · batch ~64" }),
  workerArchiver: box("box-worker-archiver", 1000, 280, 180, 50, { title: "Cold Storage (S3)", sub: "FSM-dispatched archive jobs" }),
  workerSealer:   box("box-worker-sealer",   1000, 380, 180, 50, { title: "Sealer",            sub: "periods · BLAKE3 hash chain" }),
};

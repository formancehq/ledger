// Single source of truth for box geometry. Coords used by Diagram.tsx
// AND by geometry.ts's ANCHOR table — keeps the two in sync by construction
// (move a box here, all its anchor edges follow).
//
// Each box has helper methods that return {x, y} points on its perimeter.
// Anchors elsewhere are derived from these.
export interface BoxDef {
  id: string;
  x: number; y: number; w: number; h: number;
  title?: string;
  sub?: string;
  stroke?: string;
  titleY?: number;
  subY?: number;
  titleFill?: string;
  leftMid(): { x: number; y: number };
  rightMid(): { x: number; y: number };
  topMid(): { x: number; y: number };
  bottomMid(): { x: number; y: number };
  topLeft(): { x: number; y: number };
  topRight(): { x: number; y: number };
  bottomLeft(): { x: number; y: number };
  bottomRight(): { x: number; y: number };
  center(): { x: number; y: number };
  bottomAt(fx: number): { x: number; y: number };
}

function box(id: string, x: number, y: number, w: number, h: number, props: Partial<BoxDef> = {}): BoxDef {
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
  // ─── Client + Ingress (stacked column) ─────────────────────────────
  // gRPC Client sits at the same y as the first box of every other column
  // (FOLLOWER, FSM, Index Builder) so the four column tops line up. The
  // server-side trio (gRPC Server / Routed Controller / Admission) is
  // shifted ~30px down to surface the cross-machine hop visually — the
  // wider gap from client signals "this leaves the caller's process".
  client:    box("box-client",   220, 120, 160, 60, { title: "gRPC Client",      sub: "Apply(CreateLog)" }),
  grpc:      box("box-grpc",     220, 230, 160, 60, { title: "gRPC Server",      sub: "BucketService :8888" }),
  ctrl:      box("box-ctrl",     220, 310, 160, 60, { title: "Routed Controller", sub: "forward to leader" }),
  adm:       box("box-adm",      220, 390, 160, 60, { title: "Admission",         sub: "preload volumes" }),
  // Mirrors internal/infra/preload/preloader.go's tracker — predicts the
  // next-entry index admission uses to pick a cache generation. Sits at the
  // bottom of the ingress column, vertically aligned with the compactor.
  // Title + sub left empty so Diagram.tsx can render a dynamic counter
  // showing the predicted next index.
  tracker:   box("box-tracker",  220, 590, 160, 42),

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

  // ─── Applier (small inline boxes between each cap-1 channel and FSM) ─
  // Coordinates already used by EngineDiagram's inline <rect>; centralised
  // here so Junction `fromAnchor`/`toAnchor` can derive leftMid()/rightMid()
  // and badges/arcs follow automatically when any box moves.
  applierL:  box("applierL",     720, 388,  40, 24),
  applierF1: box("applierF1",    720, 258,  40, 24),
  applierF2: box("applierF2",    720, 130,  40, 24),

  // ─── FSM (one per node) + Cache + Pebble ───────────────────────────
  fsmF2:     box("fsmF2",        780, 120, 160, 40),
  fsmF1:     box("fsmF1",        780, 250, 160, 40),
  fsmL:      box("fsmL",         780, 380, 160, 42),
  pebble:    box("box-pebble",   780, 495,  75, 42, { title: "Pebble", sub: "durable" }),
  cache:     box("box-cache",    865, 495,  75, 42, { title: "Cache",  sub: "in-memory", stroke: "#c792ea", titleFill: "#c792ea" }),
  // Visualises the per-entry computation phase between MirrorPreload (cache
  // populated with preload payload) and applyOrder (cache updated with the
  // tx's result). Aligned vertically with the compactor. Width 240 so the
  // long subtitle "numscript · validate · allocate ids" fits without
  // overflowing the box edges or colliding with the consult-edge inbound
  // from the FSM.
  processing:box("box-processing", 740, 590, 240, 42, { title: "Processing", sub: "numscript · validate · allocate ids" }),

  // ─── Notifier + subscribers ────────────────────────────────────────
  notifier:  box("box-notifier", 958, 200,  28, 230),
  workerIndex:    box("box-worker-index",    1000, 120, 180, 50, { title: "Index Builder",    sub: "tail log → ReadStore · batch ~1000" }),
  workerSinks:    box("box-worker-sinks",    1000, 200, 180, 50, { title: "Event Sinks",       sub: "Kafka · NATS · batch ~64" }),
  workerArchiver: box("box-worker-archiver", 1000, 280, 180, 50, { title: "Cold Storage (S3)", sub: "FSM-dispatched archive jobs" }),
  workerSealer:   box("box-worker-sealer",   1000, 380, 180, 50, { title: "Sealer",            sub: "periods · BLAKE3 hash chain" }),
};

// Engine — message-passing core types.
//
// One node per box (id mirrors BOXES.id from layout.ts). Each node owns
// a typed state, a FIFO mailbox, and a handler. A tick drains ONE msg
// per node and dispatches the resulting emits — either as a visual token
// animating along an SVG path (landing in the dst mailbox at anim end)
// or as instant intra-node delivery.

export type NodeId = string;
export type PathId = string;

// An edge between two nodes references one or more named SVG paths;
// reverse=true flips the anim direction so a single SVG path can serve
// both forward (e.g., leader→follower AE) and reverse (follower→leader
// Ack) hops without duplicating geometry. An array means the token
// walks the segments in order — used when a logical hop spans two SVG
// paths (e.g., admission→leader goes through the proposeCh midpoint
// queue, leader→fsmL goes through the channel-slot midpoint).
export type EdgeSegment = PathId | { id: PathId; reverse?: boolean };
export type EdgePath    = EdgeSegment | EdgeSegment[];

// Semantic of an edge:
//   - "send" (default): asynchronous one-way hop. Caller emits, token
//     animates forward to the dst, msg lands in dst's mailbox at the
//     next tick boundary. Network transfers + queue hops are sends.
//   - "call":  synchronous RPC. Caller emits, scheduler invokes the
//     callee's handler IN THE SAME TICK, captures its response, then
//     spawns ONE token that animates forward + reverse along the same
//     path. On anim end the response lands in the CALLER's mailbox.
//     Used for function-call edges (admission ↔ tracker/cache/pebble,
//     fsm ↔ cache/pebble/processing).
export type EdgeKind = "send" | "call";

// Message kinds. New kinds get added as nodes start emitting them.
//
// "Batch-level" msgs (AppendEntries, ApplyTrigger, ApplyTriggerAck,
// FsmAck, PreparedBatch, ConsultCache, CacheResp, ConsultProcessing,
// ProcessingResp, WritePebble, PebbleAck, NotifyLogs, WriteWAL,
// WALAck) all carry the same `Batch` payload — the lot of entries
// being applied/replicated. The Batch couples the entries with their
// derived `upTo` (max index) so consumers never see a stale upTo
// disagreeing with the entries themselves. Use `makeBatch(entries)`
// to construct one; never build a Batch literal manually.
export type Msg =
  | { kind: "Propose";          txId: number; order: Order }
  | { kind: "AppendEntries";    batch: Batch; leaderCommit: number; term: number; from: NodeId }
  // `batch` carries the entries the follower just persisted (= the AE
  // batch it's acking). Empty for heartbeat acks. matchIdx is kept for
  // clarity though it equals batch.upTo when the batch is non-empty.
  | { kind: "AppendResp";       matchIdx: number; success: boolean; from: NodeId; batch: Batch }
  // ApplyTrigger ships the actual entries the FSM is about to apply
  // (the leader can fish them out of its log on emit). FSM keeps them
  // on its `pending` slot so the downstream ConsultCache + WritePebble
  // msgs can carry per-entry details — used by cache/pebble panels to
  // surface real K/V content (volumes:{src}:{asset}, log:{idx}, …)
  // instead of just counters.
  | { kind: "ApplyTrigger";     batch: Batch }
  // Synchronous response to the applier→fsm CALL — fsm.PrepareEntries
  // returned (in the engine: the cache phase has been kicked off).
  // The actual prepared batch is shipped back via PreparedBatch once
  // the multi-tick cache + processing pipeline completes.
  | { kind: "ApplyTriggerAck";  batch: Batch }
  // FSM signals the applier that PrepareEntries finished — cache
  // mutations are done, processing has run, and the Pebble batch is
  // ready. The applier hands it off to its committer queue (cap-1)
  // for the pebble fsync + notifier broadcast.
  | { kind: "PreparedBatch";    batch: Batch; from: NodeId }
  | { kind: "FsmAck";           batch: Batch; from: NodeId }
  | { kind: "ClientResp";       txId: number; ok: boolean }
  // FSM-side consults (Phase 2). ConsultCache + WritePebble carry the
  // entries being applied so cache/pebble can derive real K/V keys
  // (volumes per account, log idx, …) for their displays.
  | { kind: "ConsultCache";     batch: Batch; from: NodeId }
  | { kind: "CacheResp";        batch: Batch }
  | { kind: "ConsultProcessing"; batch: Batch; from: NodeId }
  | { kind: "ProcessingResp";   batch: Batch }
  | { kind: "WritePebble";      batch: Batch; from: NodeId }
  | { kind: "PebbleAck";        batch: Batch }
  // Notifier fan-out (Phase 2)
  | { kind: "NotifyLogs";       batch: Batch }
  // WAL fsync — function call from raft node to its local WAL. The
  // caller (leader/follower) blocks on the wal.Append fsync, modelled
  // as a call edge round-trip.
  | { kind: "WriteWAL";         batch: Batch; from: NodeId }
  | { kind: "WALAck";           batch: Batch; from: NodeId }
  // Background WAL compaction (manually triggered via the Compact
  // button — no wall-clock timer in this model). Compactor reads the
  // last persisted index from Pebble, then truncates the leader's WAL
  // up to that point.
  | { kind: "Compact" }
  | { kind: "ReadPersisted";     from: NodeId }
  | { kind: "ReadPersistedResp"; upTo: number }
  | { kind: "TruncateWAL";       upTo: number; from: NodeId }
  | { kind: "TruncateWALAck";    firstIdx: number }
  // Admission preload consults (Phase 2.2). Correlated by txId so
  // multiple in-flight preloads can run in parallel.
  //
  // Tracker is consulted twice during admission's flow:
  //   - ReadTracker (predict)        — non-mutating peek at nextIndex.
  //   - IncrementTracker (commit)    — mutating bump under tracker.Lock;
  //     fires AFTER cache+pebble preload, immediately before forwarding
  //     the Propose to the leader. Mirrors real raft.Node.Propose, which
  //     does IndexTracker.Increment(1) inside the proposal lock.
  | { kind: "ReadTracker";          txId: number; from: NodeId }
  | { kind: "ReadTrackerResp";      txId: number; nextIndex: number }
  | { kind: "IncrementTracker";     txId: number; from: NodeId }
  | { kind: "IncrementTrackerResp"; txId: number; nextIndex: number }
  // ReadCache carries the order so the cache can compute hit/miss on
  // the volumes keys this tx would touch (`volumes:{src/dst}:{asset}`).
  // `hit` on the response tells admission whether to skip the pebble
  // lazy-load — that's the WHOLE point of the preload cache check.
  | { kind: "ReadCache";            txId: number; from: NodeId; order: Order }
  | { kind: "ReadCacheResp";        txId: number; hit: boolean }
  | { kind: "ReadPebble";           txId: number; from: NodeId }
  | { kind: "ReadPebbleResp";       txId: number };

// Domain shapes — kept skeletal here; full versions live in lib/tx.ts
// for now and may be ported into engine/domain.ts later.
export interface Order {
  ledger:      string;
  source:      string;
  destination: string;
  asset:       string;
  amount:      number;
  reference:   string;
}

export interface Entry {
  term:  number;
  index: number;
  txId:  number;
  order: Order;
}

// A `Batch` is the lot of entries being replicated / applied as a
// single unit on the Raft hot path. It couples the entries with their
// derived `upTo` (= highest index in the batch) so any consumer that
// only needs the watermark can read `batch.upTo` while richer consumers
// (token overlay, cache/pebble panels) walk `batch.entries`. The
// coupling is the whole point: `upTo` and the entries can no longer
// drift out of sync.
//
// Construct via `makeBatch(entries)`; never build a Batch literal.
// `makeBatch([])` yields `{ entries: [], upTo: 0 }` — used only for
// heartbeat AppendEntries which deliberately carry no tx payload.
export interface Batch {
  entries: Entry[];
  upTo:    number;
}

export function makeBatch(entries: Entry[]): Batch {
  const upTo = entries.length > 0 ? entries[entries.length - 1].index : 0;
  return { entries, upTo };
}

// One emit = one outbound message. `via` is the EdgePath along which
// the token animates from src → dst; pass null for instant intra-node
// delivery (no animation, no visual dot).
export interface Emit {
  to:  NodeId;
  via: EdgePath | null;
  msg: Msg;
}

// Context handed to a node's handler at each tick. The scheduler ref is
// kept opaque to handlers — nodes communicate by returning Emit[], not
// by calling back into the scheduler.
//
// `peek` lets a sender inspect another node's state read-only, used by
// nodes that need to pre-check downstream capacity (e.g., admission
// checking the proposeCh queue's depth before emitting).
//
// `upstreamPressure` returns the count of msgs already heading toward
// a node but not yet visible in its state — pending in its mailbox
// plus tokens currently animating toward it. Combined with the peeked
// queue depth, this gives a sender an exact "total in-flight" view
// to compare against capacity.
export interface TickCtx {
  tick: number;
  peek: <S = unknown>(nodeId: NodeId) => S | undefined;
  // Inspect a QueueEdge's snapshot (queue depth, capacity, …). Returns
  // null if no edge with that id exists or if the edge is not a queue.
  // Producers gate their emits on `queue.length + upstreamPressure` vs.
  // capacity to avoid overflowing the bounded FIFO.
  peekEdge: (edgeId: string) => EdgeSnapshot | null;
  upstreamPressure: (nodeId: NodeId) => number;
  // Topology lookup for emit routing. Producers build their Emit.via
  // through this instead of importing a module-level singleton — so
  // a Node carries no implicit dependency on the global topology
  // (testable with a custom edges map; multi-engine safe).
  pathFor: (from: NodeId, to: NodeId) => EdgePath | null;
}

// Public projection of a QueueEdge's runtime state. Matches the legacy
// QueueState shape (queue, capacity) so callers that previously peeked
// a queue node's state via `ctx.peek<QueueState>(nodeId)` migrate cleanly.
//
// `held` is the overflow buffer for QueueEdge: items the caller emitted
// while the queue was already at capacity. The Edge stores them FIFO
// and promotes to `queue` on each tick as slots free up. For
// ChannelEdge (sequential semantics — caller blocks instead of
// accumulating), `held` is always empty; the equivalent "waiting"
// count lives in the caller's own state (`commitIdx - dispatched`).
export interface EdgeSnapshot {
  queue:        Msg[];
  held:         Msg[];
  capacity:     number;
  forwarded:    number;
  rejected:     number;
  mailboxSize:  number;
}

// A token animating along a path. Three flavours coexist in the system,
// distinguished by `kind`. The discriminator makes scheduler decisions
// (parking, landing, owner-edge lookup) a clean switch rather than a
// stack of `from === to`, `onLand !== undefined`, … heuristics.
//
//   - "send"   : one-way async hop. Lands in dst's mailbox. Parks when
//                paused so the dot stays visible at the receiver's edge.
//   - "midhop" : the FIRST hop of a buffering edge (QueueEdge,
//                ChannelEdge). The token lands at the midpoint; its
//                `onLand` callback feeds the msg into the edge's slot
//                or spawns the synchronous follow-up. Never parks —
//                the slot/badge is the persistent visual.
//   - "call"   : the round-trip token of a synchronous RPC. `via` walks
//                forward+reverse so the dot returns to the caller;
//                `from === to === caller`. Parks like a send.
interface TokenBase {
  id:        number;
  from:      NodeId;
  to:        NodeId;
  via:       EdgePath;
  msg:       Msg;
  spawnTick: number;
}
export interface TokenSend extends TokenBase {
  kind:    "send";
  parked?: boolean;
}
export interface TokenMidhop extends TokenBase {
  kind:        "midhop";
  // Id of the buffering Edge that owns this hop. Lets `landToken`
  // republish the edge snapshot without walking the edges map.
  ownerEdgeId: string;
  onLand:      (msg: Msg, hooks: SchedulerHooks, ctx: TickCtx) => void;
}
export interface TokenCall extends TokenBase {
  kind:    "call";
  parked?: boolean;
}
// Discriminated union used everywhere internally.
export type Token = TokenSend | TokenMidhop | TokenCall;

// Back-compat alias used by the React store + components. The store
// receives tokens with the `onLand` closure stripped from midhop
// variants (closures aren't plain data); the rest of the shape is
// identical so consumers can switch on `kind` either way.
export type TokenInFlight = TokenSend | Omit<TokenMidhop, "onLand"> | TokenCall;

// Hooks injected by the scheduler into Edge methods AND token onLand
// callbacks. Edges never see the scheduler instance directly — they
// request operations through this surface so the dependency stays
// one-way (scheduler → edge/token). Lives in types.ts (and re-exported
// from edges/base.ts) so TokenMidhop.onLand can reference it without
// a circular import.
//
// `spawnToken` accepts an optional `midhop: { ownerEdgeId, onLand }`
// payload. When set, the resulting token is a TokenMidhop bound to
// that edge; otherwise a plain TokenSend. Call-return tokens are
// spawned internally by `_dispatchCall`, not through this hook.
export interface SchedulerHooks {
  spawnToken(args: {
    from: NodeId;
    to:   NodeId;
    via:  EdgePath;
    msg:  Msg;
    midhop?: {
      ownerEdgeId: string;
      onLand:      TokenMidhop["onLand"];
    };
  }): void;
  dispatchCall(from: NodeId, emit: Emit): void;
}

// Single 2D point — used by Junction / NodeMarker anchors to derive
// midpoints and badge positions from box-perimeter helpers.
export interface Point { x: number; y: number }

// Junction — first-class midpoint between two boxes, carrying the queue
// or cap-1 channel slot that buffers a buffering Edge. Owns the SINGLE
// source of truth for:
//   - the midpoint coordinate (x, y),
//   - the two SVG path ids (pathIds) — referenced by the engine for
//     emit routing AND by the diagram for token animation,
//   - the anchors at src/dst boxes (fromAnchor/toAnchor) — derived from
//     `BOXES.<node>.{leftMid,rightMid,…}()` so a box move propagates
//     through arcs and badge automatically,
//   - the badge style + count title.
// Both segments are rendered as straight lines by `QueueArc` (from the
// box anchor to (x, y), then (x, y) to the dst box anchor).
export interface Junction {
  pathIds:    [string, string];                 // [src→mid, mid→dst] — ids reused everywhere
  x:          number;
  y:          number;
  fromAnchor: () => Point;                      // src box perimeter helper, evaluated at render
  toAnchor:   () => Point;                      // dst box perimeter helper
  accent:     string;
  title:      (snap: EdgeSnapshot) => string;
  // Items waiting at the entry of this edge — rendered as a BatchDot
  // on the caller's box edge (at `fromAnchor`). Two flavours:
  //   - Parallel edges (QueueEdge proposeCh): caller emits without
  //     gating, surplus accumulates in `snap.held`. Junction returns
  //     `snap.held.length`.
  //   - Sequential edges (ChannelEdge): caller blocks instead of
  //     emitting; the "waiting" count lives in the caller's state.
  //     Junction reads it via `peek` (e.g. `leader.commitIdx -
  //     leader.dispatchedApplyUpTo`).
  // Returns 0 when nothing is waiting (badge stays hidden).
  held?: (
    peek: <S = unknown>(id: NodeId) => S | undefined,
    snap: EdgeSnapshot,
  ) => number;
}

// NodeMarker — decoration glued to a box edge (input or output side).
// Visualises a node-local back-pressure count (e.g. admission.ready.length,
// leader.commitIdx − dispatchedApplyUpTo) at the right geometric spot
// without ever owning a queue or a path. Rendered as a `BatchDot` pill
// at `anchor()` so a box move propagates automatically.
export interface NodeMarker {
  anchor: () => Point;                                                    // ex: () => BOXES.leader.leftMid()
  side:   "left" | "right";                                               // hint for any future offset/orientation
  accent: string;
  title:  string;
  // `peek` mirrors TickCtx.peek; the diagram supplies a reader backed by
  // its current node snapshots so the count is derived live from state.
  count:  (peek: <S = unknown>(nodeId: NodeId) => S | undefined) => number;
}

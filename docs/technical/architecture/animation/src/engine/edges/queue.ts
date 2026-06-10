import type { EdgeSegment, Junction, Msg, NodeId, TickCtx } from "../types";
import { Edge, type SchedulerHooks } from "./base";

// QueueEdge — bounded FIFO sitting on the wire between two nodes.
//
// Replaces the legacy "queue node" idea (proposeCh, qLeaderFsmL, etc.):
// the edge itself owns the queue state and the per-tick drain cadence.
// Producers emit toward the dst NodeId; the scheduler routes through
// this edge which:
//   1. dispatch(): spawns a visual token on path[0] (src → midpoint).
//      On land the msg is pushed into the edge's internal queue
//      INSTEAD of the dst's mailbox.
//   2. tick(): once per tick, if canDrain() allows, drains up to
//      `drainPerTick` head msgs by spawning a fresh token on path[1]
//      (midpoint → dst). Those tokens land normally in the dst mailbox.
//
// Behavior contract — preserved verbatim from the old queue nodes
// (nodes/queue.ts):
//   - cap=N FIFO. Senders are expected to peek via ctx.peekEdge and
//     hold locally rather than overflow; the defensive `rejected`
//     counter only fires if a sender ignores the back-pressure.
//   - Default drainPerTick=1 so accumulation is visible in the diagram.
//   - Filter by msg.kind: msgs of the wrong kind are dropped (matches
//     legacy makeQueueNode behavior).
export interface QueueSnapshot {
  queue:        Msg[];
  // Overflow buffer — items the caller emitted while `queue` was at
  // capacity. They wait FIFO and get promoted to `queue` whenever
  // tick() frees a slot. Unbounded by design: the caller no longer
  // gates on capacity, the edge absorbs the surplus.
  held:         Msg[];
  capacity:     number;
  forwarded:    number;
  rejected:     number;
  mailboxSize:  number;  // Always 0 — kept so callers that previously
                         // read mailboxSize + queue.length still work.
}

export interface QueueEdgeOpts {
  id:            string;
  // Midpoint metadata + path ids. The scheduler reads `paths` (a getter
  // backed by `junction.pathIds`), so a producer's emit still routes
  // through the correct SVG path; the diagram reads `junction` to
  // auto-position the QueueArc + badge.
  junction:      Junction;
  capacity:      number;
  drainPerTick?: number;
  accept:        Msg["kind"];
  // Optional gate — defer the drain when the downstream is saturated.
  // Same semantic as legacy QueueOpts.canDrain.
  canDrain?:     (ctx: TickCtx) => boolean;
}

export class QueueEdge extends Edge {
  readonly id:           string;
  readonly junction:     Junction;
  readonly capacity:     number;
  readonly drainPerTick: number;
  readonly accept:       Msg["kind"];
  readonly canDrain?:    (ctx: TickCtx) => boolean;

  state: QueueSnapshot;

  constructor(opts: QueueEdgeOpts) {
    super();
    this.id           = opts.id;
    this.junction     = opts.junction;
    this.capacity     = opts.capacity;
    this.drainPerTick = opts.drainPerTick ?? 1;
    this.accept       = opts.accept;
    this.canDrain     = opts.canDrain;
    this.state = {
      queue:       [],
      held:        [],
      capacity:    opts.capacity,
      forwarded:   0,
      rejected:    0,
      mailboxSize: 0,
    };
  }

  get paths(): [string, string] { return this.junction.pathIds; }

  dispatch(hooks: SchedulerHooks, _ctx: TickCtx, from: NodeId, to: NodeId, msg: Msg): void {
    // Filter by msg.kind — preserves the makeQueueNode behavior of
    // silently dropping mismatched kinds. (Doesn't happen in practice
    // because the topology routes only the accepted kind through.)
    if (msg.kind !== this.accept) return;
    // Token animates src → midpoint. Tagged as a midhop bound to THIS
    // edge: when it lands, the scheduler will fire `onLand` (enqueue),
    // then republish this edge's snapshot via the `ownerEdgeId`.
    hooks.spawnToken({
      from,
      to,                       // logical destination = final dst, not midpoint
      via:    this.paths[0],
      msg,
      midhop: {
        ownerEdgeId: this.id,
        onLand:      (landedMsg: Msg) => this._enqueueOrHold(landedMsg),
      },
    });
  }

  // Land in queue if a slot is free; otherwise park in `held` until
  // tick() promotes it. The caller is no longer responsible for
  // gating on capacity — the edge absorbs the surplus.
  private _enqueueOrHold(msg: Msg): void {
    if (this.state.queue.length < this.capacity) {
      this.state.queue.push(msg);
    } else {
      this.state.held.push(msg);
    }
  }

  tick(hooks: SchedulerHooks, ctx: TickCtx): boolean {
    if (this.state.queue.length === 0 && this.state.held.length === 0) return false;
    if (this.canDrain && !this.canDrain(ctx)) return false;
    const n     = Math.min(this.drainPerTick, this.state.queue.length);
    const head  = this.state.queue.splice(0, n);
    this.state.forwarded += n;
    for (const msg of head) {
      const [srcId, dstId] = parseEdgeId(this.id);
      hooks.spawnToken({
        from: srcId,
        to:   dstId,
        via:  this.paths[1],
        msg,
      });
    }
    // Promote held → queue FIFO until queue is at capacity again.
    // No coalescing: each msg keeps its identity.
    while (this.state.queue.length < this.capacity && this.state.held.length > 0) {
      this.state.queue.push(this.state.held.shift()!);
    }
    return n > 0 || head.length > 0;
  }

  // Always return a fresh shallow clone. Zustand's store (with Immer
  // middleware) deep-freezes any state object it accepts; if we handed
  // `this.state` directly, the next mutation (push/splice on the queue
  // array) would throw `Cannot add property X, object is not extensible`.
  // Cloning keeps `this.state` mutable inside the edge while the store
  // sees an immutable snapshot.
  snapshot(): QueueSnapshot {
    return {
      queue:       [...this.state.queue],
      held:        [...this.state.held],
      capacity:    this.state.capacity,
      forwarded:   this.state.forwarded,
      rejected:    this.state.rejected,
      mailboxSize: this.state.mailboxSize,
    };
  }

  reset(): void {
    this.state.queue.length = 0;
    this.state.held.length  = 0;
    this.state.forwarded    = 0;
    this.state.rejected     = 0;
    // mailboxSize is always 0 on disk; the live in-transit count is
    // re-derived by the scheduler's peekEdge from the tokens map.
  }
}

// Edges are keyed as "src→dst" in the scheduler's _edges map. Splitting
// the key gives us the from/to node ids for the second-hop token —
// avoids threading them through the constructor a second time.
function parseEdgeId(id: string): [NodeId, NodeId] {
  const idx = id.indexOf("→");
  if (idx < 0) return [id, id];
  return [id.slice(0, idx), id.slice(idx + 1)];
}

// Re-exported as a convenience for callers that previously typed against
// QueueState from nodes/queue.ts. Same shape.
export type { EdgeSegment };

import type { Msg, NodeId, SchedulerHooks, TickCtx } from "../types";
export type { SchedulerHooks } from "../types";

// Abstract Edge — the OO base for every directed link between two nodes.
//
// Phase 2: edges are first-class objects in the scheduler. They own the
// dispatch decision (how does an emit travel from src to dst?) and an
// optional periodic tick (drain queues, retry buffered work). The
// "queue node" idea — formerly a node sitting on the wire counting
// in-transit msgs — collapses into a QueueEdge subclass.
//
// Subclasses implement:
//   - dispatch(hooks, ctx, from, to, msg): called by the scheduler when
//     a producer emits a msg toward `to` via this edge. The edge
//     decides how to ship it (forward token, synchronous call, enqueue,
//     reverse, etc.).
//   - tick(hooks, ctx): default no-op (returns false). QueueEdge /
//     ChannelEdge override to drain their internal slot / queue at a
//     fixed rate per tick. **Return true if the edge's snapshot
//     changed** so the scheduler can republish it to the React store
//     in the same step; false if nothing happened. This puts the
//     "dirty?" knowledge inside the edge, where the drain logic lives.
//   - snapshot(): default null. QueueEdge / ChannelEdge return their
//     queue state so the diagram can render badge counts.
export abstract class Edge {
  abstract readonly id: string;

  abstract dispatch(hooks: SchedulerHooks, ctx: TickCtx, from: NodeId, to: NodeId, msg: Msg): void;

  tick(_hooks: SchedulerHooks, _ctx: TickCtx): boolean { return false; }

  snapshot(): unknown { return null; }

  // Wipe any mutable state. Default no-op for stateless edges
  // (DirectEdge, ReverseEdge). Buffering edges (QueueEdge, ChannelEdge)
  // override to clear their queue/slot + counters. Called by
  // `Scheduler.reset()` so it doesn't need to know which subclasses
  // carry state.
  reset(): void {}
}

// SchedulerHooks is defined in ../types.ts to avoid a circular import
// chain: types.ts (which declares TokenInFlight.onLand using
// SchedulerHooks) ← edges/base.ts ← edges/queue.ts ← scheduler.ts.

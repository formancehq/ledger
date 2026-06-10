import { NODE } from "../topology";
import type { Batch, Emit, Entry, Msg, NodeId, TickCtx } from "../types";
import { makeBatch } from "../types";
import { Node } from "./base";

// Follower — instantiated once for F1 and once for F2.
//
// AE arrival flow:
//   1. Append entries (if any).
//   2. Update commitIdx = min(leaderCommit, log.length).
//   3. Heartbeat (entries=[]) → emit AppendResp immediately (nothing to
//      fsync; matchIdx unchanged).
//   4. Real AE (entries > 0) → emit WriteWAL to local WAL. AppendResp
//      is deferred until WALAck lands.
//   5. If commitIdx advanced past appliedIdx, emit ApplyTrigger to
//      local FSM. (Apply doesn't gate on the WAL fsync of THIS AE —
//      it acts on entries already-fsync'd in previous AEs that are
//      now committed.)

export interface FollowerState {
  log:        Entry[];
  commitIdx:  number;
  appliedIdx: number;
  // Same coalescing pattern as the leader: while the local cap-1 apply
  // queue is full, commit advances just bump commitIdx and the next
  // dispatch ships (dispatchedApplyUpTo, commitIdx] as ONE batch.
  dispatchedApplyUpTo: number;
}

export class FollowerNode extends Node<FollowerState> {
  readonly id: NodeId;
  // Reasonably-named refs for the per-instance fsm/wal/applier ids.
  // Public so tests / debug overlays can introspect, though the engine
  // never reads them externally. The cap-1 apply queue formerly held
  // by queueId is now an Edge between this follower and applierId; we
  // look it up by `"<follower>→<applier>"` edge id.
  constructor(
    id: NodeId,
    public readonly fsmId:     NodeId,
    public readonly walId:     NodeId,
    public readonly applierId: NodeId,
  ) {
    super();
    this.id = id;
  }

  initialState(): FollowerState {
    return { log: [], commitIdx: 0, appliedIdx: 0, dispatchedApplyUpTo: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: FollowerState; emit: Emit[] } {
    const state = this.state;
    const id    = this.id;
    switch (msg.kind) {
      case "AppendEntries": {
        // De-duplicate by index — the leader sends already-committed
        // entries in heartbeat AEs (for visualization purposes) and we
        // mustn't grow the log a second time on those. Genuinely-new
        // entries (index > current log.length) need a WAL fsync; if
        // none, this is a heartbeat round and we ack immediately.
        const incomingEntries  = msg.batch.entries;
        const freshEntries     = incomingEntries.filter(e => e.index > state.log.length);
        const log              = freshEntries.length > 0 ? [...state.log, ...freshEntries] : state.log;
        // Cap commit at the highest index we actually hold.
        const newCommit = Math.max(state.commitIdx, Math.min(msg.leaderCommit, log.length));
        const emit: Emit[] = [];

        if (freshEntries.length === 0) {
          // Heartbeat round (no fresh entries to fsync). Ack immediately
          // with the same batch the leader sent so the AppendResp dot
          // stays tx-colored along the reverse hop.
          emit.push(emitAppendResp(ctx, id, log.length, msg.batch));
        } else {
          // Real AE: hand the (filtered) fresh entries to our local WAL.
          emit.push({
            to:  this.walId,
            via: ctx.pathFor(id, this.walId),
            msg: { kind: "WriteWAL", batch: makeBatch(freshEntries), from: id },
          });
        }
        // Just update commitIdx; the apply emission happens in tick()
        // via tryDispatchApply, which coalesces all newly-committed
        // entries (dispatchedApplyUpTo, commitIdx] into a single batch.
        return {
          state: { ...state, log, commitIdx: newCommit },
          emit,
        };
      }

      case "WALAck": {
        // Our WAL has fsync'd up through msg.batch.upTo. Tell the leader
        // we've persisted entries that high; forward the batch on the
        // AppendResp so the dot retains its tx color on the reverse hop.
        return {
          state,
          emit:  [emitAppendResp(ctx, id, msg.batch.upTo, msg.batch)],
        };
      }

      case "FsmAck": {
        // Local FSM finished applying — bump appliedIdx.
        return {
          state: { ...state, appliedIdx: Math.max(state.appliedIdx, msg.batch.upTo) },
          emit:  [],
        };
      }

      default:
        return { state, emit: [] };
    }
  }

  // Coalescing dispatch into the follower→applier QueueEdge. Ships ONE
  // ApplyTrigger covering (dispatchedApplyUpTo, commitIdx] whenever
  // commit has moved and the edge has room. Multiple commit advances
  // while the queue is full merge into a single bigger batch.
  tick(ctx: TickCtx): { state: FollowerState; emit: Emit[] } {
    const state = this.state;
    const id    = this.id;
    if (state.commitIdx <= state.dispatchedApplyUpTo) return { state, emit: [] };
    const edgeId = `${id}→${this.applierId}`;
    const qState = ctx.peekEdge(edgeId);
    if (!qState) return { state, emit: [] };
    const used = qState.queue.length + qState.mailboxSize;
    if (used >= qState.capacity) return { state, emit: [] };
    const upTo  = state.commitIdx;
    const batch = makeBatch(state.log.slice(state.dispatchedApplyUpTo, upTo));
    return {
      state: { ...state, dispatchedApplyUpTo: upTo },
      emit:  [{
        to:  this.applierId,
        via: ctx.pathFor(id, this.applierId),
        msg: { kind: "ApplyTrigger", batch },
      }],
    };
  }
}

function emitAppendResp(ctx: TickCtx, id: NodeId, matchIdx: number, batch: Batch): Emit {
  return {
    to:  NODE.leader,
    via: ctx.pathFor(id, NODE.leader),
    msg: { kind: "AppendResp", matchIdx, success: true, from: id, batch },
  };
}

import { NODE } from "../topology";
import type { Batch, Emit, Msg, NodeId, TickCtx } from "../types";
import { Node } from "./base";

// Applier — the dedicated goroutine sitting between the raft node and
// the FSM in the real code (`internal/infra/node/applier.go`). The
// raft Ready loop hands committed entries to the applier via a cap-1
// channel (`chan applyWork, 1`); the applier then calls
// fsm.PrepareEntries() synchronously (function call, not a channel)
// and orchestrates the parallel commit via an internal cap-1
// committer channel.
//
// In the animation:
//   - raft → applier: cap-1 queue (qLeaderFsmL / qFollower*).
//   - applier → fsm: function call (call edge in the topology).
//   - fsm → applier (async): plain send carrying the FsmAck once the
//     multi-phase pipeline (cache → processing → pebble) completes.
//   - applier → raft (async): forwards the FsmAck back to the raft node.
//
// Statistics kept on the side so the diagram can surface them in
// hover overlays if needed.
export interface ApplierState {
  forwardedToFsm:    number;
  preparedBatches:   number;
  commitsCompleted:  number;
  acksForwarded:     number;
  // Last batch handed off to the pebble committer queue (cap-1) but
  // not yet acknowledged. Stored as a full Batch (not just upTo) so
  // PebbleAck's downstream emits (FsmAck + NotifyLogs) can re-ship
  // the entries — keeps the dot colouring alive on those hops.
  inFlightCommit:    Batch | null;
}

export class ApplierNode extends Node<ApplierState> {
  readonly id: NodeId;

  constructor(
    id: NodeId,
    public readonly fsmId:  NodeId,
    public readonly raftId: NodeId,
  ) {
    super();
    this.id = id;
  }

  initialState(): ApplierState {
    return {
      forwardedToFsm:    0,
      preparedBatches:   0,
      commitsCompleted:  0,
      acksForwarded:     0,
      inFlightCommit:    null,
    };
  }

  handle(msg: Msg, ctx: TickCtx): { state: ApplierState; emit: Emit[] } {
    const state  = this.state;
    const id     = this.id;
    const fsmId  = this.fsmId;
    const raftId = this.raftId;
    switch (msg.kind) {
      // Inbound from the cap-1 channel — forward to the FSM via the
      // call edge. The CALL surfaces the "synchronous function call"
      // shape in the diagram: dot round-trips with PrepareEntries.
      case "ApplyTrigger": {
        return {
          state: { ...state, forwardedToFsm: state.forwardedToFsm + 1 },
          emit:  [{
            to:  fsmId,
            via: ctx.pathFor(id, fsmId),
            msg,
          }],
        };
      }
      // FSM finished PrepareEntries (cache + processing). The
      // prepared pebble batch lands here; hand it off to pebble via
      // the cap-1 committer QueueEdge — the edge buffers + drains.
      case "PreparedBatch": {
        return {
          state: { ...state, preparedBatches: state.preparedBatches + 1, inFlightCommit: msg.batch },
          emit:  [{
            to:  NODE.pebble,
            via: ctx.pathFor(id, NODE.pebble),
            msg: { kind: "WritePebble", batch: msg.batch, from: id },
          }],
        };
      }
      // Pebble committed the batch — emit FsmAck up to the raft node
      // (which fires ClientResp for each newly-applied entry) plus
      // a NotifyLogs fan-out to the notifier (subscribers wake up).
      // The batch payload comes from inFlightCommit (set on
      // PreparedBatch) — PebbleAck itself carries it too, so we use
      // msg.batch directly. The downstream hops keep tx colouring.
      case "PebbleAck": {
        return {
          state: { ...state, commitsCompleted: state.commitsCompleted + 1, inFlightCommit: null, acksForwarded: state.acksForwarded + 1 },
          emit:  [
            {
              to:  raftId,
              via: ctx.pathFor(id, raftId),
              msg: { kind: "FsmAck", batch: msg.batch, from: id },
            },
            {
              to:  NODE.notifier,
              via: ctx.pathFor(id, NODE.notifier),
              msg: { kind: "NotifyLogs", batch: msg.batch },
            },
          ],
        };
      }
      // Simple FSM (followers) — no pebble path. Just forward the
      // FsmAck straight through to the raft node.
      case "FsmAck": {
        return {
          state: { ...state, acksForwarded: state.acksForwarded + 1 },
          emit:  [{
            to:  raftId,
            via: ctx.pathFor(id, raftId),
            msg,
          }],
        };
      }
      // ApplyTriggerAck is the visual CALL-return — observed and
      // ignored; the real completion arrives via PreparedBatch.
      case "ApplyTriggerAck":
        return { state, emit: [] };
      default:
        return { state, emit: [] };
    }
  }
}

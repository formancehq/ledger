import type { Emit, Msg, NodeId, TickCtx } from "../types";
import { Node } from "./base";

// WAL — Write-Ahead Log. Each raft node (leader + 2 followers) owns
// its own local WAL backed by a single-writer fsync queue. The raft
// node emits WriteWAL when new entries need to be persisted, and the
// WAL acks with WALAck once the fsync is logically "done". Modeled
// here as a stateless durable counter; what matters pedagogically is
// the round-trip cost (one tick of network + one tick of fsync).

export interface WalState {
  firstSyncIdx: number;
  lastSyncIdx:  number;
  fsyncs:       number;
  truncations:  number;
}

export class WalNode extends Node<WalState> {
  readonly id: NodeId;

  constructor(id: NodeId, public readonly hostId: NodeId) {
    super();
    this.id = id;
  }

  initialState(): WalState {
    return { firstSyncIdx: 1, lastSyncIdx: 0, fsyncs: 0, truncations: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: WalState; emit: Emit[] } {
    const state  = this.state;
    const id     = this.id;
    const hostId = this.hostId;
    switch (msg.kind) {
      case "WriteWAL":
        return {
          state: {
            ...state,
            lastSyncIdx: Math.max(state.lastSyncIdx, msg.batch.upTo),
            fsyncs:      state.fsyncs + 1,
          },
          emit: [{
            to:  hostId,
            via: ctx.pathFor(id, hostId),
            // Pass-through the batch so the ack hop keeps the txId
            // colouring on its dot.
            msg: { kind: "WALAck", batch: msg.batch, from: id },
          }],
        };
      // Background compaction — caller is the Compactor node, NOT the
      // host raft node. Advance firstSyncIdx to msg.upTo; the WAL
      // bounds [firstSyncIdx..lastSyncIdx] become non-trivial after
      // a successful truncation.
      case "TruncateWAL": {
        const firstSyncIdx = Math.max(state.firstSyncIdx, msg.upTo + 1);
        return {
          state: { ...state, firstSyncIdx, truncations: state.truncations + 1 },
          emit:  [{
            to:  msg.from,
            via: ctx.pathFor(id, msg.from),
            msg: { kind: "TruncateWALAck", firstIdx: firstSyncIdx },
          }],
        };
      }
      default:
        return { state, emit: [] };
    }
  }
}

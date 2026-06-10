import { NODE } from "../topology";
import type { Emit, Msg, TickCtx } from "../types";
import { Node } from "./base";

// Compactor — background WAL compaction (one shot per "Compact" click).
// Real Raft compactors are timer-driven goroutines; this engine has no
// wall-clock, so the user manually pokes the compactor via the Compact
// button on EngineControls (which inject a Compact msg here).
//
// Flow per Compact click:
//   1. Compact         → ReadPersisted (call) to Pebble.
//   2. ReadPersistedResp(upTo) → TruncateWAL (call) to walL. Skipped
//      if upTo is at or below the current floor we last truncated to.
//   3. TruncateWALAck(firstIdx) → bump local lastTruncated, done.
//
// State tracks counter + last upTo so the UI can show "last attempted
// at idx N" and so a redundant Compact (no new persisted entries since
// last run) skips the WAL truncate.

export interface CompactorState {
  attempts:       number;
  lastTruncated:  number;   // highest idx we've successfully truncated up to
  pendingUpTo:    number | null;  // upTo from the latest ReadPersistedResp, awaiting ack
}

export class CompactorNode extends Node<CompactorState> {
  readonly id = NODE.compactor;

  initialState(): CompactorState {
    return { attempts: 0, lastTruncated: 0, pendingUpTo: null };
  }

  handle(msg: Msg, ctx: TickCtx): { state: CompactorState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "Compact":
        return {
          state: { ...state, attempts: state.attempts + 1 },
          emit:  [{
            to:  NODE.pebble,
            via: ctx.pathFor(NODE.compactor, NODE.pebble),
            msg: { kind: "ReadPersisted", from: NODE.compactor },
          }],
        };
      case "ReadPersistedResp": {
        if (msg.upTo <= state.lastTruncated) {
          // Pebble hasn't moved its floor since last compact — nothing
          // to do. Caller can poke again later.
          return { state, emit: [] };
        }
        return {
          state: { ...state, pendingUpTo: msg.upTo },
          emit:  [{
            to:  NODE.walLeader,
            via: ctx.pathFor(NODE.compactor, NODE.walLeader),
            msg: { kind: "TruncateWAL", upTo: msg.upTo, from: NODE.compactor },
          }],
        };
      }
      case "TruncateWALAck": {
        // The WAL ack carries its new firstIdx; the compactor records
        // the upTo it had asked to truncate to as the new high-water.
        const upTo = state.pendingUpTo ?? msg.firstIdx;
        return {
          state: { ...state, lastTruncated: upTo, pendingUpTo: null },
          emit:  [],
        };
      }
      default:
        return { state, emit: [] };
    }
  }
}

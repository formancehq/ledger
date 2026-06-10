import { NODE } from "../topology";
import type { Emit, Msg, TickCtx } from "../types";
import { Node } from "./base";

// IndexTracker — the preloader's predicted next-Raft-index source.
//
// Real flow (internal/infra/preload/preloader.go +
// internal/infra/node/index_tracker.go):
//   - Preloader.Next() peeks the predicted index — NON mutating.
//   - rawNode.Propose() calls IndexTracker.Increment(1) under
//     tracker.Lock at the moment of appending to raftLog.
//
// We model the two as distinct messages:
//   - ReadTracker      → predict only, no state change.
//   - IncrementTracker → bump nextIndex AND ack with the value JUST
//                        taken (the index this tx will get).
//
// Counter `reads` / `increments` published in state are pedagogical
// hooks — let the hover overlay later show traffic at the tracker.

export interface TrackerState {
  nextIndex:  number;
  reads:      number;
  increments: number;
}

export class TrackerNode extends Node<TrackerState> {
  readonly id = NODE.tracker;

  initialState(): TrackerState {
    return { nextIndex: 1, reads: 0, increments: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: TrackerState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "ReadTracker": {
        // Peek: read nextIndex, don't bump. Multiple concurrent reads
        // see the same value — that's the legitimate Raft prediction
        // semantic (it's an estimate; the real index is taken at
        // IncrementTracker time below).
        return {
          state: { ...state, reads: state.reads + 1 },
          emit:  [{
            to:  msg.from,
            via: ctx.pathFor(NODE.tracker, msg.from),
            msg: { kind: "ReadTrackerResp", txId: msg.txId, nextIndex: state.nextIndex },
          }],
        };
      }
      case "IncrementTracker": {
        // Take + bump atomically. The acked nextIndex is THIS tx's
        // actual index; subsequent IncrementTrackers see nextIndex+1.
        const taken = state.nextIndex;
        return {
          state: { ...state, nextIndex: taken + 1, increments: state.increments + 1 },
          emit:  [{
            to:  msg.from,
            via: ctx.pathFor(NODE.tracker, msg.from),
            msg: { kind: "IncrementTrackerResp", txId: msg.txId, nextIndex: taken },
          }],
        };
      }
      default:
        return { state, emit: [] };
    }
  }
}

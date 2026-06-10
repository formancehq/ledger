import { NODE } from "../topology";
import type { Emit, Msg, NodeId, TickCtx } from "../types";
import { Node } from "./base";

// Notifier — the signal.FanOut subscribers proxy. Receives NotifyLogs
// from the leader's FSM, broadcasts to all four workers. Real notifier
// is buffered-1 per subscriber with a 100 ms ticker fallback; Phase 2
// models the fan-out only.

const WORKERS: readonly NodeId[] = [
  NODE.workerIndex,
  NODE.workerSinks,
  NODE.workerArch,
  NODE.workerSealer,
];

export interface NotifierState {
  lastSeq: number;
}

export class NotifierNode extends Node<NotifierState> {
  readonly id = NODE.notifier;

  initialState(): NotifierState {
    return { lastSeq: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: NotifierState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "NotifyLogs":
        return {
          state: { lastSeq: Math.max(state.lastSeq, msg.batch.upTo) },
          // Fan-out re-ships the same Batch payload to each worker so
          // every hop in the notifier→worker leg keeps tx colouring.
          emit:  WORKERS.map(w => ({
            to:  w,
            via: ctx.pathFor(NODE.notifier, w),
            msg: { kind: "NotifyLogs", batch: msg.batch },
          })),
        };
      default:
        return { state, emit: [] };
    }
  }
}

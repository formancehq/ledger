import { NODE } from "../topology";
import type { Emit, Msg, TickCtx } from "../types";
import { Node } from "./base";

// Processing — the per-entry compute phase (numscript execution, posting
// validation, id allocation). The FSM hands an entry here between the
// cache MirrorPreload and the pebble write. Phase 2 stays stateless.

export interface ProcessingState {
  computed: number;
}

export class ProcessingNode extends Node<ProcessingState> {
  readonly id = NODE.processing;

  initialState(): ProcessingState {
    return { computed: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: ProcessingState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "ConsultProcessing":
        return {
          state: { computed: state.computed + 1 },
          emit: [{
            to:  msg.from,
            via: ctx.pathFor(NODE.processing, msg.from),
            // Pass-through the batch so the ack hop keeps tx colouring.
            msg: { kind: "ProcessingResp", batch: msg.batch },
          }],
        };
      default:
        return { state, emit: [] };
    }
  }
}

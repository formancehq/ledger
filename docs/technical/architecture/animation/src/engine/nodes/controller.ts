import { NODE } from "../topology";
import type { Emit, Msg, TickCtx } from "../types";
import { Node } from "./base";
import { forward } from "./client";

// Routed Controller — picks the leader and forwards. Stateless in
// Phase 1; multi-leader topology can later live here.

export interface ControllerState {
  forwarded: number;
}

export class ControllerNode extends Node<ControllerState> {
  readonly id = NODE.controller;

  initialState(): ControllerState {
    return { forwarded: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: ControllerState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "Propose":
        return {
          state: { forwarded: state.forwarded + 1 },
          emit:  [forward(ctx, NODE.controller, NODE.admission, msg)],
        };
      case "ClientResp":
        return {
          state,
          emit: [forward(ctx, NODE.controller, NODE.grpc, msg)],
        };
      default:
        return { state, emit: [] };
    }
  }
}

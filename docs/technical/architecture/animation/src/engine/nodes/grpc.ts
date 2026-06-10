import { NODE } from "../topology";
import type { Emit, Msg, TickCtx } from "../types";
import { Node } from "./base";
import { forward } from "./client";

// gRPC server — accepts the request from the network and hands it to
// the controller. In real code this is BucketServiceServer / Apply().
// Stateless: each Propose forwards directly.

export interface GrpcState {
  inFlight: number;
}

export class GrpcNode extends Node<GrpcState> {
  readonly id = NODE.grpc;

  initialState(): GrpcState {
    return { inFlight: 0 };
  }

  handle(msg: Msg, ctx: TickCtx): { state: GrpcState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "Propose":
        return {
          state: { inFlight: state.inFlight + 1 },
          emit:  [forward(ctx, NODE.grpc, NODE.controller, msg)],
        };
      case "ClientResp":
        return {
          state: { inFlight: Math.max(0, state.inFlight - 1) },
          emit:  [forward(ctx, NODE.grpc, NODE.client, msg)],
        };
      default:
        return { state, emit: [] };
    }
  }
}

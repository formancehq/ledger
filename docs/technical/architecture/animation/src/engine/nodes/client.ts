import { NODE } from "../topology";
import type { Emit, Msg, Order, TickCtx } from "../types";
import { Node } from "./base";

// Client — the application calling Apply(CreateLog) over gRPC. Holds
// pending and done maps keyed by txId, each carrying the order so the
// Inflight / History side panels can render the full request shape
// without re-deriving it from the log.

export interface ClientState {
  pending: Record<number, Order>;
  done:    Record<number, { ok: boolean; order: Order }>;
}

export class ClientNode extends Node<ClientState> {
  readonly id = NODE.client;

  initialState(): ClientState {
    return { pending: {}, done: {} };
  }

  handle(msg: Msg, ctx: TickCtx): { state: ClientState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "Propose": {
        // The user-facing inject seeds the pipeline by dropping a
        // Propose into the client's own mailbox. Record the order
        // for the Inflight panel, then forward to the gRPC server.
        return {
          state: {
            pending: { ...state.pending, [msg.txId]: msg.order },
            done:    state.done,
          },
          emit: [forward(ctx, NODE.client, NODE.grpc, msg)],
        };
      }
      case "ClientResp": {
        const order = state.pending[msg.txId];
        const nextPending = { ...state.pending };
        delete nextPending[msg.txId];
        return {
          state: {
            pending: nextPending,
            done:    { ...state.done, [msg.txId]: { ok: msg.ok, order } },
          },
          emit: [],
        };
      }
      default:
        return { state, emit: [] };
    }
  }
}

// Pass-through helper: forward `msg` from→to along the declared edge.
// `ctx` provides the topology lookup (`ctx.pathFor`) so the helper has
// no implicit dependency on the global EDGES singleton. Returns the
// emit with `via=null` if the topology has no matching edge — the
// scheduler then falls back to instant intra-node delivery.
export function forward(ctx: TickCtx, from: string, to: string, msg: Msg): Emit {
  return { to, via: ctx.pathFor(from, to), msg };
}

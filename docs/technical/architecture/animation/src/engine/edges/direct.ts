import type { EdgePath, Msg, NodeId, TickCtx } from "../types";
import { Edge, type SchedulerHooks } from "./base";

// DirectEdge — single-hop edge between two nodes, no buffering.
//
// Two variants by `kind`:
//   - "send" (async one-way): a single token animates forward along
//     `path`; on landing the msg lands in dst's mailbox via the
//     scheduler's default landToken plumbing.
//   - "call" (synchronous RPC): the callee runs in-tick, its response
//     is shipped back along a round-trip token. The scheduler owns the
//     call mechanics; the edge just routes the dispatch to it.
export class DirectEdge extends Edge {
  constructor(
    public readonly id: string,
    public readonly kind: "send" | "call",
    public readonly path: EdgePath,
  ) {
    super();
  }

  dispatch(hooks: SchedulerHooks, _ctx: TickCtx, from: NodeId, to: NodeId, msg: Msg): void {
    if (this.kind === "call") {
      hooks.dispatchCall(from, { to, via: this.path, msg });
      return;
    }
    hooks.spawnToken({ from, to, via: this.path, msg });
  }
}

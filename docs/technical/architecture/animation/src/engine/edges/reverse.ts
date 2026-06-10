import type { Msg, NodeId, PathId, TickCtx } from "../types";
import { Edge, type SchedulerHooks } from "./base";

// ReverseEdge — convenience wrapper for "async reverse" hops that reuse
// the geometry of a declared forward path. Mostly used for AppendResp
// and ClientResp style returns where the same SVG arc serves both
// directions (forward = AppendEntries, reverse = AppendResp).
//
// The wire-level behavior is identical to a DirectEdge with a single
// reversed segment; the class exists so the topology can spell intent
// ("this edge reverse-animates an existing path") without redefining
// the segment literal twice.
export class ReverseEdge extends Edge {
  constructor(
    public readonly id: string,
    public readonly refPathId: PathId,
  ) {
    super();
  }

  dispatch(hooks: SchedulerHooks, _ctx: TickCtx, from: NodeId, to: NodeId, msg: Msg): void {
    hooks.spawnToken({
      from,
      to,
      via:  { id: this.refPathId, reverse: true },
      msg,
    });
  }
}

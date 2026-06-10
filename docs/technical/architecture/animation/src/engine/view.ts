import { Edge } from "./edges/base";
import { DirectEdge } from "./edges/direct";
import { QueueEdge } from "./edges/queue";
import { ChannelEdge } from "./edges/channel";
import type { Node } from "./nodes/base";
import type {
  EdgePath,
  EdgeSnapshot,
  NodeId,
  TickCtx,
  Token,
} from "./types";

// EngineView — read-only window over the engine's runtime state. It
// owns nothing; just closes over the same node/edge/token maps the
// Scheduler maintains and exposes the four queries that handlers need
// (`peek`, `peekEdge`, `upstreamPressure`, `pathFor`). Construction of
// a `TickCtx` is a single method call on the view (`ctx(tick)`).
//
// Keeps the Scheduler from carrying a 50-line ctx-factory closure and
// makes the read surface testable in isolation.
export class EngineView {
  constructor(
    private readonly nodes:  Map<NodeId, Node>,
    private readonly edges:  Map<string, Edge>,
    private readonly tokens: Map<number, Token>,
  ) {}

  ctx(tick: number): TickCtx {
    return {
      tick,
      peek:             this.peek,
      peekEdge:         this.peekEdge,
      upstreamPressure: this.upstreamPressure,
      pathFor:          this.pathFor,
    };
  }

  // Read a node's current state. Returns undefined for unknown ids.
  peek = <S = unknown>(nodeId: NodeId): S | undefined => {
    return this.nodes.get(nodeId)?.state as S | undefined;
  };

  // Snapshot of a buffering edge + its real-time in-transit count
  // (midhop tokens still heading toward this edge's midpoint). Used by
  // producers to gate emits on `queue + mailboxSize` vs capacity.
  peekEdge = (edgeId: string): EdgeSnapshot | null => {
    const edge = this.edges.get(edgeId);
    if (!isBufferingEdge(edge)) return null;
    let inTransit = 0;
    for (const tok of this.tokens.values()) {
      if (tok.kind === "midhop" && tok.ownerEdgeId === edge.id) inTransit++;
    }
    return { ...edge.snapshot(), mailboxSize: inTransit };
  };

  // Mailbox size + in-transit non-midhop tokens heading toward
  // `nodeId`. Midhop tokens are counted by peekEdge as queue pressure
  // — not here, where we care about the consumer's mailbox depth.
  upstreamPressure = (nodeId: NodeId): number => {
    const mailbox = this.nodes.get(nodeId)?.mailbox.length ?? 0;
    let transit = 0;
    for (const tok of this.tokens.values()) {
      if (tok.kind === "midhop") continue;
      if (tok.to === nodeId) transit++;
    }
    return mailbox + transit;
  };

  // Resolve the SVG path a producer should use for an emit going
  // (from → to). For buffering edges, the FIRST segment (midpoint
  // hop); for DirectEdge, the declared path. Null if no edge exists.
  pathFor = (from: NodeId, to: NodeId): EdgePath | null => {
    const edge = this.edges.get(`${from}→${to}`);
    if (!edge) return null;
    if (edge instanceof QueueEdge)   return edge.paths[0];
    if (edge instanceof ChannelEdge) return edge.paths[0];
    if (edge instanceof DirectEdge)  return edge.path;
    return null;
  };
}

function isBufferingEdge(edge: unknown): edge is QueueEdge | ChannelEdge {
  return edge instanceof QueueEdge || edge instanceof ChannelEdge;
}

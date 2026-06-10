import type { Junction, Msg, NodeId, TickCtx } from "../types";
import { Edge, type SchedulerHooks } from "./base";
import type { QueueSnapshot } from "./queue";

// ChannelEdge — cap-1 wake-up channel modelling a Go `chan T, 1`.
//
// Behaviorally close to a QueueEdge(capacity=1, drainPerTick=1) but
// differs on one critical point: when a msg lands at the midpoint AND
// the receiver is idle (canDrain returns true), the second-hop fires
// IMMEDIATELY rather than waiting for the next scheduler.tick. Mirrors
// the real synchronous channel handoff (sender ↔ idle receiver).
//
// When the receiver is busy (canDrain blocks), the msg parks in the
// single slot; the periodic tick() retries until canDrain unblocks.
// At most one msg in flight from the sender — well-behaved producers
// peek the edge depth via ctx.peekEdge and hold locally otherwise.
//
// Used for: leader→applier, follower{F1,F2}→applier, applier→pebble.
// Buffered channels (proposeCh, cap-8) keep using QueueEdge.

export interface ChannelEdgeOpts {
  id:        string;
  // Midpoint metadata + path ids — see QueueEdgeOpts.junction for why
  // this replaces the legacy `paths: [string, string]`.
  junction:  Junction;
  accept:    Msg["kind"];
  canDrain?: (ctx: TickCtx) => boolean;
}

export class ChannelEdge extends Edge {
  readonly id:        string;
  readonly junction:  Junction;
  readonly accept:    Msg["kind"];
  readonly canDrain?: (ctx: TickCtx) => boolean;

  state: { occupied: Msg | null; forwarded: number; rejected: number };

  constructor(opts: ChannelEdgeOpts) {
    super();
    this.id       = opts.id;
    this.junction = opts.junction;
    this.accept   = opts.accept;
    this.canDrain = opts.canDrain;
    this.state = { occupied: null, forwarded: 0, rejected: 0 };
  }

  get paths(): [string, string] { return this.junction.pathIds; }

  dispatch(hooks: SchedulerHooks, _ctx: TickCtx, from: NodeId, to: NodeId, msg: Msg): void {
    if (msg.kind !== this.accept) return;
    hooks.spawnToken({
      from,
      to,
      via:    this.paths[0],
      msg,
      midhop: {
        ownerEdgeId: this.id,
        onLand:      (landedMsg, landHooks, landCtx) => this._onLand(landHooks, landCtx, landedMsg),
      },
    });
  }

  // Called when the first-hop token reaches the midpoint. If the
  // receiver is idle (canDrain OK), we spawn the second-hop right away
  // — that's the wake-up. Else we park the msg in the cap-1 slot and
  // wait for tick().
  private _onLand(hooks: SchedulerHooks, ctx: TickCtx, msg: Msg): void {
    if (this.state.occupied !== null) {
      // Cap-1 violated. Producers should have checked ctx.peekEdge first.
      this.state.rejected++;
      return;
    }
    if (!this.canDrain || this.canDrain(ctx)) {
      // Synchronous handoff — never lands in the slot.
      this._handoff(hooks, msg);
    } else {
      this.state.occupied = msg;
    }
  }

  tick(hooks: SchedulerHooks, ctx: TickCtx): boolean {
    if (this.state.occupied === null) return false;
    if (this.canDrain && !this.canDrain(ctx)) return false;
    const msg = this.state.occupied;
    this.state.occupied = null;
    this._handoff(hooks, msg);
    return true;   // forwarded incremented inside _handoff → snapshot dirty
  }

  private _handoff(hooks: SchedulerHooks, msg: Msg): void {
    const [srcId, dstId] = parseEdgeId(this.id);
    hooks.spawnToken({
      from: srcId,
      to:   dstId,
      via:  this.paths[1],
      msg,
    });
    this.state.forwarded++;
  }

  // Compat with QueueSnapshot so peekEdge / the diagram render badges
  // the same way they do for QueueEdge. capacity=1; queue holds the
  // single slot if occupied. `held` is always empty for ChannelEdge —
  // the sequential semantic means the caller blocks instead of
  // accumulating (the caller-side counter lives in its own state).
  snapshot(): QueueSnapshot {
    return {
      queue:       this.state.occupied ? [this.state.occupied] : [],
      held:        [],
      capacity:    1,
      forwarded:   this.state.forwarded,
      rejected:    this.state.rejected,
      mailboxSize: 0,
    };
  }

  reset(): void {
    this.state.occupied  = null;
    this.state.forwarded = 0;
    this.state.rejected  = 0;
  }
}

function parseEdgeId(id: string): [NodeId, NodeId] {
  const idx = id.indexOf("→");
  if (idx < 0) return [id, id];
  return [id.slice(0, idx), id.slice(idx + 1)];
}

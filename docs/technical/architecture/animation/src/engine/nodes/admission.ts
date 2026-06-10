import { NODE } from "../topology";
import type { Emit, Msg, Order, TickCtx } from "../types";
import { Node } from "./base";
import { forward } from "./client";

// Admission — gates the client request before handing it to Raft. In
// the real code it predicts the next index, reads the cache, fills any
// remaining miss by loading from Pebble, then calls leader.Propose.
//
// Engine model: per-tx preload state machine. Each in-flight tx walks
// through phases tracker → cache → pebble → tracker-incr → forwarded.
// Multiple txs can be in different phases concurrently — admission
// processes ONE msg per tick, but each msg carries its txId so the
// correct slot advances independently of the others.
//
// Once `IncrementTrackerResp` lands, the Propose is emitted directly
// to the leader — no `ready[]` staging, no `tick()` drain. The
// proposeCh QueueEdge owns its own overflow buffer (`held`) so the
// caller (us) emits without gating; the edge absorbs the burst.

type PreloadPhase = "tracker" | "cache" | "pebble" | "tracker-incr";

export interface AdmissionState {
  forwarded: number;
  // Per-tx preload progress. Order is stashed here so it can be carried
  // forward into the eventual Propose to the leader.
  inFlight: Record<number, { phase: PreloadPhase; order: Order }>;
}

export class AdmissionNode extends Node<AdmissionState> {
  readonly id = NODE.admission;

  initialState(): AdmissionState {
    return { forwarded: 0, inFlight: {} };
  }

  handle(msg: Msg, ctx: TickCtx): { state: AdmissionState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "Propose": {
        // Brand new client request. Kick off preload by consulting the
        // tracker first (predict next Raft index). Subsequent phases
        // chain on the responses below.
        return {
          state: {
            ...state,
            inFlight: { ...state.inFlight, [msg.txId]: { phase: "tracker", order: msg.order } },
          },
          emit: [emitReadTracker(ctx, msg.txId)],
        };
      }

      case "ReadTrackerResp": {
        const slot = state.inFlight[msg.txId];
        if (!slot || slot.phase !== "tracker") return { state, emit: [] };
        return {
          state: {
            ...state,
            inFlight: { ...state.inFlight, [msg.txId]: { ...slot, phase: "cache" } },
          },
          emit: [emitReadCache(ctx, msg.txId, slot.order)],
        };
      }

      case "ReadCacheResp": {
        const slot = state.inFlight[msg.txId];
        if (!slot || slot.phase !== "cache") return { state, emit: [] };
        // Cache hit → skip the pebble lazy-load and jump straight to
        // tracker-increment (reserve the actual Raft index). Miss →
        // fall through to ReadPebble as before. This is the point of
        // having a cache in the preload path.
        if (msg.hit) {
          return {
            state: {
              ...state,
              inFlight: { ...state.inFlight, [msg.txId]: { ...slot, phase: "tracker-incr" } },
            },
            emit: [emitIncrementTracker(ctx, msg.txId)],
          };
        }
        return {
          state: {
            ...state,
            inFlight: { ...state.inFlight, [msg.txId]: { ...slot, phase: "pebble" } },
          },
          emit: [emitReadPebble(ctx, msg.txId)],
        };
      }

      case "ReadPebbleResp": {
        const slot = state.inFlight[msg.txId];
        if (!slot || slot.phase !== "pebble") return { state, emit: [] };
        // Preload done — now take the actual Raft index by asking the
        // tracker to Increment. Mirrors raft.Node.Propose calling
        // IndexTracker.Increment(1) under tracker.Lock at proposal time.
        return {
          state: {
            ...state,
            inFlight: { ...state.inFlight, [msg.txId]: { ...slot, phase: "tracker-incr" } },
          },
          emit: [emitIncrementTracker(ctx, msg.txId)],
        };
      }

      case "IncrementTrackerResp": {
        const slot = state.inFlight[msg.txId];
        if (!slot || slot.phase !== "tracker-incr") return { state, emit: [] };
        // Preload done + index reserved → fire the Propose to the
        // leader RIGHT NOW. No staging. proposeCh's QueueEdge will
        // either enqueue immediately or park into its overflow `held`
        // buffer if the queue is at capacity.
        const nextInFlight = { ...state.inFlight };
        delete nextInFlight[msg.txId];
        return {
          state: {
            ...state,
            inFlight:  nextInFlight,
            forwarded: state.forwarded + 1,
          },
          emit: [forward(ctx, NODE.admission, NODE.leader, {
            kind:  "Propose",
            txId:  msg.txId,
            order: slot.order,
          })],
        };
      }

      case "ClientResp":
        return {
          state,
          emit: [forward(ctx, NODE.admission, NODE.controller, msg)],
        };

      default:
        return { state, emit: [] };
    }
  }

  // No per-tick hook — admission no longer drains a `ready[]` queue
  // with capacity-gated emits. The proposeCh edge absorbs the surplus
  // into its own `held` buffer; admission just fires Proposes as the
  // preload finishes.
}

function emitReadTracker(ctx: TickCtx, txId: number): Emit {
  return {
    to:  NODE.tracker,
    via: ctx.pathFor(NODE.admission, NODE.tracker),
    msg: { kind: "ReadTracker", txId, from: NODE.admission },
  };
}

function emitIncrementTracker(ctx: TickCtx, txId: number): Emit {
  return {
    to:  NODE.tracker,
    via: ctx.pathFor(NODE.admission, NODE.tracker),
    msg: { kind: "IncrementTracker", txId, from: NODE.admission },
  };
}

function emitReadCache(ctx: TickCtx, txId: number, order: Order): Emit {
  return {
    to:  NODE.cache,
    via: ctx.pathFor(NODE.admission, NODE.cache),
    msg: { kind: "ReadCache", txId, from: NODE.admission, order },
  };
}

function emitReadPebble(ctx: TickCtx, txId: number): Emit {
  return {
    to:  NODE.pebble,
    via: ctx.pathFor(NODE.admission, NODE.pebble),
    msg: { kind: "ReadPebble", txId, from: NODE.admission },
  };
}

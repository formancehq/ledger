import { NODE } from "../topology";
import type { Batch, Emit, Entry, Msg, NodeId, TickCtx } from "../types";
import { makeBatch } from "../types";
import { Node } from "./base";

// Two FSM flavors:
//
// FsmSimpleNode (followers F1, F2): receives ApplyTrigger, advances
// appliedIdx, acks immediately. No consult round-trips — the follower
// FSMs in the diagram have no paths to cache/pebble/processing.
//
// FsmPipelinedNode (leader): full apply pipeline modelling the real
// machine.go flow. Three phases gate the work:
//   1. "cache"      — MirrorPreload: ConsultCache (RPC call to cache)
//   2. "processing" — applyOrder: ConsultProcessing (RPC call)
//   3. "commit"    — batch.Commit: PARALLEL calls to cache (audit
//                     trail of the mirror writes) AND pebble (the
//                     durable batch). The FSM waits for BOTH responses
//                     before emitting FsmAck + NotifyLogs.
//
// ApplyTriggers arriving mid-pipeline queue and chain on completion.

// Pending also carries the entries the FSM is applying so the
// downstream ConsultCache emit surfaces real K/V to the cache panel.
// Phases mirror fsm.PrepareEntries in the real code: cache mutations
// first, then processor (numscript), then the prepared batch is
// returned to the applier — the FSM does NOT write to Pebble. That's
// the applier's job (it ships the batch through its committer queue).
// Pipeline phase model. Each phase ends with the FSM either WAITING
// for an external response (cache / processing) or HOLDING a deferred
// emit that the next tick() will dispatch. The "*-pending" phases
// exist so a `handle(...)` call can return ONLY the synchronous call
// response (ApplyTriggerAck, PreparedBatch send, …) and defer any
// downstream call to fsm.tick() — which prevents two animated dots
// from spawning in the same tick.
type Pending =
  | { upTo: number; entries: Entry[]; phase: "cache-pending" }       // will emit ConsultCache on next tick
  | { upTo: number; entries: Entry[]; phase: "cache" }                // awaiting CacheResp
  | { upTo: number; entries: Entry[]; phase: "processing-pending" }  // will emit ConsultProcessing on next tick
  | { upTo: number; entries: Entry[]; phase: "processing" };          // awaiting ProcessingResp

// Two-slot ApplyStager (back-pressure between Ready loop and applier):
//   inflight   — fsm.pending, currently walking cache → processing → commit.
//   gestating  — single slot, next upTo to start once inflight finishes.
//                Modelled as a capacity-1 channel slot.
// Anything beyond these two slots is back-pressured to the leader, which
// holds the overflow in LeaderState.pendingApply and only releases the
// next ApplyTrigger once an FsmAck frees a slot. So this FSM never sees
// more than two ApplyTriggers in flight at once.
export interface PendingEntries {
  upTo:    number;
  entries: Entry[];
}

// Common shape for the pipelined FSM (leader) + a separate simple shape
// for follower FSMs which only need an "ack deferred?" flag.
export interface FsmState {
  appliedIdx: number;
  pending:    Pending | null;
  gestating:  PendingEntries | null;
}

export interface FsmSimpleState {
  appliedIdx: number;
  // When set, handle(ApplyTrigger) consumed the trigger and returned
  // the synchronous ApplyTriggerAck only — the async FsmAck still owes
  // a tick. fsm.tick() drains this slot and clears the field.
  ackPending: { upTo: number; entries: Entry[] } | null;
}

function initialFsmState(): FsmState {
  return { appliedIdx: 0, pending: null, gestating: null };
}

// Simple FSM for followers — applies immediately on trigger. Returns
// only the call-response (ApplyTriggerAck) synchronously and DEFERS
// FsmAck to the next tick(). Without the defer, both tokens would
// spawn in the same tick and animate concurrently — confusing the
// reader who expects "one event per tick" pacing.
export class FsmSimpleNode extends Node<FsmSimpleState> {
  readonly id: NodeId;

  constructor(id: NodeId, public readonly hostNodeId: NodeId) {
    super();
    this.id = id;
  }

  initialState(): FsmSimpleState {
    return { appliedIdx: 0, ackPending: null };
  }

  handle(msg: Msg, ctx: TickCtx): { state: FsmSimpleState; emit: Emit[] } {
    const state      = this.state;
    const id         = this.id;
    const hostNodeId = this.hostNodeId;
    if (msg.kind !== "ApplyTrigger" || msg.batch.upTo <= state.appliedIdx) {
      return { state, emit: [] };
    }
    const batch = msg.batch;
    return {
      state: {
        appliedIdx: batch.upTo,
        ackPending: { upTo: batch.upTo, entries: batch.entries },
      },
      // Synchronous call response only — FsmAck is deferred to tick().
      emit: [{
        to:  hostNodeId,
        via: ctx.pathFor(id, hostNodeId),
        msg: { kind: "ApplyTriggerAck", batch },
      }],
    };
  }

  tick(ctx: TickCtx): { state: FsmSimpleState; emit: Emit[] } {
    const state = this.state;
    if (state.ackPending === null) return { state, emit: [] };
    const batch = makeBatch(state.ackPending.entries);
    return {
      state: { ...state, ackPending: null },
      emit:  [{
        to:  this.hostNodeId,
        via: ctx.pathFor(this.id, this.hostNodeId),
        msg: { kind: "FsmAck", batch, from: this.id },
      }],
    };
  }
}

// Pipelined FSM for the leader — chains cache → processing → (cache + pebble in parallel).
export class FsmPipelinedNode extends Node<FsmState> {
  readonly id: NodeId;

  constructor(id: NodeId, public readonly hostNodeId: NodeId) {
    super();
    this.id = id;
  }

  initialState(): FsmState {
    return initialFsmState();
  }

  handle(msg: Msg, _ctx: TickCtx): { state: FsmState; emit: Emit[] } {
    const state      = this.state;
    const id         = this.id;
    const hostNodeId = this.hostNodeId;
    switch (msg.kind) {
      case "ApplyTrigger": {
        const trigger = msg.batch;
        if (trigger.upTo <= state.appliedIdx) return { state, emit: [] };
        // ApplyTriggerAck is the synchronous response to the applier's
        // CALL edge — it lets the call's round-trip dot land back at
        // the applier so the visual matches the "function call" shape.
        // The ConsultCache emit is DEFERRED to tick() (phase
        // "cache-pending") so the two dots don't spawn concurrently.
        const ack: Emit = {
          to:  hostNodeId,
          via: _ctx.pathFor(id, hostNodeId),
          msg: { kind: "ApplyTriggerAck", batch: trigger },
        };
        if (state.pending === null) {
          return {
            state: { ...state, pending: { upTo: trigger.upTo, entries: trigger.entries, phase: "cache-pending" } },
            emit:  [ack],
          };
        }
        if (state.gestating === null) {
          return { state: { ...state, gestating: { upTo: trigger.upTo, entries: trigger.entries } }, emit: [ack] };
        }
        // Both slots full — should not happen because qLeaderFsmL's
        // canDrain gate holds the queue when gestating is occupied.
        return { state, emit: [ack] };
      }

      case "CacheResp": {
        if (state.pending?.upTo !== msg.batch.upTo || state.pending.phase !== "cache") return { state, emit: [] };
        // Phase 1 → 2 : preload cache done. Transition to
        // "processing-pending"; tick() will emit ConsultProcessing.
        return {
          state: { ...state, pending: { ...state.pending, phase: "processing-pending" } },
          emit:  [],
        };
      }

      case "ProcessingResp": {
        if (state.pending?.phase !== "processing" || state.pending.upTo !== msg.batch.upTo) return { state, emit: [] };
        // Phase 2 → done: PrepareEntries finished. PreparedBatch is a
        // SEND (not a call) so emitting it here is safe — only one
        // animated token spawns. If gestating is set, we transition it
        // into pending="cache-pending" so the next batch's ConsultCache
        // fires on the following tick (separate from this PreparedBatch).
        return completePrepare(state, msg.batch.upTo, state.pending.entries, id, hostNodeId, _ctx);
      }

      default:
        return { state, emit: [] };
    }
  }

  // Deferred-emit pump. Walks "*-pending" phases to the corresponding
  // "in-flight" phase, emitting the call that was held back from
  // `handle(...)`. Ensures each cache / processing consult animates in
  // its own tick rather than spawning alongside the applier→fsm round-
  // trip token.
  tick(ctx: TickCtx): { state: FsmState; emit: Emit[] } {
    const state = this.state;
    if (state.pending === null) return { state, emit: [] };
    if (state.pending.phase === "cache-pending") {
      const next = makeBatch(state.pending.entries);
      return {
        state: { ...state, pending: { ...state.pending, phase: "cache" } },
        emit:  [emitConsultCache(ctx, this.id, next)],
      };
    }
    if (state.pending.phase === "processing-pending") {
      const next = makeBatch(state.pending.entries);
      return {
        state: { ...state, pending: { ...state.pending, phase: "processing" } },
        emit:  [emitConsultProcessing(ctx, this.id, next)],
      };
    }
    return { state, emit: [] };
  }
}

// Helper — produce the next state + emits when fsm.PrepareEntries
// completes (cache + processing done). The FSM ships PreparedBatch
// back to the applier (a SEND, so safe to emit synchronously) and
// promotes the gestating slot if any. The next batch's ConsultCache
// is NOT emitted here — gestating transitions to phase
// "cache-pending" and tick() will dispatch on the following tick.
function completePrepare(state: FsmState, upTo: number, batchEntries: Entry[], id: NodeId, hostNodeId: NodeId, ctx: TickCtx): { state: FsmState; emit: Emit[] } {
  const prepared = makeBatch(batchEntries);
  const emit: Emit[] = [
    {
      to:  hostNodeId,
      via: ctx.pathFor(id, hostNodeId),
      msg: { kind: "PreparedBatch", batch: prepared, from: id },
    },
  ];
  if (state.gestating === null) {
    return { state: { ...state, appliedIdx: upTo, pending: null }, emit };
  }
  const next = state.gestating;
  return {
    state: {
      appliedIdx: upTo,
      pending:    { upTo: next.upTo, entries: next.entries, phase: "cache-pending" },
      gestating:  null,
    },
    emit,
  };
}

function emitConsultCache(ctx: TickCtx, from: NodeId, batch: Batch): Emit {
  return {
    to:  NODE.cache,
    via: ctx.pathFor(from, NODE.cache),
    msg: { kind: "ConsultCache", batch, from },
  };
}

function emitConsultProcessing(ctx: TickCtx, from: NodeId, batch: Batch): Emit {
  return {
    to:  NODE.processing,
    via: ctx.pathFor(from, NODE.processing),
    msg: { kind: "ConsultProcessing", batch, from },
  };
}

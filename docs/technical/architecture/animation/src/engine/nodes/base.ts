import type { Emit, Msg, NodeId, TickCtx } from "../types";

// Abstract Node — the OO base for every engine node.
//
// A subclass declares:
//   - id              : the stable NodeId mirroring BOXES.id from layout.
//   - initialState()  : factory for the per-instance state slot.
//   - handle(msg,ctx) : reducer-style msg handler.
//
// And optionally overrides:
//   - flush(ctx)      : post-drain hook (default no-op).
//   - tick(ctx)       : always-fire periodic hook (default no-op).
//   - snapshot()      : projection for the store (default = `this.state`).
//
// The scheduler operates polymorphically on these methods: it pulls msgs
// out of `mailbox`, calls handle/flush/tick, and assigns the returned
// state back to `this.state`. Handlers remain functional w.r.t. their
// inputs (state + msg → state + emit); mutation is encapsulated by the
// scheduler.
export abstract class Node<S = unknown> {
  abstract readonly id: NodeId;
  state: S;
  mailbox: Msg[] = [];

  constructor() {
    // initialState() is abstract — concrete subclasses provide the
    // initial state shape. The `as unknown as S` cast bridges the fact
    // that TS can't see the subclass method from within the abstract
    // constructor.
    this.state = (this as unknown as { initialState: () => S }).initialState();
  }

  abstract initialState(): S;

  abstract handle(msg: Msg, ctx: TickCtx): { state: S; emit: Emit[] };

  flush(_ctx: TickCtx): { state: S; emit: Emit[] } {
    return { state: this.state, emit: [] };
  }

  tick(_ctx: TickCtx): { state: S; emit: Emit[] } {
    return { state: this.state, emit: [] };
  }

  snapshot(): unknown {
    return this.state;
  }
}

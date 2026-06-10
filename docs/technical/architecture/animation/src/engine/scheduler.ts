import { Node } from "./nodes/base";
import { Edge, type SchedulerHooks } from "./edges/base";
import type { EngineStore } from "./store";
import { NODE } from "./topology";
import { EngineView } from "./view";
import { TokenManager, makeSpawnHook } from "./tokens";
import type { Emit, Msg, NodeId, TickCtx } from "./types";

// Scheduler — orchestrates the tick loop. Two collaborators carry the
// heavy work:
//   - `view`   (EngineView)   reads node / edge / token state and
//                             builds a TickCtx for handlers.
//   - `tokens` (TokenManager) owns the token map and every operation
//                             that mutates it (spawn / land / park /
//                             consume / publish).
//
// Each tick walks four phases:
//   1. drainEdges       — buffering edges drain head msgs toward dst.
//   2. snapshotMailboxes— freeze each node's mailbox so msgs landing
//                         mid-tick wait for the next iteration.
//   3. processNode      — per node: handle each msg, then flush, then
//                         tick. Emits flow through `dispatch`.
//   4. finalizeTick     — publish active-nodes + tokens snapshot.
//
// The Scheduler instance is per-engine — no module-level singleton —
// so the same class drives test fixtures and side-by-side animations
// interchangeably.

const TICK_INTERVAL_MS = 250;
// Auto-trigger background WAL compaction every N ticks.
const AUTO_COMPACT_EVERY_TICKS = 15;

export class Scheduler {
  private readonly nodes  = new Map<NodeId, Node>();
  private readonly tokens: TokenManager;
  private readonly view:   EngineView;
  private readonly hooks:  SchedulerHooks;
  // Set of node ids that did work this tick — published in finalizeTick
  // to drive the diagram's box highlighting. Reset at every tick.
  private tickActive: Set<NodeId> = new Set();
  // Set of node ids that were invoked synchronously via `dispatchCall`
  // during THIS tick (i.e., the callee of a "call" edge). Their `tick()`
  // pump is SKIPPED for the remainder of the tick — state mutations
  // from `handle()` shouldn't be observed by `tick()` in the same tick,
  // otherwise deferred-emit patterns (e.g., FSM's cache-pending phase)
  // collapse back into the same tick and the animated dots overlap.
  // Reset at every tick.
  private calleesThisTick: Set<NodeId> = new Set();
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly store: EngineStore,
    private readonly edges: Map<string, Edge>,
  ) {
    this.tokens = new TokenManager(store);
    this.view   = new EngineView(this.nodes, this.edges, this.tokens.tokens);
    this.hooks = {
      spawnToken:   makeSpawnHook(this.tokens, () => this.store.state.tick),
      dispatchCall: (from, emit) => this.dispatchCall(from, emit),
    };
  }

  // ── Lifecycle ────────────────────────────────────────────────────────

  register(node: Node): void {
    if (this.nodes.has(node.id)) return;
    this.nodes.set(node.id, node);
    this.publishNode(node);
  }

  // Seed every stateful edge's initial (empty) snapshot so queue badges
  // render `cap N / 0` before the first tick fires.
  publishInitialEdgeSnapshots(): void {
    for (const edge of this.edges.values()) this.publishEdge(edge);
  }

  reset(): void {
    this.pause();
    this.tokens.clear();
    this.store.reset();
    for (const node of this.nodes.values()) {
      node.state = node.initialState();
      node.mailbox.length = 0;
      this.publishNode(node);
    }
    // Each edge clears its own mutable state via Edge.reset() (no-op
    // by default; QueueEdge / ChannelEdge override). Adding a new
    // stateful edge subclass requires zero changes here.
    for (const edge of this.edges.values()) {
      edge.reset();
      this.publishEdge(edge);
    }
  }

  // ── Tick driving ─────────────────────────────────────────────────────

  start(): void {
    if (this.timer !== null) return;
    this.store.setPaused(false);
    this.timer = setInterval(() => this.tick(), TICK_INTERVAL_MS);
  }

  pause(): void {
    if (this.timer === null) return;
    clearInterval(this.timer);
    this.timer = null;
    this.store.setPaused(true);
  }

  step(): void { this.tick(); }

  // ── External-injection API ───────────────────────────────────────────

  inject(to: NodeId, msg: Msg): void {
    const dst = this.nodes.get(to);
    if (!dst) { console.warn(`Scheduler.inject: unknown node ${to}`); return; }
    dst.mailbox.push(msg);
    this.publishNode(dst);
  }

  // Drain one node's mailbox right now, without advancing the global
  // tick. Used by `sendTx` so the first client→grpc hop fires the
  // instant the user clicks Send (paused-mode visual continuity).
  flushNode(nodeId: NodeId): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    const ctx  = this.view.ctx(this.store.state.tick);
    const msgs = node.mailbox.splice(0);
    if (msgs.length === 0) return;
    for (const msg of msgs) {
      this.store.appendLog({ tick: ctx.tick, node: node.id, msg });
      const { state, emit } = node.handle(msg, ctx);
      node.state = state;
      for (const e of emit) this.dispatch(node.id, e, ctx);
    }
    this.publishNode(node);
  }

  // ── Visual-layer bridge: animation completion → mailbox push ─────────

  // Called by EngineDiagram when a dot's anim resolves. Decision is a
  // straight switch on token.kind:
  //   - midhop: fire onLand (enqueue / handoff), republish the owner
  //     edge, ALWAYS delete the token.
  //   - send / call: push msg to dst mailbox, park or delete based on
  //     pause state.
  landToken(tokenId: number): void {
    const token = this.tokens.get(tokenId);
    if (!token) return;
    if (token.kind === "midhop") {
      const ctx = this.view.ctx(this.store.state.tick);
      token.onLand(token.msg, this.hooks, ctx);
      const owner = this.edges.get(token.ownerEdgeId);
      if (owner) this.publishEdge(owner);
      this.tokens.delete(tokenId);
      return;
    }
    // send | call — both end up in some node's mailbox.
    const landed = this.tokens.landSendOrCall(tokenId, this.store.state.paused);
    if (!landed) return;
    const dst = this.nodes.get(landed.dst);
    if (dst) {
      dst.mailbox.push(landed.token.msg);
      this.publishNode(dst);
    }
  }

  // Sweep parked tokens on resume. EngineDiagram calls this on the
  // paused → !paused transition (after wiping its DOM dots).
  clearParkedTokens(): void {
    this.tokens.clearParked();
  }

  // ── Tick core (phases) ───────────────────────────────────────────────

  private tick(): void {
    this.store.advanceTick();
    const ctx = this.view.ctx(this.store.state.tick);
    this.tickActive      = new Set();
    this.calleesThisTick = new Set();
    this.maybeAutoCompact(ctx.tick);
    this.drainEdges(ctx);
    const inbox = this.snapshotMailboxes();
    for (const node of this.nodes.values()) {
      this.processNode(node, inbox.get(node.id) ?? [], ctx);
    }
    this.finalizeTick();
  }

  private maybeAutoCompact(tick: number): void {
    if (tick > 0 && tick % AUTO_COMPACT_EVERY_TICKS === 0) {
      this.inject(NODE.compactor, { kind: "Compact" });
    }
  }

  // Phase 1: every buffering edge drains its head. Stateless edges
  // no-op (Edge.tick returns false by default).
  private drainEdges(ctx: TickCtx): void {
    for (const edge of this.edges.values()) {
      if (edge.tick(this.hooks, ctx)) this.publishEdge(edge);
    }
  }

  // Phase 2: snapshot every mailbox up front so msgs landing during
  // dispatch (call returns, send tokens that completed mid-tick) wait
  // for the next iteration. Removes order-dependence between nodes.
  private snapshotMailboxes(): Map<NodeId, Msg[]> {
    const m = new Map<NodeId, Msg[]>();
    for (const node of this.nodes.values()) {
      const msgs = node.mailbox.splice(0);
      if (msgs.length > 0) m.set(node.id, msgs);
    }
    return m;
  }

  // Phases 3–5: handle each msg, then flush (batched output), then tick
  // (retry-blocked emits). Publishes the node snapshot if anything
  // changed.
  private processNode(node: Node, msgs: Msg[], ctx: TickCtx): void {
    const hadWork = msgs.length > 0;
    for (const msg of msgs) {
      this.store.appendLog({ tick: ctx.tick, node: node.id, msg });
      this.tokens.consumeParkedFor(node.id, msg);
      const { state, emit } = node.handle(msg, ctx);
      node.state = state;
      for (const e of emit) this.dispatch(node.id, e, ctx);
    }
    if (hadWork) {
      const { state, emit } = node.flush(ctx);
      node.state = state;
      for (const e of emit) this.dispatch(node.id, e, ctx);
    }
    // Skip the tick() pump if this node was synchronously invoked as a
    // callee via `dispatchCall` earlier in the tick — its state was
    // just mutated by `handle()` and tick() reading it would collapse
    // deferred-emit patterns (FSM's cache-pending phase, etc.) into the
    // same tick as the original call. The skipped tick() fires
    // naturally on the next iteration.
    let tickChanged = false;
    if (!this.calleesThisTick.has(node.id)) {
      const prevState = node.state;
      const { state, emit } = node.tick(ctx);
      node.state = state;
      for (const e of emit) this.dispatch(node.id, e, ctx);
      tickChanged = state !== prevState || emit.length > 0;
    }
    if (hadWork || tickChanged) {
      this.publishNode(node);
      this.tickActive.add(node.id);
    }
  }

  // Phase 6: publish the active-nodes set + tokens snapshot. The
  // active set drives box highlighting; replacing it wholesale ensures
  // the glow only stays for the tick that produced it.
  private finalizeTick(): void {
    this.store.publishActive(Array.from(this.tickActive));
    this.tokens.publish();
  }

  // ── Routing ──────────────────────────────────────────────────────────

  private dispatch(from: NodeId, emit: Emit, ctx: TickCtx): void {
    if (emit.via === null) {
      // Instant delivery (intra-node trigger).
      const dst = this.nodes.get(emit.to);
      if (!dst) return;
      dst.mailbox.push(emit.msg);
      this.publishNode(dst);
      return;
    }
    const edge = this.edges.get(`${from}→${emit.to}`);
    if (!edge) {
      console.warn(`Scheduler.dispatch: no edge for ${from}→${emit.to}`);
      return;
    }
    edge.dispatch(this.hooks, ctx, from, emit.to, emit.msg);
  }

  // Synchronous RPC. Called via `SchedulerHooks.dispatchCall` from
  // DirectEdge "call" dispatch. Runs the callee in-tick, then spawns
  // a round-trip token whose anim returns the response to the caller.
  private dispatchCall(from: NodeId, emit: Emit): void {
    if (emit.via === null) return;
    const callee = this.nodes.get(emit.to);
    if (!callee) return;
    const tick = this.store.state.tick;
    const ctx  = this.view.ctx(tick);
    this.store.appendLog({ tick, node: callee.id, msg: emit.msg });
    const result = callee.handle(emit.msg, ctx);
    callee.state = result.state;
    this.publishNode(callee);
    this.tickActive.add(callee.id);
    // Mark the callee so its tick() doesn't fire later in the same
    // tick (see processNode). Without this, a deferred-emit set by
    // handle() (e.g., FSM's cache-pending phase) would collapse back
    // into the current tick when tick() reads the just-mutated state.
    this.calleesThisTick.add(callee.id);

    // First emit going back to the caller = the response. Any other
    // emits are dispatched as regular sends.
    let response: Msg | null = null;
    for (const sub of result.emit) {
      if (response === null && sub.to === from) {
        response = sub.msg;
      } else if (response !== null && sub.to === from) {
        console.warn(`call ${from}→${emit.to}: callee emitted >1 response; using first`);
      } else {
        this.dispatch(callee.id, sub, ctx);
      }
    }
    if (response === null) {
      console.warn(`call ${from}→${emit.to}: callee emitted no response`);
      return;
    }
    this.tokens.spawnCallReturn(tick, from, emit.via, response);
  }

  // ── Snapshots ────────────────────────────────────────────────────────

  private publishNode(node: Node): void {
    this.store.publishNode({
      id:          node.id,
      state:       node.snapshot(),
      mailboxSize: node.mailbox.length,
    });
  }

  private publishEdge(edge: Edge): void {
    const snap = edge.snapshot();
    if (snap === null) return;          // stateless edge — nothing to publish
    this.store.publishEdge(edge.id, snap);
  }
}

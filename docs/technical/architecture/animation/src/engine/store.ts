import { createStore } from "zustand";
import { immer } from "zustand/middleware/immer";
import type { Msg, NodeId, TokenInFlight } from "./types";

// Engine-side store, kept fully separate from src/store/index.ts so the
// legacy animation keeps running while the new engine is built up next
// to it.
//
// Snapshots:
//   - tick:   monotonic logical clock, advances 1 per Scheduler step.
//   - nodes:  immutable snapshot of every registered node's state +
//             mailbox depth. The scheduler publishes a fresh copy after
//             each tick so React selectors can subscribe narrowly.
//   - tokens: tokens currently animating along a path. A token is
//             added on emit (with non-null via) and removed when its
//             anim completes (msg lands in dst mailbox).
//   - paused: user-controlled. step() ignores it; start() respects it.
//   - log:    append-only history of (tick, NodeId, Msg) for the banner
//             + debug panel + lifecycle replay. Capped to keep memory
//             bounded.

export interface NodeSnapshot {
  id:           NodeId;
  state:        unknown;
  mailboxSize:  number;
}

export interface EngineState {
  tick:    number;
  nodes:   Record<NodeId, NodeSnapshot>;
  // Mutable-edge snapshots (e.g. QueueEdge state). Keyed by the edge's
  // "from→to" id. Stateless edges (DirectEdge, ReverseEdge) don't
  // publish here. Used by the diagram to render queue badges and by
  // producers (via TickCtx.peekEdge) to gate emits on back-pressure.
  edges:   Record<string, unknown>;
  tokens:  TokenInFlight[];
  paused:  boolean;
  log:     LogEntry[];
  // Node ids whose handler fired during the current tick. The diagram
  // reads this to highlight the boxes that just did work. Snapshot
  // semantics: replaced wholesale at the end of each tick — so a node
  // only stays "lit" for the tick it processed a msg.
  activeNodes: NodeId[];
  // User-selected tx, set by clicking a row in Inflight/History. Drives
  // the lifecycle panel and the rich banner narrative.
  selectedTxId: number | null;
}

export interface LogEntry {
  tick: number;
  node: NodeId;
  msg:  Msg;
}

const LOG_CAP = 2000;

export const initialEngineState = (): EngineState => ({
  tick:         0,
  nodes:        {},
  edges:        {},
  tokens:       [],
  paused:       true,
  log:          [],
  activeNodes:  [],
  selectedTxId: null,
});

export class EngineStore {
  // Vanilla Zustand store so each EngineStore instance is fully
  // self-contained — no module-level binding via `create()`. React
  // components consume it through `useStore(api, selector)` (wired in
  // EngineContext.tsx); engine-side code uses `api.getState()` /
  // `api.setState()` directly through the methods below.
  // Type inference is intentional — annotating `api: StoreApi<...>` here
  // would strip the immer middleware's recipe-style `setState` signature.
  readonly api = createStore<EngineState>()(immer(() => initialEngineState()));

  get state(): EngineState { return this.api.getState(); }

  setState(recipe: (s: EngineState) => void): void {
    this.api.setState(recipe);
  }

  publishNode(snapshot: NodeSnapshot): void {
    this.api.setState(s => { s.nodes[snapshot.id] = snapshot; });
  }

  publishEdge(edgeId: string, snapshot: unknown): void {
    this.api.setState(s => { s.edges[edgeId] = snapshot; });
  }

  publishTokens(tokens: TokenInFlight[]): void {
    this.api.setState(s => { s.tokens = tokens; });
  }

  publishActive(active: NodeId[]): void {
    this.api.setState(s => { s.activeNodes = active; });
  }

  advanceTick(): void {
    this.api.setState(s => { s.tick += 1; });
  }

  appendLog(entry: LogEntry): void {
    this.api.setState(s => {
      s.log.push(entry);
      if (s.log.length > LOG_CAP) s.log.splice(0, s.log.length - LOG_CAP);
    });
  }

  setPaused(paused: boolean): void {
    this.api.setState(s => { s.paused = paused; });
  }

  setSelectedTxId(txId: number | null): void {
    this.api.setState(s => { s.selectedTxId = txId; });
  }

  reset(): void {
    this.api.setState(() => initialEngineState());
  }
}

// EngineStore is now instantiated per-engine inside `createEngine()`
// (see ./createEngine.ts). React components consume it through
// `useEngine()` / `useEngineStore(selector)` from `./EngineContext.tsx`.
// No module-level singleton: each engine handle owns its own store.

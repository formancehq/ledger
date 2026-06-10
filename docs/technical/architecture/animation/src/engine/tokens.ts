import type { EngineStore } from "./store";
import type {
  EdgePath,
  Msg,
  NodeId,
  TickCtx,
  Token,
  TokenInFlight,
  TokenMidhop,
  SchedulerHooks,
} from "./types";
import { roundTripVia } from "./edges/path-utils";

// Inputs accepted by `TokenManager.spawn()`. Caller picks the variant
// by either passing a `midhop` payload (becomes a TokenMidhop bound to
// the given ownerEdgeId) or a `call` flag (TokenCall with from===to
// and a round-trip via). Default is a TokenSend.
export type SpawnArgs =
  | { from: NodeId; to: NodeId; via: EdgePath; msg: Msg }
  | { from: NodeId; to: NodeId; via: EdgePath; msg: Msg;
      midhop: { ownerEdgeId: string; onLand: TokenMidhop["onLand"] } }
  | { from: NodeId; to: NodeId; via: EdgePath; msg: Msg; call: true };

// TokenManager — owns the token map + every operation that mutates it.
// Scheduler delegates spawn / land / consume / clearParked here so the
// orchestrator stays a thin driver. Each operation publishes a fresh
// snapshot to the store (closures stripped from midhop variants).
//
// `land()` is the central piece: it switches on token kind, fires the
// appropriate side-effect (mailbox push or onLand callback), and
// decides whether to park or delete. Returns enough metadata for the
// Scheduler to publish dependent edges/nodes without re-walking maps.
export class TokenManager {
  private readonly map: Map<number, Token> = new Map();
  private nextId = 1;

  constructor(private readonly store: EngineStore) {}

  // Pure read — exposed so EngineView can iterate tokens without
  // owning the map directly.
  get tokens(): Map<number, Token> { return this.map; }

  spawn(spawnTick: number, args: SpawnArgs): Token {
    const id = this.nextId++;
    const base = {
      id,
      from:      args.from,
      to:        args.to,
      via:       args.via,
      msg:       args.msg,
      spawnTick,
    };
    const token: Token =
        "midhop" in args ? { kind: "midhop", ...base,
                              ownerEdgeId: args.midhop.ownerEdgeId,
                              onLand:      args.midhop.onLand }
      : "call"   in args ? { kind: "call",   ...base }
      :                    { kind: "send",   ...base };
    this.map.set(id, token);
    this.publish();
    return token;
  }

  // Spawn a call round-trip token. The scheduler builds the forward
  // path; here we just wrap it as a TokenCall whose via is `forward +
  // reversed(forward)` and whose msg is the response.
  spawnCallReturn(spawnTick: number, caller: NodeId, forward: EdgePath, response: Msg): Token {
    return this.spawn(spawnTick, {
      from: caller, to: caller,
      via:  roundTripVia(forward),
      msg:  response,
      call: true,
    });
  }

  // Drop the token by id. Returns the token if it existed so the
  // caller can publish any dependent state (e.g. the owner edge for a
  // midhop). The store snapshot is republished after every removal.
  get(id: number): Token | undefined { return this.map.get(id); }

  delete(id: number): void {
    this.map.delete(id);
    this.publish();
  }

  // Mark a token parked (paused mode). Midhop tokens never park —
  // their slot/badge is the persistent visual — so this no-ops for
  // them. Returns the updated token, or undefined if the id is gone.
  park(id: number): void {
    const tok = this.map.get(id);
    if (!tok || tok.kind === "midhop") return;
    this.map.set(id, { ...tok, parked: true });
    this.publish();
  }

  // Land semantics for send/call tokens: push msg into dst's mailbox,
  // then park (paused) or delete (running). Returns the resolved dst
  // node id so the Scheduler can publish that node's snapshot.
  // Midhops have their own path and DON'T go through this method.
  landSendOrCall(id: number, paused: boolean): { token: Token; dst: NodeId } | null {
    const tok = this.map.get(id);
    if (!tok || tok.kind === "midhop") return null;
    if (paused) this.park(id);
    else        this.delete(id);
    return { token: tok, dst: tok.to };
  }

  // Drop EVERY parked token whose msg is being consumed by `to`'s
  // handle right now. A QueueEdge produces two tokens for the same
  // msg ref (midhop, then drain spawn); both vanish when dst picks
  // it up.
  consumeParkedFor(to: NodeId, msg: Msg): void {
    let removed = false;
    for (const [id, tok] of this.map) {
      if (tok.kind === "midhop") continue;
      if (tok.parked && tok.to === to && tok.msg === msg) {
        this.map.delete(id);
        removed = true;
      }
    }
    if (removed) this.publish();
  }

  // Resume sweep: drop parked tokens whose dots the diagram already
  // wiped. EngineDiagram calls this on the paused → !paused transition.
  clearParked(): void {
    let removed = false;
    for (const [id, tok] of this.map) {
      if (tok.kind !== "midhop" && tok.parked) {
        this.map.delete(id);
        removed = true;
      }
    }
    if (removed) this.publish();
  }

  clear(): void {
    this.map.clear();
    this.publish();
  }

  // Republish a snapshot of the map to the store. Closures on midhop
  // variants are stripped — they aren't plain data and the diagram has
  // no use for them.
  publish(): void {
    const list: TokenInFlight[] = [];
    for (const tok of this.map.values()) {
      if (tok.kind === "midhop") {
        const { onLand: _unused, ...rest } = tok;
        list.push(rest);
      } else {
        list.push(tok);
      }
    }
    this.store.publishTokens(list);
  }
}

// SchedulerHooks adapter — turns a `spawnToken({...})` call from an
// Edge into a TokenManager.spawn(). Kept here (not on the manager
// directly) so the Scheduler controls when ticks are advanced before
// spawning (the `spawnTick` value flows from the store).
export function makeSpawnHook(
  manager: TokenManager,
  currentTick: () => number,
): SchedulerHooks["spawnToken"] {
  return (args) => {
    if (args.midhop) {
      manager.spawn(currentTick(), {
        from: args.from, to: args.to, via: args.via, msg: args.msg,
        midhop: args.midhop,
      });
    } else {
      manager.spawn(currentTick(), {
        from: args.from, to: args.to, via: args.via, msg: args.msg,
      });
    }
  };
}

// Type-narrowing re-export so `ctx.pathFor` etc. callers can import the
// SpawnArgs union without touching engine/types.ts internals.
export type { TickCtx };

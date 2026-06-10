import { NODE } from "../topology";
import type { Emit, Entry, Msg, TickCtx } from "../types";
import { Node } from "./base";

// Cache — in-memory mirror with Gen0 / Gen1 generation split, mirroring
// the legacy two-generation cache from internal/infra/cache/cache.go.
// Gen0 holds keys guaranteed for the current index range; Gen1 holds
// the previous generation, discarded at the next rotation.
//
// In this animation we don't compute real balances — values are mocked
// (`{baseTick}@{idx}`) but keys are derived from each Entry's order
// (volumes:{src}:{asset}, volumes:{dst}:{asset}). That's enough to
// surface a meaningful "data accumulates here" K/V in the right-panel
// CachePanel, complete with NEW / TOUCHED badges that age out.

const ROTATION_EVERY = 3;   // # of consults (apply batches) before Gen0 → Gen1.
const BADGE_TTL      = 2;   // ticks (flush invocations) a badge stays lit.

export interface CacheEntry {
  value:    string;
  badge:    "NEW" | "TOUCHED" | null;
  badgeAge: number;
}

export interface CacheState {
  consultsServed:     number;
  consultsSinceRot:   number;  // resets to 0 after each rotation.
  // Plain Records (not Map) so Immer can shallow-copy without
  // needing enableMapSet. Order of keys = insertion order in
  // modern V8 / SpiderMonkey, sufficient for stable display.
  gen0:       Record<string, CacheEntry>;
  gen1:       Record<string, CacheEntry>;
  currentGen: number;
  baseIdx:    { gen0: number; gen1: number };
}

const initialEntry = (value: string, badge: "NEW" | "TOUCHED"): CacheEntry => ({
  value, badge, badgeAge: BADGE_TTL,
});

export class CacheNode extends Node<CacheState> {
  readonly id = NODE.cache;

  initialState(): CacheState {
    return {
      consultsServed:   0,
      consultsSinceRot: 0,
      gen0:             {},
      gen1:             {},
      currentGen:       0,
      baseIdx:          { gen0: 1, gen1: 0 },
    };
  }

  // Age badges once per tick. flush fires only when this node had work,
  // so a badge stays lit until the next time the cache is touched —
  // good enough; pedagogically aligns "badge persists across the
  // immediate next consult, then fades".
  flush(): { state: CacheState; emit: Emit[] } {
    const state = this.state;
    const aged  = { gen0: ageBadges(state.gen0), gen1: ageBadges(state.gen1) };
    return { state: { ...state, ...aged }, emit: [] };
  }

  handle(msg: Msg, ctx: TickCtx): { state: CacheState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      // FSM-side: ConsultCache during apply. The entries arg carries
      // the actual log entries being applied — we install mock K/V
      // derived from each entry's order so the panel reflects the
      // accumulating cache contents.
      case "ConsultCache": {
        let next = {
          ...state,
          consultsServed:   state.consultsServed + 1,
          consultsSinceRot: state.consultsSinceRot + 1,
        };
        for (const entry of msg.batch.entries) {
          next = installVolumeKeys(next, entry, /*tick*/ 0);
        }
        // Rotate after N consults — works even when the same keys are
        // overwritten repeatedly (dedup would otherwise prevent gen0
        // from ever growing past 2 entries).
        if (next.consultsSinceRot >= ROTATION_EVERY) {
          next = {
            ...next,
            gen1:             next.gen0,
            gen0:             {},
            currentGen:       next.currentGen + 1,
            consultsSinceRot: 0,
            baseIdx:          { gen0: msg.batch.upTo + 1, gen1: next.baseIdx.gen0 },
          };
        }
        return {
          state: next,
          emit: [{
            to:  msg.from,
            via: ctx.pathFor(NODE.cache, msg.from),
            // Pass-through the batch so the ack hop is coloured by tx.
            msg: { kind: "CacheResp", batch: msg.batch },
          }],
        };
      }
      // Admission-side: ReadCache — pure read, no state mutation.
      // Computes hit/miss against gen0+gen1 for the two volumes keys
      // the tx would touch. A `hit` lets admission skip the pebble
      // lazy-load entirely; a miss forces ReadPebble.
      case "ReadCache": {
        const srcKey = `volumes:${msg.order.source}:${msg.order.asset}`;
        const dstKey = `volumes:${msg.order.destination}:${msg.order.asset}`;
        const has = (k: string) => state.gen0[k] !== undefined || state.gen1[k] !== undefined;
        const hit = has(srcKey) && has(dstKey);
        return {
          state: { ...state, consultsServed: state.consultsServed + 1 },
          emit: [{
            to:  msg.from,
            via: ctx.pathFor(NODE.cache, msg.from),
            msg: { kind: "ReadCacheResp", txId: msg.txId, hit },
          }],
        };
      }
      default:
        return { state, emit: [] };
    }
  }
}

// Install the two volume keys for an entry. Writes only go to Gen0;
// Gen1 is read-only (it holds the previous generation until the next
// rotation discards it). The badge distinguishes a fresh write (NEW)
// from a write that shadows an existing Gen1 entry (TOUCHED) — the
// latter helps the viewer see "this key was already cached one
// generation back."
function installVolumeKeys(state: CacheState, entry: Entry, _tick: number): CacheState {
  const keys = [
    `volumes:${entry.order.source}:${entry.order.asset}`,
    `volumes:${entry.order.destination}:${entry.order.asset}`,
  ];
  let gen0 = state.gen0;
  for (const key of keys) {
    const badge: "NEW" | "TOUCHED" = state.gen1[key] !== undefined ? "TOUCHED" : "NEW";
    gen0 = { ...gen0, [key]: initialEntry(`@${entry.index}`, badge) };
  }
  return { ...state, gen0 };
}

function ageBadges(map: Record<string, CacheEntry>): Record<string, CacheEntry> {
  const out: Record<string, CacheEntry> = {};
  for (const [k, e] of Object.entries(map)) {
    if (e.badge === null) { out[k] = e; continue; }
    const next = e.badgeAge - 1;
    out[k] = next <= 0
      ? { ...e, badge: null, badgeAge: 0 }
      : { ...e, badgeAge: next };
  }
  return out;
}

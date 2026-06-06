import { SvelteMap } from "svelte/reactivity";
import { ROTATION_EVERY, BADGE_TTL } from "./geometry.js";
import { cacheTick } from "./cacheTick.svelte.js";

// Two-generation cache (Gen0 / Gen1) mirroring internal/infra/cache/cache.go.
// Maps must be SvelteMap, not plain Map — Svelte 5 doesn't auto-reactify Map
// instances that sit nested inside a $state(...) wrapper (only the top-level
// Map gets that treatment). SvelteMap.set/delete/clear trigger subscribers
// exactly like a $state proxy would.
//
// IMPORTANT: rotate() must NEVER swap the SvelteMap instances (i.e. never do
// `this.gen1 = this.gen0; this.gen0 = new SvelteMap()`). The CachePanel
// subscribes to each SvelteMap's intrinsic version signal the first time it
// iterates them; if we reassign the property to a brand-new SvelteMap, the
// panel's subscription stays attached to the *old* instance and the new ref's
// mutations never wake it up. Instead, keep the two SvelteMap instances stable
// for the lifetime of the cache and rotate by transferring entries in place
// (clear + set). SvelteMap.clear / SvelteMap.set both bump the version signal,
// so subscribers re-derive without any extra plumbing.

export const generationFor = (idx, th) =>
  idx <= 0 ? 0 : Math.floor((idx - 1) / th);

export function makeCache() {
  const cache = {
    threshold: ROTATION_EVERY,
    currentGen: 0,
    baseIdx: { gen0: 1, gen1: 0 },
    gen0: new SvelteMap(),
    gen1: new SvelteMap(),

    _entry(value) {
      return { value, deleted: false, badge: null, badgeAge: 0, flash: false };
    },

    checkCache(futureIdx, key) {
      const fg = generationFor(futureIdx, this.threshold);
      const delta = fg - this.currentGen;
      if (delta === 0) {
        if (this.gen0.has(key) && !this.gen0.get(key).deleted) return "GUARANTEED";
        if (this.gen1.has(key) && !this.gen1.get(key).deleted) return "NEEDS_TOUCH";
        return "MISS";
      }
      if (delta === 1) {
        // The proposal will land after a generation rotation: current Gen0
        // becomes Gen1, new Gen0 starts empty. A key currently in Gen0 will
        // need to be promoted (touched) into the new Gen0 at FSM.Preload time
        // so apply can read it. Treat it as NEEDS_TOUCH, not GUARANTEED —
        // otherwise the FSM apply finds Gen0 empty and silently no-ops.
        if (this.gen0.has(key) && !this.gen0.get(key).deleted) return "NEEDS_TOUCH";
        return "MISS";
      }
      return "MISS";
    },

    touch(key) {
      const existing = this.gen0.get(key);
      if (existing && !existing.deleted) return;
      const fromGen1 = this.gen1.get(key);
      if (!fromGen1) return;
      const copy = this._entry(fromGen1.value);
      copy.badge = "TOUCHED"; copy.badgeAge = BADGE_TTL; copy.flash = true;
      this.gen0.set(key, copy);
    },

    put(key, value) {
      const prev = this.gen0.get(key);
      const entry = this._entry(value);
      if (!prev || prev.deleted) {
        entry.badge = "NEW"; entry.badgeAge = BADGE_TTL;
      } else if (prev.badge) {
        entry.badge = prev.badge; entry.badgeAge = prev.badgeAge;
      }
      entry.flash = true;
      this.gen0.set(key, entry);
    },

    del(key) {
      for (const m of [this.gen0, this.gen1]) {
        const e = m.get(key);
        if (e) { e.deleted = true; e.badge = "DELETED"; e.badgeAge = BADGE_TTL; e.flash = true; }
      }
    },

    rotate(newBaseIdx) {
      // Snapshot gen0's entries before we mutate either map. The previous
      // gen1 is discarded outright (mirror of cache.go: Gen1 is overwritten,
      // not merged), so we just clear it and refill with what gen0 had.
      const oldGen0 = [...this.gen0];
      this.gen1.clear();
      for (const [k, v] of oldGen0) this.gen1.set(k, v);
      this.gen0.clear();
      this.currentGen += 1;
      this.baseIdx.gen1 = this.baseIdx.gen0;
      this.baseIdx.gen0 = newBaseIdx;
      // currentGen and baseIdx live on a plain object, not in a SvelteMap or
      // $state proxy, so the meta line (`gen N · base M …`) doesn't refresh
      // on its own. cacheTick covers that — entry lists refresh through the
      // SvelteMap signals fired by clear() / set() above.
      cacheTick.v++;
    },

    ageBadges() {
      for (const m of [this.gen0, this.gen1]) {
        for (const e of m.values()) {
          if (e.badgeAge > 0) {
            e.badgeAge -= 1;
            if (e.badgeAge === 0) e.badge = null;
          }
        }
      }
    },

    reset() {
      this.currentGen = 0;
      this.baseIdx = { gen0: 1, gen1: 0 };
      this.gen0.clear();
      this.gen1.clear();
      cacheTick.v++;
    },

  };
  return cache;
}

// Logical-key helpers — keep them next to the cache so UI / payload code can
// derive the same set of keys an Admission preload would care about.
export function txKeys(tx) {
  return [
    `volumes(${tx.source}.${tx.asset})`,
    `volumes(${tx.destination}.${tx.asset})`,
    `boundary(${tx.ledger})`,
  ];
}
export function txDefaults(tx) {
  return {
    [`volumes(${tx.source}.${tx.asset})`]:      { in: 0, out: 0 },
    [`volumes(${tx.destination}.${tx.asset})`]: { in: 0, out: 0 },
    [`boundary(${tx.ledger})`]:                 { nextLogId: 1, nextTxId: 1, postingCount: 0 },
  };
}

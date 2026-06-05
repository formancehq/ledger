import { makeCache } from "./cache.js";
import { makePebble } from "./pebble.js";

// Cache and Pebble live OUTSIDE the $state proxy. They contain SvelteMap
// instances which are already reactive on their own (set/delete/clear trigger
// subscribers). Putting them inside a $state(...) wrapper makes Svelte build
// a second proxy layer around them — read paths and write paths can then
// end up disagreeing about which storage they're talking to, which
// silently broke `cache.checkCache` (admission was seeing MISS for keys that
// were already in Gen0).
export const cache  = makeCache();
export const pebble = makePebble();

// One canonical reactive store for everything that's a plain object/array.
// Components read via destructuring; mutations go through this proxy.
//
// Exported as `app` (not `state`) so the local name doesn't shadow Svelte 5's
// `$state` rune in components.
export const app = $state({
  // Raft cluster -------------------------------------------------------
  raft: {
    term: 1,
    leaderIdx: 1,
    f1Match: 1, f2Match: 1,
    leaderApplied: 1,
    f1Applied: 1, f2Applied: 1,
  },
  // In-flight ledger transactions (tx objects, see lib/tx.js) ----------
  inflight: [],
  // Completed transactions kept around so the user can replay the lifecycle
  // of any past tx by clicking it. Snapshots take this into account so
  // Previous can roll a tx back from `completed` into `inflight`.
  completed: [],
  txSeq: 0,
  selectedTxId: null,
  // The form-side memory of the last completed tx for Repeat ----------
  lastTxData: null,
  // Animation control --------------------------------------------------
  paused: false,
  cancelRequested: false,
  activeActions: 0,
  // Stack of state snapshots for the Previous button -------------------
  history: [],
});

// Non-reactive coordination — Promises waiting on Pause/Next gates. Not part
// of `state` because mutations during the resolve flow shouldn't churn every
// reactive consumer.
export const resumeWaiters = [];

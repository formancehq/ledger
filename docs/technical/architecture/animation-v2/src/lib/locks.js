import { STAGGER_MS } from "./geometry.js";

// Two-stage pipeline lock used at ①f (raft) and ⑤a (FSM).
//
//   acquire(tx)   — grants immediately if free; otherwise queues. Queued txs
//                   are woken up by release().
//   release()     — drains the queue in one go.
//   staggerMs opt — per-tx visual offset applied when a wave is drained. The
//                   raft lock passes 0 (BATCHED_STAGGER_MS) so the wave at
//                   admission→leader looks like a single batched proposal;
//                   other locks fall back to the default which spreads dots
//                   out so the reader can follow each tx.
//
// callbacks are optional hooks for the UI (block/unblock pulse, render).
export function makeLock({ onBlock, onUnblock, staggerMs = STAGGER_MS } = {}) {
  return {
    busy: false,
    queue: [],
    // Retained so existing call sites (none after the Next bypass removal,
    // but kept for symmetry / forward compat) don't blow up if they set it.
    batchGrants: 0,

    async acquire(tx) {
      if (!this.busy) { this.busy = true; return; }
      if (this.batchGrants > 0) { this.batchGrants--; return; }
      onBlock?.(tx);
      await new Promise(res => this.queue.push({ res, tx }));
      onUnblock?.(tx);
    },

    release() {
      const drained = this.queue.splice(0);
      if (drained.length === 0) this.busy = false;
      // The configured staggerMs decides whether the wave fans out visually
      // (independent dots) or stays in lockstep (one batched group).
      drained.forEach((item, i) => { item.tx.staggerMs = i * staggerMs; });
      drained.forEach(({ res }) => res());
    },

    // Force-drain — invoked from Next/Restart paths. Returns the drained tx
    // objects so the caller can also nudge per-tx state (e.g., nextTokens).
    drain() {
      const drained = this.queue.splice(0);
      drained.forEach((item, i) => { item.tx.staggerMs = i * staggerMs; });
      return drained;
    },

    reset() { this.busy = false; this.queue.length = 0; this.batchGrants = 0; },
  };
}

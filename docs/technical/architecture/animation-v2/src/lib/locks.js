// Strict mutex used to serialize batches across the raft pipeline and the
// FSM apply loop. ONE waiter per release — the next batch's lead can only
// enter once the current one fully releases. This mirrors machine.go's
// single-threaded apply loop: the FSM picks up the next Ready tick's
// entries only after its current batch's writes are committed to cache.
//
//   acquire(tx)   — grants immediately if free; otherwise queues. Queued txs
//                   are woken up one at a time by release().
//   release()     — pops ONE waiter off the queue, sets staggerMs=0 on it,
//                   resolves its promise. If the queue is empty, marks the
//                   lock free.
//
// callbacks:
//   onBlock(tx) / onUnblock(tx) — UI red-pulse hooks for the queued tx.
export function makeLock({ onBlock, onUnblock } = {}) {
  return {
    busy: false,
    queue: [],

    async acquire(tx) {
      if (!this.busy) { this.busy = true; return; }
      onBlock?.(tx);
      await new Promise(res => this.queue.push({ res, tx }));
      onUnblock?.(tx);
    },

    release() {
      const next = this.queue.shift();
      if (!next) { this.busy = false; return; }
      // Lock stays busy — the new holder is `next`. No staggerMs needed
      // since strict mutex only ever wakes one tx at a time.
      next.tx.staggerMs = 0;
      next.res();
    },

    // Force-wake every queued tx without batch formation. Restart/Previous
    // paths use this to let cancelled coroutines bail.
    drain() {
      const drained = this.queue.splice(0);
      drained.forEach((item) => { item.tx.staggerMs = 0; });
      return drained;
    },

    reset() { this.busy = false; this.queue.length = 0; },
  };
}

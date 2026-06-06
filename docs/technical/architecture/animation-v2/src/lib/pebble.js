import { SvelteMap } from "svelte/reactivity";

// Durable key-value store mirror. Two reactivity gotchas avoided here:
//   1. SvelteMap (not plain Map) so the panel actually re-renders when the
//      FSM applies ⑤c writes — a `new Map()` nested in $state(...) is opaque
//      to the Svelte 5 proxy.
//   2. Methods reference `this.map`, NOT a closure-captured local. The closure
//      version mutates the original Map directly, bypassing the proxy ; using
//      `this.map` resolves through the wrapping proxy at call time so writes
//      reach subscribers.
export function makePebble() {
  return {
    map: new SvelteMap(),
    set(key, value)  { this.map.set(key, { value, flash: true }); },
    get(key)         { return this.map.get(key); },
    clear()          { this.map.clear(); },
  };
}

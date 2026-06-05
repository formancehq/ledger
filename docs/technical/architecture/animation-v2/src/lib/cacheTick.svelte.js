// Reactive trigger for cache-level state changes that DON'T go through a
// SvelteMap mutation — specifically, rotation (which replaces gen0 with a
// new SvelteMap instance) and reset. SvelteMap's internal signals only
// cover set/delete/clear on the instance the subscriber attached to; when
// the cache swaps the gen0 ref, subscribers stay listening to the old
// instance and never re-derive. Bumping this tick forces them to.
export const cacheTick = $state({ v: 0 });

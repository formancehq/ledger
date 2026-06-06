<script>
  import { app } from "../lib/state.svelte.js";

  let collapsed = $state(true);

  // $derived.by() with explicit function form — the previous expression-form
  // with `??` chained off two array.find() calls produced inconsistent reads
  // in Svelte 5 (header saw the entry, body saw null in the same render).
  // Force-coercing undefined → null at the tail makes the {#if selected}
  // and {#if !selected} branches agree.
  const selected = $derived.by(() => {
    if (app.selectedTxId == null) return null;
    const live = app.inflight.find(t => t.id === app.selectedTxId);
    if (live) return live;
    const done = app.completed.find(t => t.id === app.selectedTxId);
    return done ?? null;
  });
  const events  = $derived(selected ? selected.timeline : []);
  const isDone  = $derived(selected != null && app.inflight.every(t => t.id !== selected.id));

  // Accordion: only one event open at a time, defaulting to the newest. When
  // a new event lands (timeline grows), it becomes the open one and the
  // previous-newest auto-collapses. Manual clicks toggle the chosen index.
  let openIdx       = $state(-1);
  let lastSeenLen   = $state(0);
  let lastSeenTxId  = $state(null);
  $effect(() => {
    if (!selected) { openIdx = -1; lastSeenLen = 0; lastSeenTxId = null; return; }
    if (selected.id !== lastSeenTxId) {
      openIdx       = selected.timeline.length - 1;
      lastSeenLen   = selected.timeline.length;
      lastSeenTxId  = selected.id;
      return;
    }
    if (selected.timeline.length > lastSeenLen) {
      openIdx     = selected.timeline.length - 1;
      lastSeenLen = selected.timeline.length;
    } else if (selected.timeline.length < lastSeenLen) {
      openIdx     = selected.timeline.length - 1;
      lastSeenLen = selected.timeline.length;
    }
  });
  function toggle(i) { openIdx = (openIdx === i) ? -1 : i; }
</script>

<div class="payload-panel collapsible" class:collapsed>
  <div class="payload-header collapsible-header"
       onclick={() => collapsed = !collapsed}
       onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); collapsed = !collapsed; } }}
       role="button" tabindex="0">
    <span class="payload-title">
      {#if selected}
        <span class="payload-tx-chip" style="background: {selected.color}"></span>
        <span class="payload-tx-id">#{selected.id}</span>
        Lifecycle {isDone ? "· done" : ""}
      {:else}
        Lifecycle
      {/if}
    </span>
    {#if selected}
      <span class="cache-meta">{events.length} event{events.length === 1 ? "" : "s"}</span>
    {/if}
  </div>
  <div class="collapsible-body">
    {#if !selected}
      <div class="event-empty">No transaction selected. Send one — its lifecycle events will land here.</div>
    {:else if events.length === 0}
      <div class="event-empty">Tx just sent — events will appear here as steps run.</div>
    {:else}
      {#each events.slice().reverse() as ev, j (events.length - 1 - j)}
        {@const i = events.length - 1 - j}
        <div class="event-section" class:open={i === openIdx} style="--evt-color: {ev.color}">
          <div class="event-title"
               role="button" tabindex="0"
               onclick={() => toggle(i)}
               onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(i); } }}>
            <span class="event-dot"></span>
            <span class="event-title-text">{ev.title}</span>
            <span class="event-caret">{i === openIdx ? "▾" : "▸"}</span>
          </div>
          {#if i === openIdx}
            <pre class="payload-body">{@html ev.html}</pre>
          {/if}
        </div>
      {/each}
    {/if}
  </div>
</div>

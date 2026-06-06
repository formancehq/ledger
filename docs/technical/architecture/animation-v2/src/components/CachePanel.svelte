<script>
  import { cache } from "../lib/state.svelte.js";
  import { cacheTick } from "../lib/cacheTick.svelte.js";
  import { fmtValue } from "../lib/payloadBuilders.js";

  let collapsed = $state(true);

  // SvelteMap covers set/delete/clear reactivity within a generation.
  // cacheTick.v covers the gen0 / gen1 SWAP at rotation and the reset (the
  // cache object itself is plain, so the ref swap doesn't signal on its own).
  const gen0 = $derived.by(() => { cacheTick.v; return [...cache.gen0]; });
  const gen1 = $derived.by(() => { cacheTick.v; return [...cache.gen1]; });
  const meta = $derived.by(() => {
    cacheTick.v;
    return `gen ${cache.currentGen} · base ${cache.baseIdx.gen0} · |g0|=${cache.gen0.size} · |g1|=${cache.gen1.size}`;
  });

  // Trigger a row-flash whenever a fresh entry was just written. We consume
  // entry.flash on read so the animation only fires once per write.
  function rowClass(entry) {
    const cls = ["cache-row"];
    if (entry.deleted) cls.push("deleted");
    if (entry.flash)   { cls.push("row-flash"); entry.flash = false; }
    return cls.join(" ");
  }
</script>

<div class="cache-panel collapsible" class:collapsed>
  <div class="cache-header collapsible-header" onclick={() => collapsed = !collapsed}
       onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); collapsed = !collapsed; } }} role="button" tabindex="0">
    <span class="cache-title">Cache (in-memory, authoritative)</span>
    <span class="cache-meta">{meta}</span>
  </div>
  <div class="collapsible-body">
    <div class="cache-section">
      <div class="cache-section-title gen0">Gen0 <span class="cache-section-sub">— guaranteed for current index range</span></div>
      <ul class="cache-list">
        {#if gen0.length === 0}
          <li class="cache-empty">∅ empty</li>
        {:else}
          {#each gen0 as [key, entry] (key)}
            <li class={rowClass(entry)}>
              <span class="k">{key}</span>
              <span class="v">{fmtValue(entry.value)}</span>
              {#if entry.badge}<span class="badge b-{entry.badge.toLowerCase()}">{entry.badge}</span>{/if}
            </li>
          {/each}
        {/if}
      </ul>
    </div>
    <div class="cache-section">
      <div class="cache-section-title gen1">Gen1 <span class="cache-section-sub">— previous generation, discarded next rotation</span></div>
      <ul class="cache-list">
        {#if gen1.length === 0}
          <li class="cache-empty">∅ empty</li>
        {:else}
          {#each gen1 as [key, entry] (key)}
            <li class={rowClass(entry) + " gen1-line"}>
              <span class="k">{key}</span>
              <span class="v">{fmtValue(entry.value)}</span>
              {#if entry.badge}<span class="badge b-{entry.badge.toLowerCase()}">{entry.badge}</span>{/if}
            </li>
          {/each}
        {/if}
      </ul>
    </div>
  </div>
</div>

<script>
  import { pebble } from "../lib/state.svelte.js";
  import { fmtValue } from "../lib/payloadBuilders.js";

  let collapsed = $state(false);
  const entries = $derived([...pebble.map]);
  const meta = $derived(`${pebble.map.size} entr${pebble.map.size === 1 ? "y" : "ies"}`);

  function rowClass(entry) {
    const cls = ["pebble-row"];
    if (entry.flash) { cls.push("row-flash"); entry.flash = false; }
    return cls.join(" ");
  }
</script>

<div class="cache-panel collapsible" class:collapsed>
  <div class="cache-header collapsible-header" onclick={() => collapsed = !collapsed}
       onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); collapsed = !collapsed; } }} role="button" tabindex="0">
    <span class="cache-title">Pebble (durable store)</span>
    <span class="cache-meta">{meta}</span>
  </div>
  <div class="collapsible-body">
    <ul class="cache-list">
      {#if entries.length === 0}
        <li class="cache-empty">∅ empty</li>
      {:else}
        {#each entries as [key, entry] (key)}
          <li class={rowClass(entry)}>
            <span class="k">{key}</span>
            <span class="v">{fmtValue(entry.value)}</span>
          </li>
        {/each}
      {/if}
    </ul>
  </div>
</div>

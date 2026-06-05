<script>
  import { app } from "../lib/state.svelte.js";
  import { selectTx } from "../lib/controls.js";

  let collapsed = $state(false);
  const meta = $derived(app.completed.length === 0 ? "0" : `${app.completed.length} done`);
</script>

<div class="cache-panel collapsible" class:collapsed>
  <div class="cache-header collapsible-header"
       onclick={() => collapsed = !collapsed}
       onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); collapsed = !collapsed; } }}
       role="button" tabindex="0">
    <span class="cache-title">Transaction history</span>
    <span class="cache-meta">{meta}</span>
  </div>
  <div class="collapsible-body">
    <ul class="cache-list scrollable-list">
      {#if app.completed.length === 0}
        <li class="cache-empty">∅ no completed transactions yet</li>
      {:else}
        {#each app.completed.slice().reverse() as t (t.id)}
          <li class="inflight-row history-row"
              class:selected={t.id === app.selectedTxId}
              onclick={() => selectTx(t.id)}
              onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); selectTx(t.id); } }}
              role="button" tabindex="0">
            <span class="inflight-chip" style="background: {t.color}"></span>
            <span class="inflight-id">#{t.id}</span>
            <span class="inflight-route">{t.source} → {t.destination} · {t.amount} {t.asset}</span>
            <span class="inflight-step history-done">DONE</span>
          </li>
        {/each}
      {/if}
    </ul>
  </div>
</div>

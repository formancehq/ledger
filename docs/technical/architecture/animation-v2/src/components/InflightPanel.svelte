<script>
  import { app } from "../lib/state.svelte.js";
  import { steps } from "../lib/steps.js";
  import { selectTx } from "../lib/controls.js";

  let collapsed = $state(false);
  const meta = $derived(app.inflight.length === 0
    ? "0"
    : `${app.inflight.length} in flight` + (app.inflight.some(t => t.status === "blocked") ? " · some blocked" : ""));
</script>

<div class="cache-panel collapsible" class:collapsed>
  <div class="cache-header collapsible-header" onclick={() => collapsed = !collapsed}
       onkeydown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); collapsed = !collapsed; } }} role="button" tabindex="0">
    <span class="cache-title">In-flight transactions</span>
    <span class="cache-meta">{meta}</span>
  </div>
  <div class="collapsible-body">
    <ul class="cache-list scrollable-list">
      {#if app.inflight.length === 0}
        <li class="cache-empty">∅ none</li>
      {:else}
        {#each app.inflight as t (t.id)}
          <li class="inflight-row"
              class:blocked={t.status === "blocked"}
              class:selected={t.id === app.selectedTxId}
              onclick={() => selectTx(t.id)}
              role="button" tabindex="0">
            <span class="inflight-chip" style="background: {t.color}"></span>
            <span class="inflight-id">#{t.id}</span>
            <span class="inflight-route">{t.source} → {t.destination} · {t.amount} {t.asset}</span>
            <span class="inflight-step">{t.status === "blocked" ? "BLOCKED" : `step ${t.stepIndex + 1}/${steps.length}`}</span>
          </li>
        {/each}
      {/if}
    </ul>
  </div>
</div>

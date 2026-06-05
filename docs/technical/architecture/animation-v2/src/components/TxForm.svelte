<script>
  import { app } from "../lib/state.svelte.js";
  import { sendTx, repeatLast } from "../lib/controls.js";

  let form = $state({
    source:      "world",
    destination: "alice",
    asset:       "USD",
    amount:      100,
    ledger:      "main",
  });
  let error = $state("");

  function validate(f) {
    if (!f.source || !f.destination || !f.asset) return "All fields are required.";
    if (f.source === f.destination)               return "Source and destination must differ.";
    if (f.amount <= 0)                             return "Amount must be positive.";
    return null;
  }
  function onSend() {
    const err = validate(form);
    error = err || "";
    if (err) return;
    sendTx({ ...form, amount: Number(form.amount) });
  }
  const canRepeat = $derived(app.lastTxData != null);
</script>

<form class="tx-form" onsubmit={(e) => e.preventDefault()}>
  <label class="tx-field"><span>From</span>          <input bind:value={form.source} /></label>
  <label class="tx-field"><span>To</span>            <input bind:value={form.destination} /></label>
  <label class="tx-field"><span>Asset</span>         <input bind:value={form.asset} /></label>
  <label class="tx-field"><span>Amount</span>        <input type="number" min="1" bind:value={form.amount} /></label>
  <div class="tx-actions">
    <button class="ctrl-btn primary" type="button" onclick={onSend}>▶ Send</button>
    <button class="ctrl-btn"         type="button" onclick={repeatLast} disabled={!canRepeat}>↺ Repeat last</button>
  </div>
  <span class="tx-error">{error}</span>
</form>

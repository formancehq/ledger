<script>
  import { onMount } from "svelte";
  import { app } from "./lib/state.svelte.js";
  import { HIGHLIGHTS } from "./lib/geometry.js";
  import { bindRunBus } from "./lib/runCycle.js";
  import { startCompactor, stopCompactor } from "./lib/compactor.js";

  import Diagram        from "./components/Diagram.svelte";
  import StepBanner     from "./components/StepBanner.svelte";
  import TxForm         from "./components/TxForm.svelte";
  import TxControls     from "./components/TxControls.svelte";
  import InflightPanel  from "./components/InflightPanel.svelte";
  import HistoryPanel   from "./components/HistoryPanel.svelte";
  import PayloadPanel   from "./components/PayloadPanel.svelte";
  import CachePanel     from "./components/CachePanel.svelte";
  import PebblePanel    from "./components/PebblePanel.svelte";

  // The runtime emits per-step events through this bus. Last writer wins for
  // banner/highlights when multiple txs share a step.
  let banner = $state({
    title: "Ledger v3 — Architecture overview",
    desc:  "Fill the form and click ▶ Send to inject a transaction. Multiple sends queue up; the FSM + Raft pipeline serializes the critical section.",
    color: "var(--grpc)",
  });
  let currentHighlights = $state([]);

  bindRunBus({
    onStep(step, tx) {
      banner = { title: step.title, desc: step.desc, color: step.color };
      // The runtime won't emit onStep for steps it skipped (step.skipIf), so
      // we can just trust HIGHLIGHTS[tx.stepIndex] here — no conditional logic.
      currentHighlights = HIGHLIGHTS[tx.stepIndex] ?? [];
    },
  });

  onMount(() => {
    startCompactor();
    return () => stopCompactor();
  });

  // When the simulation finishes everywhere, fall back to a "done" message.
  // We watch on inflight emptying with $effect.
  $effect(() => {
    if (app.inflight.length === 0) {
      banner = {
        title: "Ledger v3 — Architecture overview",
        desc:  "Fill the form and click ▶ Send to inject a transaction. Multiple sends queue up; the FSM + Raft pipeline serializes the critical section.",
        color: "var(--grpc)",
      };
      currentHighlights = [];
    } else if (app.inflight.length === 0) {
      banner = {
        title: "Transaction complete · ready for next",
        desc:  "Send queues another transaction; pipeline locks will line them up if you stack them quickly.",
        color: "var(--resp)",
      };
      currentHighlights = [];
    }
  });
</script>

<div class="wrap">
  <header class="top-bar">
    <div class="brand">
      <h1>Ledger v3 — Architecture overview</h1>
      <div class="sub">gRPC → Admission → Raft (3 nodes) → FSM → Pebble → Workers</div>
    </div>
  </header>

  <div class="layout-row">
    <div class="layout-svg">
      <Diagram highlights={currentHighlights} />
      <StepBanner {...banner} />
    </div>
    <aside class="layout-panel">
      <TxForm />
      <TxControls />
      <InflightPanel />
      <HistoryPanel />
      <PayloadPanel />
      <CachePanel />
      <PebblePanel />
    </aside>
  </div>
</div>

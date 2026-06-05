<script>
  import { app } from "../lib/state.svelte.js";
  import { steps } from "../lib/steps.js";
  import { togglePause, stepNext, stepPrevious, restart } from "../lib/controls.js";

  const visible       = $derived(app.inflight.length > 0 || app.history.length > 0);
  const active        = $derived(app.inflight.length > 0);
  const nextDisabled  = $derived(!app.paused || !active);
  const prevDisabled  = $derived(app.activeActions > 0 || app.history.length === 0);
  const playLabel     = $derived(app.paused ? "▶ Resume" : "⏸ Pause");
  const lead          = $derived(app.inflight.reduce((m, t) => (m == null || t.stepIndex > m.stepIndex ? t : m), null));
  const stepNumber    = $derived(lead ? `${lead.stepIndex + 1}` : "—");
</script>

{#if visible}
  <div class="tx-controls">
    <button class="ctrl-btn" class:primary={app.paused} disabled={!active} onclick={togglePause}>{playLabel}</button>
    <button class="ctrl-btn" disabled={prevDisabled}  onclick={stepPrevious}>⏮ Previous</button>
    <button class="ctrl-btn" disabled={nextDisabled}  onclick={stepNext}>⏭ Next</button>
    <button class="ctrl-btn" onclick={restart}>⟲ Restart</button>
    <span class="step-indicator" class:paused={app.paused}>Step <b>{stepNumber}</b> / {steps.length}</span>
  </div>
{/if}

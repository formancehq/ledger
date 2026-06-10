import { useEngine, useEngineStore } from "../engine/EngineContext";

// Floating overlay anchored to the bottom-right of the diagram area.
// Send + Compact moved out — Send lives in the right-panel TxForm,
// Compact lives in the LogPanel header (closer to the WAL bounds it
// affects). What stays here is the tick rhythm: Pause/Resume, Next,
// Restart.

export default function EngineControls() {
  const { scheduler, resetEngine } = useEngine();
  const tick   = useEngineStore(s => s.tick);
  const paused = useEngineStore(s => s.paused);
  const playLabel = paused ? "▶ Resume" : "⏸ Pause";

  const togglePause = (): void => {
    if (paused) scheduler.start();
    else scheduler.pause();
  };

  return (
    <div className="tx-controls diagram-overlay">
      <button
        className={`ctrl-btn${paused ? " primary" : ""}`}
        onClick={togglePause}
      >
        {playLabel}
      </button>
      <button className="ctrl-btn" onClick={() => scheduler.step()}>⏭ Next</button>
      <button className="ctrl-btn" onClick={resetEngine}>⟲ Restart</button>
      <div className="step-indicator">
        tick <b>{tick}</b>
      </div>
    </div>
  );
}

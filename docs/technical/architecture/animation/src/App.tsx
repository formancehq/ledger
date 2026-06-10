import { EngineProvider } from "./engine/EngineContext";
import EngineDiagram from "./components/EngineDiagram";
import EngineControls from "./components/EngineControls";
import EngineBanner from "./components/EngineBanner";
import EngineTxForm from "./components/EngineTxForm";
import EngineInflightPanel from "./components/EngineInflightPanel";
import EngineHistoryPanel from "./components/EngineHistoryPanel";
import EngineLifecyclePanel from "./components/EngineLifecyclePanel";
import EngineCachePanel from "./components/EngineCachePanel";
import EnginePebblePanel from "./components/EnginePebblePanel";

// `EngineProvider` instantiates the engine inside `useState(() => createEngine())`
// so it boots ONCE at mount and survives re-renders. The legacy
// `useEffect(() => bootEngine())` is gone — initialization is now part
// of creating the handle.

export default function App() {
  return (
    <EngineProvider>
      <div className="wrap">
        <header className="top-bar">
          <div className="brand">
            <h1>Ledger v3 — Architecture overview</h1>
            <div className="sub">Message-passing engine · node graph · tick-driven</div>
          </div>
        </header>

        <div className="layout-row">
          <div className="layout-svg">
            <EngineBanner />
            <div className="diagram-wrap">
              <EngineDiagram />
              <EngineControls />
            </div>
          </div>
          <aside className="layout-panel">
            <EngineTxForm />
            <EngineInflightPanel />
            <EngineHistoryPanel />
            <EngineLifecyclePanel />
            <EngineCachePanel />
            <EnginePebblePanel />
          </aside>
        </div>
      </div>
    </EngineProvider>
  );
}

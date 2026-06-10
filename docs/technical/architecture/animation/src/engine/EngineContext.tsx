import { createContext, useContext, useState, type ReactNode } from "react";
import { useStore } from "zustand";
import { createEngine, type EngineHandle } from "./createEngine";
import type { EngineState } from "./store";

// React Context bridging the engine handle (engine-side imperative API)
// to the component tree (React-side reactive hooks).
//
// Pattern:
//   - `EngineProvider` instantiates ONE engine via `useState(() =>
//     createEngine())` so the same handle survives re-renders. Tests
//     can pass an explicit `engine` prop to inject a mock or shared
//     instance.
//   - `useEngine()` returns the handle for imperative calls
//     (scheduler.start, sendTx, …).
//   - `useEngineStore(selector)` reads state from the engine's Zustand
//     vanilla store — a drop-in for the old module-level hook.

const EngineContext = createContext<EngineHandle | null>(null);

export function EngineProvider({ children, engine }: { children: ReactNode; engine?: EngineHandle }) {
  const [instance] = useState(() => engine ?? createEngine());
  return <EngineContext.Provider value={instance}>{children}</EngineContext.Provider>;
}

// Imperative handle access — for components that call scheduler methods
// or sendTx/resetEngine.
export function useEngine(): EngineHandle {
  const handle = useContext(EngineContext);
  if (handle === null) {
    throw new Error("useEngine() must be called inside <EngineProvider>");
  }
  return handle;
}

// State-subscribing hook — drop-in replacement for the legacy
// `useEngineStore(selector)` from store.ts. Reads from the handle's
// vanilla Zustand store via `useStore(api, selector)`. For shallow
// equality, wrap the selector in `useShallow(...)` at the call site
// (Zustand v5 removed the 3rd-arg equality fn).
export function useEngineStore<T>(selector: (s: EngineState) => T): T {
  const { store } = useEngine();
  return useStore(store.api, selector);
}

// Engine entry — re-exports the consumer-facing surface. The actual
// per-instance wiring (Scheduler + EngineStore + edges + all node
// registrations) lives in `./createEngine.ts`; React consumers reach
// it through `<EngineProvider>` (see `./EngineContext.tsx`).
//
// Module-level singletons (`engineStore`, `scheduler`, `EDGES`,
// `bootEngine`, `sendTx`, `resetEngine`) are GONE — see
// /Users/gfyrag/.claude/plans/est-ce-qu-on-pourrait-mettre-graceful-catmull.md
// for the DI rationale.

export { createEngine, type EngineHandle } from "./createEngine";
export { EngineProvider, useEngine, useEngineStore } from "./EngineContext";
export { NODE } from "./topology";

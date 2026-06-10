import { useShallow } from "zustand/react/shallow";
import { useEngine, useEngineStore } from "../engine/EngineContext";
import { NODE } from "../engine/topology";
import type { ClientState } from "../engine/nodes/client";
import Collapsible from "./Collapsible";

const RECENT = 50;

export default function EngineHistoryPanel() {
  const { store } = useEngine();
  const client = useEngineStore(useShallow(s =>
    s.nodes[NODE.client]?.state as ClientState | undefined,
  ));
  const selectedTxId = useEngineStore(s => s.selectedTxId);
  const done = client?.done ?? {};
  const entries = Object.entries(done)
    .map(([id, v]) => [Number(id), v] as const)
    .sort(([a], [b]) => b - a)
    .slice(0, RECENT);

  return (
    <Collapsible title="History" meta={`${Object.keys(done).length} done`}>
      {entries.length === 0
        ? <div className="cache-empty">(none)</div>
        : (
          <ul className="cache-list scrollable-list">
            {entries.map(([id, v]) => {
              const selected = id === selectedTxId;
              return (
                <li key={id}
                    className={`inflight-row history-row${selected ? " selected" : ""}`}
                    onClick={() => store.setSelectedTxId(selected ? null : id)}>
                  <span className="history-done">{v.ok ? "✓" : "✗"}</span>
                  <span className="payload-tx-id">#{id}</span>
                  <span className="k">{v.order.source} → {v.order.destination}</span>
                  <span className="v">{v.order.amount} {v.order.asset}</span>
                </li>
              );
            })}
          </ul>
        )}
    </Collapsible>
  );
}

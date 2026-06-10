import { useShallow } from "zustand/react/shallow";
import { useEngine, useEngineStore } from "../engine/EngineContext";
import { NODE } from "../engine/topology";
import type { ClientState } from "../engine/nodes/client";
import Collapsible from "./Collapsible";

export default function EngineInflightPanel() {
  const { store } = useEngine();
  const client = useEngineStore(useShallow(s =>
    s.nodes[NODE.client]?.state as ClientState | undefined,
  ));
  const selectedTxId = useEngineStore(s => s.selectedTxId);
  const pending = client?.pending ?? {};
  const entries = Object.entries(pending);

  return (
    <Collapsible title="In-flight" meta={`${entries.length} tx`}>
      {entries.length === 0
        ? <div className="cache-empty">(none)</div>
        : (
          <ul className="cache-list">
            {entries.map(([id, order]) => {
              const txId = Number(id);
              const selected = txId === selectedTxId;
              return (
                <li key={id}
                    className={`inflight-row${selected ? " selected" : ""}`}
                    onClick={() => store.setSelectedTxId(selected ? null : txId)}>
                  <span className="payload-tx-id">#{id}</span>
                  <span className="k">{order.source} → {order.destination}</span>
                  <span className="v">{order.amount} {order.asset}</span>
                </li>
              );
            })}
          </ul>
        )}
    </Collapsible>
  );
}

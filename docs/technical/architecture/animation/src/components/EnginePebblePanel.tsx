import { useShallow } from "zustand/react/shallow";
import { useEngineStore } from "../engine/EngineContext";
import { NODE } from "../engine/topology";
import Collapsible from "./Collapsible";
import type { PebbleState } from "../engine/nodes/pebble";

// Pebble panel — durable K/V dump (log:{idx} + volumes per endpoint).

export default function EnginePebblePanel() {
  const pebble = useEngineStore(useShallow(s =>
    s.nodes[NODE.pebble]?.state as PebbleState | undefined,
  ));
  const entries = pebble ? Object.entries(pebble.map) : [];
  const meta = pebble
    ? `${entries.length} entr${entries.length === 1 ? "y" : "ies"} · persistedIdx=${pebble.highestPersistedIdx}`
    : "—";

  return (
    <Collapsible title="Pebble (durable)" meta={meta}>
      <ul className="cache-list">
        {entries.length === 0
          ? <li className="cache-empty">∅ empty</li>
          : entries.map(([key, entry]) => (
              <li key={key} className={entry.flashAge > 0 ? "row-flash" : undefined}>
                <span className="k">{key}</span>
                <span className="v">{entry.value}</span>
              </li>
            ))}
      </ul>
    </Collapsible>
  );
}

import { useEffect, useRef, useState } from "react";
import { useShallow } from "zustand/react/shallow";
import { useEngineStore } from "../engine/EngineContext";
import { NODE } from "../engine/topology";
import Collapsible from "./Collapsible";
import type { CacheState, CacheEntry } from "../engine/nodes/cache";

// Cache panel — two-section view (Gen0 / Gen1) with K/V rows + badges.
// A rotation (gen0 → gen1, gen0 emptied) briefly flashes the panel
// border so the user can see when the cache cycled.

export default function EngineCachePanel() {
  const cache = useEngineStore(useShallow(s =>
    s.nodes[NODE.cache]?.state as CacheState | undefined,
  ));

  const gen0 = cache ? Object.entries(cache.gen0) : [];
  const gen1 = cache ? Object.entries(cache.gen1) : [];
  const meta = cache
    ? `gen ${cache.currentGen} · base ${cache.baseIdx.gen0} · |g0|=${gen0.length} · |g1|=${gen1.length}`
    : "—";

  // Rotation flash — when currentGen advances, briefly add the flash
  // class so the user sees that the cache just cycled.
  const lastGenRef = useRef<number | undefined>(undefined);
  const [flash, setFlash] = useState(false);
  useEffect(() => {
    if (cache == null) return;
    const prev = lastGenRef.current;
    lastGenRef.current = cache.currentGen;
    if (prev != null && cache.currentGen > prev) {
      setFlash(true);
      const t = setTimeout(() => setFlash(false), 1000);
      return () => clearTimeout(t);
    }
  }, [cache?.currentGen]);

  const wrapperClass = `cache-panel${flash ? " rotate-flash" : ""}`;

  return (
    <Collapsible title="Cache (in-memory)" meta={meta} wrapperClass={wrapperClass}>
      <Section title="Gen0" sub="guaranteed for current index range" entries={gen0} dim={false} />
      <Section title="Gen1" sub="previous generation, discarded next rotation" entries={gen1} dim={true} />
    </Collapsible>
  );
}

interface SectionProps {
  title:   string;
  sub:     string;
  entries: Array<[string, CacheEntry]>;
  dim:     boolean;
}

function Section({ title, sub, entries, dim }: SectionProps) {
  const sectionClass = title === "Gen0" ? "cache-section-title gen0" : "cache-section-title gen1";
  return (
    <div className="cache-section">
      <div className={sectionClass}>{title} <span className="cache-section-sub">— {sub}</span></div>
      <ul className="cache-list">
        {entries.length === 0
          ? <li className="cache-empty">∅ empty</li>
          : entries.map(([key, entry]) => (
              <li key={key} className={dim ? "gen1-line" : undefined}>
                <span className="k">{key}</span>
                <span className="v">{entry.value}</span>
                {entry.badge && (
                  <span className={`badge b-${entry.badge.toLowerCase()}`}>{entry.badge}</span>
                )}
              </li>
            ))}
      </ul>
    </div>
  );
}

import { useEffect, useRef, useState } from "react";
import { useShallow } from "zustand/react/shallow";
import { useEngineStore } from "../engine/EngineContext";
import { describeMsgLong, detailsFor, labelFor, relevantTxIds, txIdOf } from "../engine/banner";
import Collapsible from "./Collapsible";

// Per-tx lifecycle accordion — filtered tail of the engine log keyed
// by the user-selected txId. One event expanded at a time; the latest
// event of the selected tx is auto-opened. Clicking a header toggles
// the body, which shows the long description + the structured details
// for the underlying msg.

const PALETTE = [
  "#82aaff", "#c3e88d", "#ffcb6b", "#c792ea",
  "#89ddff", "#addb67", "#ff9bd2", "#fbc02d",
];
function colorForTx(txId: number | null): string {
  if (txId == null) return "#7a8aa6";
  return PALETTE[(txId - 1) % PALETTE.length];
}

export default function EngineLifecyclePanel() {
  const selectedTxId = useEngineStore(s => s.selectedTxId);
  const log          = useEngineStore(useShallow(s => s.log));

  const filtered = selectedTxId == null
    ? []
    : log.filter(e => relevantTxIds(e.msg).includes(selectedTxId));

  // Auto-open the latest event whenever a new one arrives for the
  // selected tx, or when the selection changes.
  const [openIdx, setOpenIdx] = useState<number | null>(null);
  const lastSeenLenRef = useRef(0);
  const lastSeenTxIdRef = useRef<number | null>(null);
  useEffect(() => {
    const txChanged = lastSeenTxIdRef.current !== selectedTxId;
    const grew      = filtered.length > lastSeenLenRef.current;
    if (txChanged || grew) {
      setOpenIdx(filtered.length === 0 ? null : filtered.length - 1);
    }
    lastSeenTxIdRef.current = selectedTxId;
    lastSeenLenRef.current  = filtered.length;
  }, [selectedTxId, filtered.length]);

  const title = selectedTxId == null ? "Lifecycle" : `Lifecycle · tx#${selectedTxId}`;
  const meta  = selectedTxId == null ? "no tx selected" : `${filtered.length} event${filtered.length > 1 ? "s" : ""}`;

  return (
    <Collapsible title={title} meta={meta} wrapperClass="payload-panel">
      {selectedTxId == null ? (
        <div className="cache-empty">Click a tx in In-flight or History to see its full event chain.</div>
      ) : filtered.length === 0 ? (
        <div className="cache-empty">(no events yet — tx is queued)</div>
      ) : (
        <div className="lifecycle-accordion">
          {filtered.map((e, i) => {
            const open  = i === openIdx;
            const isLast = i === filtered.length - 1;
            const info  = describeMsgLong(e.node, e.msg);
            const detail = detailsFor(e.msg);
            const dotColor = colorForTx(txIdOf(e.msg));
            return (
              <div key={`${e.tick}-${i}`} className={`event-section${open ? " open" : ""}${isLast ? " latest" : ""}`}>
                <button
                  type="button"
                  className="event-title"
                  onClick={() => setOpenIdx(open ? null : i)}
                >
                  <span className="event-caret">{open ? "▾" : "▸"}</span>
                  <span className="event-dot" style={{ background: dotColor }} />
                  <span className="event-tick">@{e.tick}</span>
                  <span className="event-node">{labelFor(e.node)}</span>
                  <span className="event-headline">{info.title}</span>
                </button>
                {open && (
                  <div className="payload-body">
                    <p className="event-desc">{info.desc}</p>
                    {detail.length > 0 && (
                      <ul className="event-details">
                        {detail.map(([label, value], j) => (
                          <li key={j}>
                            <span className="k">{label}</span>
                            <span className="v">{value}</span>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </Collapsible>
  );
}

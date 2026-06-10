import { useShallow } from "zustand/react/shallow";
import { useEngineStore } from "../engine/EngineContext";
import { describeMsgLong, relevantTxIds } from "../engine/banner";

// Bottom banner — legacy-style technical narrative. If a tx is
// selected (via Inflight / History row click), shows the most recent
// event involving THAT tx with a paragraph explaining the protocol
// detail behind it. With no selection, falls back to the engine-wide
// last event so the banner is never blank once anything has happened.

export default function EngineBanner() {
  const log          = useEngineStore(useShallow(s => s.log));
  const tick         = useEngineStore(s => s.tick);
  const selectedTxId = useEngineStore(s => s.selectedTxId);

  // Pick the most recent log entry to describe.
  let entry = null as null | (typeof log)[number];
  if (selectedTxId != null) {
    for (let i = log.length - 1; i >= 0; i--) {
      if (relevantTxIds(log[i].msg).includes(selectedTxId)) {
        entry = log[i];
        break;
      }
    }
  } else {
    entry = log[log.length - 1] ?? null;
  }

  if (!entry) {
    return (
      <div className="step-banner-html">
        <span className="step-dot-html" />
        <div className="step-content">
          <div className="step-title">tick {tick} — engine idle</div>
          <div className="step-desc">Click <b>▶ Send</b> to inject a tx, then <b>⏭ Next</b> or <b>▶ Resume</b> to advance ticks. Click a row in <b>In-flight</b> or <b>History</b> to focus this banner on that tx's lifecycle.</div>
        </div>
      </div>
    );
  }

  const { title, desc } = describeMsgLong(entry.node, entry.msg);
  const prefix = selectedTxId == null ? `@${entry.tick}` : `@${entry.tick} · tx#${selectedTxId}`;

  return (
    <div className="step-banner-html">
      <span className="step-dot-html" />
      <div className="step-content">
        <div className="step-title">{prefix} · {title}</div>
        <div className="step-desc">{desc}</div>
      </div>
    </div>
  );
}

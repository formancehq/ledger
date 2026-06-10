import type { ReactNode } from "react";
import { labelFor } from "../engine/banner";
import type { Batch, Msg, TokenInFlight } from "../engine/types";

interface Props {
  hovered: { token: TokenInFlight; x: number; y: number } | null;
  pinned?: boolean;
}

// Floating panel showing the payload of a token currently in flight.
// Triggered when the user hovers an animated dot in the diagram.
// When pinned (user clicked the dot), the panel sticks until a click
// elsewhere unpins it; pointer events are enabled so its scrollbar is
// usable for long batches.

export default function EngineTokenOverlay({ hovered, pinned = false }: Props) {
  if (!hovered) return null;
  const { token } = hovered;

  const PANEL_W = 320;
  const PANEL_H_MAX = 280;
  const pad = 8;
  let left = hovered.x + 14;
  let top  = hovered.y + 14;
  if (left + PANEL_W + pad > window.innerWidth)  left = hovered.x - 14 - PANEL_W;
  if (top  + PANEL_H_MAX + pad > window.innerHeight) top = window.innerHeight - PANEL_H_MAX - pad;
  if (left < pad) left = pad;
  if (top  < pad) top  = pad;

  return (
    <div
      className={`batch-overlay engine-overlay${pinned ? " pinned" : ""}`}
      style={{
        position: "fixed", left, top,
        width: PANEL_W, maxHeight: PANEL_H_MAX,
        pointerEvents: pinned ? "auto" : "none", zIndex: 1000,
      }}
      onClick={(e) => e.stopPropagation()}
    >
      <div className="batch-overlay-title">
        <span className="batch-overlay-kind kind-propose">{token.msg.kind}</span>
        <span className="batch-overlay-sub">{labelFor(token.from)} → {labelFor(token.to)}</span>
        {pinned && <span className="batch-overlay-pinned" title="Pinned — click outside to unpin">📌</span>}
      </div>
      <div className="engine-overlay-body">
        {formatMsg(token.msg)}
      </div>
    </div>
  );
}

// Compact list of entries riding inside a batched msg (AppendEntries,
// ApplyTrigger, ConsultCache, WritePebble, …). One line per entry with
// idx + txId + the order summary so the user can see WHICH txs are
// in this batch without expanding any panel.
function BatchList({ batch }: { batch: Batch }) {
  return (
    <ul className="engine-overlay-batch">
      {batch.entries.map(e => (
        <li key={e.index}>
          <span className="engine-overlay-batch-idx">idx {e.index}</span>
          <span className="engine-overlay-batch-tx">tx#{e.txId}</span>
          <span className="engine-overlay-batch-order">{e.order.source}→{e.order.destination} {e.order.amount}{e.order.asset}</span>
        </li>
      ))}
    </ul>
  );
}

function formatMsg(msg: Msg): ReactNode {
  const rows: Array<[string, ReactNode]> = [];
  switch (msg.kind) {
    case "Propose":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["order", `${msg.order.source} → ${msg.order.destination}`]);
      rows.push(["amount", `${msg.order.amount} ${msg.order.asset}`]);
      rows.push(["ref", msg.order.reference]);
      break;
    case "AppendEntries": {
      rows.push(["entries", msg.batch.entries.length === 0
        ? "(heartbeat — 0 entries)"
        : <BatchList batch={msg.batch} />]);
      rows.push(["leaderCommit", String(msg.leaderCommit)]);
      rows.push(["term", String(msg.term)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    }
    case "AppendResp":
      rows.push(["matchIdx", String(msg.matchIdx)]);
      rows.push(["success",  String(msg.success)]);
      rows.push(["from",     labelFor(msg.from)]);
      break;
    case "ApplyTrigger":
    case "ApplyTriggerAck":
      rows.push(["upTo",    String(msg.batch.upTo)]);
      rows.push(["entries", msg.batch.entries.length === 0
        ? "(none)"
        : <BatchList batch={msg.batch} />]);
      break;
    case "FsmAck":
      rows.push(["appliedIdx", String(msg.batch.upTo)]);
      rows.push(["from",       labelFor(msg.from)]);
      break;
    case "ClientResp":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["ok",   String(msg.ok)]);
      break;
    case "ReadTracker":
    case "IncrementTracker":
    case "ReadCache":
    case "ReadPebble":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ReadTrackerResp":
    case "IncrementTrackerResp":
      rows.push(["txId",      `#${msg.txId}`]);
      rows.push(["nextIndex", String(msg.nextIndex)]);
      break;
    case "ReadCacheResp":
    case "ReadPebbleResp":
      rows.push(["txId", `#${msg.txId}`]);
      break;
    case "ConsultCache":
    case "WritePebble":
    case "PreparedBatch":
      rows.push(["upTo",    String(msg.batch.upTo)]);
      rows.push(["entries", msg.batch.entries.length === 0
        ? "(none)"
        : <BatchList batch={msg.batch} />]);
      rows.push(["from",    labelFor(msg.from)]);
      break;
    case "ConsultProcessing":
    case "WriteWAL":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "CacheResp":
    case "ProcessingResp":
    case "PebbleAck":
      rows.push(["upTo", String(msg.batch.upTo)]);
      break;
    case "WALAck":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "NotifyLogs":
      rows.push(["upTo", String(msg.batch.upTo)]);
      break;
    case "Compact":
      rows.push(["—", "compactor poke"]);
      break;
    case "ReadPersisted":
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ReadPersistedResp":
      rows.push(["upTo", String(msg.upTo)]);
      break;
    case "TruncateWAL":
      rows.push(["upTo", String(msg.upTo)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "TruncateWALAck":
      rows.push(["firstIdx", String(msg.firstIdx)]);
      break;
  }
  return (
    <ul className="engine-overlay-rows">
      {rows.map(([k, v]) => (
        <li key={k}>
          <span className="engine-overlay-k">{k}</span>
          <span className="engine-overlay-v">{v}</span>
        </li>
      ))}
    </ul>
  );
}

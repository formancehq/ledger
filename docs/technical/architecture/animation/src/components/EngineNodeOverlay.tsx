import type { ReactNode } from "react";
import { useEngineStore } from "../engine/EngineContext";
import { useShallow } from "zustand/react/shallow";
import { NODE } from "../engine/topology";
import { labelFor } from "../engine/banner";
import type { NodeId } from "../engine/types";

interface Props {
  hovered: { id: NodeId; x: number; y: number } | null;
}

// Floating panel showing the hovered node's internal state. Reads
// engineStore.nodes[id] and routes to a per-NodeId formatter so the
// most relevant fields surface cleanly (commitIdx, log size, matchIdx
// per follower, …) rather than a generic JSON dump.

export default function EngineNodeOverlay({ hovered }: Props) {
  const nodes = useEngineStore(useShallow(s => s.nodes));
  if (!hovered) return null;
  const snap = nodes[hovered.id];
  if (!snap) return null;

  const PANEL_W = 300;
  const PANEL_H_MAX = 320;
  const pad = 8;
  let left = hovered.x + 14;
  let top  = hovered.y + 14;
  if (left + PANEL_W + pad > window.innerWidth)  left = hovered.x - 14 - PANEL_W;
  if (top  + PANEL_H_MAX + pad > window.innerHeight) top = window.innerHeight - PANEL_H_MAX - pad;
  if (left < pad) left = pad;
  if (top  < pad) top  = pad;

  return (
    <div
      className="batch-overlay engine-overlay"
      style={{
        position: "fixed", left, top,
        width: PANEL_W, maxHeight: PANEL_H_MAX,
        pointerEvents: "none", zIndex: 1000,
      }}
    >
      <div className="batch-overlay-title">
        <span className="batch-overlay-kind kind-apply">{labelFor(hovered.id)}</span>
        <span className="batch-overlay-id">{hovered.id}</span>
        <span className="batch-overlay-sub">mailbox: {snap.mailboxSize}</span>
      </div>
      <div className="engine-overlay-body">
        {formatState(hovered.id, snap.state)}
      </div>
    </div>
  );
}

// Per-NodeId formatter — surfaces the few fields a reader cares about
// at a glance. Returns plain text (kept readable in monospace).
function formatState(id: NodeId, state: unknown): ReactNode {
  const s = state as Record<string, unknown>;
  const rows: Array<[string, ReactNode]> = [];

  switch (id) {
    case NODE.leader: {
      const log = (s.log as Array<{ index: number; txId: number }>) ?? [];
      const matchIdx = (s.matchIdx as Record<string, number>) ?? {};
      const pending  = (s.pendingBatch as Array<{ index: number }>) ?? [];
      rows.push(["log", `${log.length} entries${log.length > 0 ? ` (last idx ${log[log.length - 1].index})` : ""}`]);
      rows.push(["commitIdx",  String(s.commitIdx ?? 0)]);
      rows.push(["appliedIdx", String(s.appliedIdx ?? 0)]);
      rows.push(["matchIdx",   matchSummary(matchIdx)]);
      if (pending.length > 0) rows.push(["pendingBatch", `${pending.length} entr${pending.length > 1 ? "ies" : "y"} awaiting flush`]);
      break;
    }
    case NODE.followerF1:
    case NODE.followerF2: {
      const log = (s.log as unknown[]) ?? [];
      rows.push(["log",        `${log.length} entries`]);
      rows.push(["commitIdx",  String(s.commitIdx ?? 0)]);
      rows.push(["appliedIdx", String(s.appliedIdx ?? 0)]);
      break;
    }
    case NODE.fsmL: {
      const pending   = s.pending as { upTo: number; phase: string; awaiting?: Set<string> } | null;
      const gestating = s.gestating as { upTo: number } | null;
      rows.push(["appliedIdx", String(s.appliedIdx ?? 0)]);
      // Two-slot stager: inflight (= pending phase) + gestating slot.
      // Overflow is held by the leader (see LeaderState.pendingApply).
      if (pending) {
        const waiting = pending.awaiting ? ` waiting=${[...pending.awaiting].join("+")}` : "";
        rows.push(["inflight",    `phase=${pending.phase}, upTo=${pending.upTo}${waiting}`]);
      } else {
        rows.push(["inflight",    "(idle)"]);
      }
      rows.push(["gestating",     gestating == null ? "(empty)" : `upTo=${gestating.upTo}`]);
      break;
    }
    case NODE.fsmF1:
    case NODE.fsmF2: {
      rows.push(["appliedIdx", String(s.appliedIdx ?? 0)]);
      break;
    }
    case NODE.tracker: {
      rows.push(["nextIndex",  String(s.nextIndex  ?? 1)]);
      rows.push(["reads",      String(s.reads      ?? 0)]);
      rows.push(["increments", String(s.increments ?? 0)]);
      break;
    }
    case NODE.walLeader:
    case NODE.walF1:
    case NODE.walF2: {
      rows.push(["bounds",      `[${s.firstSyncIdx ?? 1}..${s.lastSyncIdx ?? 0}]`]);
      rows.push(["fsyncs",      String(s.fsyncs      ?? 0)]);
      rows.push(["truncations", String(s.truncations ?? 0)]);
      break;
    }
    case NODE.compactor: {
      rows.push(["attempts",      String(s.attempts      ?? 0)]);
      rows.push(["lastTruncated", String(s.lastTruncated ?? 0)]);
      if (s.pendingUpTo != null) rows.push(["pending", `awaiting truncate upTo=${s.pendingUpTo}`]);
      break;
    }
    case NODE.cache:      rows.push(["consultsServed", String(s.consultsServed ?? 0)]); break;
    case NODE.pebble: {
      rows.push(["writesServed",        String(s.writesServed        ?? 0)]);
      rows.push(["highestPersistedIdx", String(s.highestPersistedIdx ?? 0)]);
      break;
    }
    case NODE.processing: rows.push(["computed", String(s.computed ?? 0)]); break;
    case NODE.notifier:   rows.push(["lastSeq",  String(s.lastSeq  ?? 0)]); break;
    case NODE.workerIndex:
    case NODE.workerSinks:
    case NODE.workerArch:
    case NODE.workerSealer: {
      rows.push(["processed",  String(s.processed  ?? 0)]);
      rows.push(["highestSeq", String(s.highestSeq ?? 0)]);
      break;
    }
    case NODE.admission: {
      const inFlight = (s.inFlight as Record<string, unknown>) ?? {};
      rows.push(["forwarded", String(s.forwarded ?? 0)]);
      const txs = Object.keys(inFlight);
      if (txs.length > 0) rows.push(["preload in-flight", `tx#${txs.join(", tx#")}`]);
      break;
    }
  }
  if (rows.length === 0) rows.push(["(no inspectable state)", ""]);

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

function matchSummary(m: Record<string, number>): string {
  return Object.entries(m)
    .map(([k, v]) => `${labelFor(k as NodeId)}=${v}`)
    .join(", ");
}

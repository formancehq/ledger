import { NODE } from "./topology";
import type { Msg, NodeId } from "./types";

// Human-readable labels for the topology nodes. Keeps the banner text
// terse and consistent — "Leader" reads better than "nodeL" in a
// narrative line, and grouping followers under generic "F1 / F2" hides
// implementation detail.
const NODE_LABELS: Record<NodeId, string> = {
  [NODE.client]:        "Client",
  [NODE.grpc]:          "gRPC",
  [NODE.controller]:    "Controller",
  [NODE.admission]:     "Admission",
  [NODE.tracker]:       "IndexTracker",
  [NODE.leader]:        "Leader",
  [NODE.followerF1]:    "F1",
  [NODE.followerF2]:    "F2",
  [NODE.walLeader]:     "WAL(L)",
  [NODE.walF1]:         "WAL(F1)",
  [NODE.walF2]:         "WAL(F2)",
  [NODE.fsmL]:          "FSM(L)",
  [NODE.fsmF1]:         "FSM(F1)",
  [NODE.fsmF2]:         "FSM(F2)",
  [NODE.cache]:         "Cache",
  [NODE.pebble]:        "Pebble",
  [NODE.processing]:    "Processing",
  [NODE.notifier]:      "Notifier",
  [NODE.compactor]:     "Compactor",
  [NODE.workerIndex]:   "IndexBuilder",
  [NODE.workerSinks]:   "EventSinks",
  [NODE.workerArch]:    "ColdStorage",
  [NODE.workerSealer]:  "Sealer",
};

export function labelFor(id: NodeId): string {
  return NODE_LABELS[id] ?? id;
}

// Every txId a msg "touches" — used to filter the log by tx for the
// lifecycle panel. For AppendEntries it's the union of all the
// entries' txIds, so a batched AE shows up in EACH tx's lifecycle.
// Returns an empty array for batch-level msgs (commit triggers,
// fsync acks, heartbeats with no entries, …).
export function relevantTxIds(msg: Msg): number[] {
  switch (msg.kind) {
    case "Propose":              return [msg.txId];
    case "ClientResp":           return [msg.txId];
    // Every batch-level msg carries the full lot of entries through
    // `batch.entries`. One uniform case handles them all — no more
    // grey dots on WriteWAL / WALAck / FsmAck / NotifyLogs etc.
    case "AppendEntries":
    case "AppendResp":
    case "ApplyTrigger":
    case "ApplyTriggerAck":
    case "FsmAck":
    case "PreparedBatch":
    case "ConsultCache":
    case "CacheResp":
    case "ConsultProcessing":
    case "ProcessingResp":
    case "WritePebble":
    case "PebbleAck":
    case "NotifyLogs":
    case "WriteWAL":
    case "WALAck":               return msg.batch.entries.map(e => e.txId);
    case "ReadTracker":
    case "ReadTrackerResp":
    case "IncrementTracker":
    case "IncrementTrackerResp":
    case "ReadCache":
    case "ReadCacheResp":
    case "ReadPebble":
    case "ReadPebbleResp":       return [msg.txId];
    default:                     return [];
  }
}

// Pick a single tx id from a msg — used to color the in-flight token
// when multiple txs are racing. Heartbeats / batch-level msgs return
// null and render in grey.
export function txIdOf(msg: Msg): number | null {
  const ids = relevantTxIds(msg);
  return ids.length > 0 ? ids[0] : null;
}

// Structured fields for the lifecycle accordion's expanded body. Each
// row is a (label, value) pair. Per kind, surface the most relevant
// fields — keeps the body tighter than a raw JSON dump.
export function detailsFor(msg: Msg): Array<[string, string]> {
  const rows: Array<[string, string]> = [];
  switch (msg.kind) {
    case "Propose":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["source", msg.order.source]);
      rows.push(["destination", msg.order.destination]);
      rows.push(["asset", msg.order.asset]);
      rows.push(["amount", String(msg.order.amount)]);
      rows.push(["reference", msg.order.reference]);
      break;
    case "AppendEntries":
      rows.push(["entries", msg.batch.entries.length === 0 ? "(heartbeat — 0 entries)" : msg.batch.entries.map(e => `idx ${e.index} tx#${e.txId}`).join("; ")]);
      rows.push(["leaderCommit", String(msg.leaderCommit)]);
      rows.push(["term", String(msg.term)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "AppendResp":
      rows.push(["matchIdx", String(msg.matchIdx)]);
      rows.push(["success", String(msg.success)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ApplyTrigger":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["entries", msg.batch.entries.map(e => `idx ${e.index} tx#${e.txId}`).join("; ")]);
      break;
    case "FsmAck":
      rows.push(["appliedIdx", String(msg.batch.upTo)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ClientResp":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["ok", String(msg.ok)]);
      break;
    case "ReadTracker":
    case "IncrementTracker":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ReadTrackerResp":
    case "IncrementTrackerResp":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["nextIndex", String(msg.nextIndex)]);
      break;
    case "ReadCache":
    case "ReadPebble":
      rows.push(["txId", `#${msg.txId}`]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ReadCacheResp":
    case "ReadPebbleResp":
      rows.push(["txId", `#${msg.txId}`]);
      break;
    case "ConsultCache":
    case "WritePebble":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["entries", msg.batch.entries.map(e => `idx ${e.index} tx#${e.txId}`).join("; ")]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ConsultProcessing":
    case "WriteWAL":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "ApplyTriggerAck":
    case "CacheResp":
    case "ProcessingResp":
    case "PebbleAck":
      rows.push(["upTo", String(msg.batch.upTo)]);
      break;
    case "PreparedBatch":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["entries", msg.batch.entries.map(e => `idx ${e.index} tx#${e.txId}`).join("; ")]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "WALAck":
      rows.push(["upTo", String(msg.batch.upTo)]);
      rows.push(["from", labelFor(msg.from)]);
      break;
    case "NotifyLogs":
      rows.push(["upTo", String(msg.batch.upTo)]);
      break;
    case "Compact":
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
  return rows;
}

// Turn a (node, msg) pair into a one-line narrative. `node` is the
// node whose handler processed the msg, so the phrasing is "X did
// something because of msg". Phrasing favours active voice and elides
// uninteresting fields (e.g., `term` is always 1 in this demo).
export function describeMsg(node: NodeId, msg: Msg): string {
  const at = labelFor(node);
  switch (msg.kind) {
    case "Propose":
      return `${at} forwards tx#${msg.txId}`;
    case "AppendEntries": {
      const n = msg.batch.entries.length;
      if (n === 0) return `${at} ← heartbeat AE (leaderCommit=${msg.leaderCommit})`;
      const idxs = msg.batch.entries.map(e => e.index).join(", ");
      const txs  = msg.batch.entries.map(e => `tx#${e.txId}`).join(", ");
      return `${at} ← AE [${idxs}] (${txs}), leaderCommit=${msg.leaderCommit}`;
    }
    case "AppendResp":
      return `${at} ← ${labelFor(msg.from)} acked matchIdx=${msg.matchIdx}`;
    case "ApplyTrigger":
      return `${at} ← apply trigger upTo=${msg.batch.upTo}`;
    case "ApplyTriggerAck":
      return `${at} ← fsm.PrepareEntries returned (upTo=${msg.batch.upTo})`;
    case "PreparedBatch":
      return `${at} ← prepared batch ready (upTo=${msg.batch.upTo})`;
    case "FsmAck":
      return `${at} ← FSM applied=${msg.batch.upTo}`;
    case "ClientResp":
      return `${at} forwards client response for tx#${msg.txId}`;

    case "ReadTracker":
      return `${at} ← peek next index for tx#${msg.txId}`;
    case "ReadTrackerResp":
      return `${at} ← tracker says next=${msg.nextIndex} (tx#${msg.txId})`;
    case "IncrementTracker":
      return `${at} ← reserve index for tx#${msg.txId}`;
    case "IncrementTrackerResp":
      return `${at} ← tracker reserved index=${msg.nextIndex} for tx#${msg.txId}`;

    case "ReadCache":
      return `${at} ← cache preload check for tx#${msg.txId}`;
    case "ReadCacheResp":
      return `${at} ← cache resp for tx#${msg.txId}`;
    case "ReadPebble":
      return `${at} ← pebble lazy-load for tx#${msg.txId}`;
    case "ReadPebbleResp":
      return `${at} ← pebble resp for tx#${msg.txId}`;

    case "ConsultCache":
      return `${at} ← MirrorPreload upTo=${msg.batch.upTo}`;
    case "CacheResp":
      return `${at} ← cache resp upTo=${msg.batch.upTo}`;
    case "ConsultProcessing":
      return `${at} ← compute / numscript upTo=${msg.batch.upTo}`;
    case "ProcessingResp":
      return `${at} ← processing resp upTo=${msg.batch.upTo}`;
    case "WritePebble":
      return `${at} ← batch.Commit upTo=${msg.batch.upTo}`;
    case "PebbleAck":
      return `${at} ← pebble fsync ack upTo=${msg.batch.upTo}`;

    case "WriteWAL":
      return `${at} ← fsync WAL upTo=${msg.batch.upTo}`;
    case "WALAck":
      return `${at} ← WAL fsync ack upTo=${msg.batch.upTo}`;

    case "NotifyLogs":
      return `${at} ← NotifyLogs upTo=${msg.batch.upTo}`;

    case "Compact":
      return `${at} ← compact trigger`;
    case "ReadPersisted":
      return `${at} ← read highestPersistedIdx`;
    case "ReadPersistedResp":
      return `${at} ← pebble.persisted=${msg.upTo}`;
    case "TruncateWAL":
      return `${at} ← truncate WAL upTo=${msg.upTo}`;
    case "TruncateWALAck":
      return `${at} ← WAL truncated, firstIdx=${msg.firstIdx}`;
  }
}

// Long-form description: title + paragraph explaining the protocol
// detail behind a given (node, msg) event. Drives the rich bottom
// banner so the user gets the legacy-style "step.desc" feel back.
//
// `node` is the handler that processed the msg, so phrasing favors
// "X just did Y because Z".
export function describeMsgLong(node: NodeId, msg: Msg): { title: string; desc: string } {
  const at = labelFor(node);
  switch (msg.kind) {
    case "Propose":
      return {
        title: `${at} — Propose tx#${msg.txId}`,
        desc:  `Admission has finished preloading and reserved an index for tx#${msg.txId} (${msg.order.source} → ${msg.order.destination}, ${msg.order.amount} ${msg.order.asset}). The Propose now sits in the leader's proposeCh until the next Ready loop, where it'll be folded into the next AppendEntries batch alongside any other proposals received this tick.`,
      };
    case "AppendEntries": {
      if (msg.batch.entries.length === 0) {
        return {
          title: `${at} — heartbeat AppendEntries`,
          desc:  `Empty AE from the leader carrying the latest leaderCommit=${msg.leaderCommit}. No new entries to fsync — followers can advance their commit index and trigger local FSM apply on previously-replicated entries. This is how committed-but-not-yet-propagated state reaches the followers when there's no fresh traffic.`,
        };
      }
      const idxs = msg.batch.entries.map(e => e.index).join(",");
      return {
        title: `${at} — AppendEntries (${msg.batch.entries.length} entr${msg.batch.entries.length > 1 ? "ies" : "y"})`,
        desc:  `Leader broadcasts indices [${idxs}] with leaderCommit=${msg.leaderCommit}. Receiving followers fsync the entries to their local WAL, then AppendResp with their new matchIdx so the leader can recompute quorum. In real Raft this is one batch per Ready emission — drain-all in the scheduler models that "all proposes received this tick collapse into one AE" behavior.`,
      };
    }
    case "AppendResp":
      return {
        title: `${at} — AppendResp(${msg.matchIdx})`,
        desc:  `${labelFor(msg.from)} confirms its WAL is fsync'd through matchIdx=${msg.matchIdx}. The leader updates its matchIdx[${labelFor(msg.from)}] and recomputes quorum (the k-th highest matchIdx for k = quorum size). Crossing a new watermark advances commitIdx and triggers ApplyTrigger to fsmL + a heartbeat AE to propagate to the other followers.`,
      };
    case "ApplyTrigger":
      return {
        title: `${at} — ApplyTrigger(upTo=${msg.batch.upTo})`,
        desc:  `Entries up through index ${msg.batch.upTo} are committed and ready for the deterministic apply path. The raft node hands them off via a cap-1 channel to the applier goroutine, which then calls fsm.PrepareEntries(...). Each apply walks Cache → Processing → Pebble before acking back through the applier.`,
      };
    case "ApplyTriggerAck":
      return {
        title: `${at} — fsm.PrepareEntries returned`,
        desc:  `Synchronous response to the applier's call into the FSM. The cache mutations + Pebble batch have been built (or — in the animation — the cache phase has been kicked off). The applier now hands the prepared batch to its async committer; the FsmAck will follow once the pebble fsync + notifier broadcast complete.`,
      };
    case "PreparedBatch":
      return {
        title: `${at} — PreparedBatch(upTo=${msg.batch.upTo})`,
        desc:  `FSM finished PrepareEntries: cache mutations are committed in-memory, the pebble batch is built. The applier picks it up, ships it through its cap-1 committer channel to Pebble (synchronous batch.Commit), then fans out via the notifier once durability is confirmed.`,
      };
    case "FsmAck":
      return {
        title: `${at} — FsmAck(applied=${msg.batch.upTo})`,
        desc:  `${labelFor(msg.from)}'s FSM finished applying through index ${msg.batch.upTo}. On the leader this triggers ClientResp messages for every newly-applied tx (routed back via the reverse network chain), plus the 4-tier stager promotes the gestating slot to inflight so the next batch can start.`,
      };
    case "ClientResp":
      return {
        title: `${at} — ClientResp(tx#${msg.txId}, ok=${msg.ok})`,
        desc:  `End of the chain. The apply ack walks leader → Admission → Controller → gRPC → Client, arriving at the originating Client to correlate by txId. The client moves the tx from pending to done — the Inflight panel shrinks, History grows.`,
      };

    case "ReadTracker":
      return {
        title: `${at} — IndexTracker.Next()`,
        desc:  `Admission peeks the predicted next Raft index for tx#${msg.txId}. Non-mutating — multiple in-flight preloads can see the same value. The actual reserved index will be taken later via IncrementTracker once cache + pebble preload completes.`,
      };
    case "ReadTrackerResp":
      return {
        title: `${at} — predicted nextIndex=${msg.nextIndex}`,
        desc:  `Tracker returns the predicted Raft index for tx#${msg.txId}. Admission will now consult Cache (CheckCache) + Pebble (lazy-miss load) before reserving the actual index.`,
      };
    case "IncrementTracker":
      return {
        title: `${at} — IndexTracker.Increment(1)`,
        desc:  `Admission reserves an actual Raft index for tx#${msg.txId} by atomically bumping the tracker. Mirrors raft.Node.Propose's internal call under tracker.Lock — this guarantees each in-flight proposal gets a unique, monotonic index even when many run concurrently.`,
      };
    case "IncrementTrackerResp":
      return {
        title: `${at} — reserved index=${msg.nextIndex}`,
        desc:  `Tracker reserved index ${msg.nextIndex} for tx#${msg.txId}. Admission can now forward the Propose to the leader; once leader's flush() fires, that tx becomes part of the next AppendEntries batch.`,
      };

    case "ReadCache":
    case "ReadCacheResp":
      return {
        title: `${at} — preload cache check`,
        desc:  `Admission consults the in-memory cache for keys the preloader needs (account volumes, posting metadata). Any miss is filled by a follow-up ReadPebble. The full preload payload is what makes the FSM apply deterministic across nodes — every node sees identical inputs.`,
      };
    case "ReadPebble":
    case "ReadPebbleResp":
      return {
        title: `${at} — preload pebble lazy-load`,
        desc:  `Admission reads from Pebble (durable KV) for the keys the cache didn't already hold. This is the slow path of preload — Pebble I/O is fsync-bounded. The values fetched here will be mirrored into Cache by the FSM's MirrorPreload during apply.`,
      };

    case "ConsultCache":
      return {
        title: `${at} — MirrorPreload`,
        desc:  `FSM consults the cache to mirror the apply batch's preload payload into Gen0 (populating any keys the apply will touch). On the leader's pipeline this is the first apply phase, before numscript runs. Synchronous from the FSM's view.`,
      };
    case "CacheResp":
      return {
        title: `${at} — cache responded`,
        desc:  `Cache acked the consult. The FSM advances to the next apply phase (processing for preload, or completion for the commit phase).`,
      };
    case "ConsultProcessing":
      return {
        title: `${at} — applyOrder (numscript)`,
        desc:  `FSM hands the entry to the Processing node: numscript execution, posting validation, logId / txId allocation. The resulting mutations land in Gen0 next to the preloaded payload.`,
      };
    case "ProcessingResp":
      return {
        title: `${at} — processing complete`,
        desc:  `Numscript / validation done; the apply batch enters the commit phase — TWO parallel emits from the FSM: ConsultCache (audit trail of mirror writes) + WritePebble (durable batch.Commit). The FSM waits for both before acking the leader and emitting NotifyLogs.`,
      };
    case "WritePebble":
      return {
        title: `${at} — batch.Commit (NoSync)`,
        desc:  `FSM submits the apply batch to Pebble's writer queue. Capacity-1 send (same shape as the leader→fsm applier slot). The Pebble fsync completes durability — afterwards the entry's effects are persisted independent of Raft.`,
      };
    case "PebbleAck":
      return {
        title: `${at} — pebble persisted`,
        desc:  `Pebble has fsync'd the apply batch up through index ${msg.batch.upTo}. Combined with the parallel CacheResp from this commit phase, the FSM can now ack the leader and emit NotifyLogs to fan out to subscribers.`,
      };

    case "WriteWAL":
      return {
        title: `${at} — wal.Append (fsync)`,
        desc:  `${labelFor(msg.from)} synchronously persists new entries to its local WAL. This is the durability step that gates the AppendResp (followers) or matchIdx[self] update (leader). In real Raft, fsync latency directly bounds proposal throughput.`,
      };
    case "WALAck":
      return {
        title: `${at} — WAL fsync done`,
        desc:  `WAL fsync of ${labelFor(msg.from)} reported up through index ${msg.batch.upTo}. ${node === "nodeL" ? "Leader can now count itself toward quorum." : "Follower can now reply AppendResp."}`,
      };

    case "NotifyLogs":
      return {
        title: `${at} — NotifyLogsCommitted`,
        desc:  `Leader's FSM signals the notifier that entries up through ${msg.batch.upTo} are now committed. The notifier fans out to all subscribers — Index Builder, Event Sinks, Cold Storage, Sealer — each of which does its own thing on its own schedule.`,
      };

    case "Compact":
      return {
        title: `${at} — compaction kicked`,
        desc:  `User-triggered WAL compaction. The compactor reads Pebble's highestPersistedIdx (= safe floor), then asks the leader's WAL to truncate everything below it. Frees disk space; doesn't affect commit / apply semantics.`,
      };
    case "ReadPersisted":
      return {
        title: `${at} — read Pebble.highestPersistedIdx`,
        desc:  `Compactor queries Pebble for the safe truncation floor. Anything Pebble has durably persisted can be dropped from the WAL since the FSM can replay from Pebble alone.`,
      };
    case "ReadPersistedResp":
      return {
        title: `${at} — safe floor = ${msg.upTo}`,
        desc:  `Pebble reports that index ${msg.upTo} is durably persisted. Compactor will now issue TruncateWAL up to this index — anything ≤ it is replayable from Pebble.`,
      };
    case "TruncateWAL":
      return {
        title: `${at} — wal.Truncate(${msg.upTo})`,
        desc:  `Compactor asks ${labelFor(msg.from)} to drop WAL entries through index ${msg.upTo}. Atomic from the WAL's perspective — the bounds advance, the disk space is reclaimable.`,
      };
    case "TruncateWALAck":
      return {
        title: `${at} — WAL truncated, firstIdx=${msg.firstIdx}`,
        desc:  `WAL's first-stored index is now ${msg.firstIdx}; everything below was dropped. The Pebble / WAL panel's bounds reflect the new floor.`,
      };
  }
}

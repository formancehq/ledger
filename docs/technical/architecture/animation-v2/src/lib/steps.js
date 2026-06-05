import { app, cache, pebble } from "./state.svelte.js";
import { anim, makeDot } from "./anim.js";
import { COLORS } from "./colors.js";
import { generationFor, txKeys, txDefaults } from "./cache.js";
import { raftLock, fsmLock } from "./gates.js";
import {
  eventOrder, eventCheckCache, eventPebbleLoads, eventProposal,
  eventCommitted, eventPreloadResult, eventApplyResult, eventResponse,
} from "./payloadBuilders.js";

// Cache rotation is per-entry (machine.go CheckRotationNeeded(entry.Index)) —
// it MUST use the index of the entry being applied, not the live leaderIdx.
// In pipelined mode the leader may already have proposed several later
// entries while this entry is still being applied; using app.raft.leaderIdx
// would trigger a spurious rotation here and silently empty Gen0 right under
// ⑤c's feet (the bndEarly snapshot would come back null → "?").
function maybeRotateCache(tx) {
  const targetGen = generationFor(tx.commitIndex, cache.threshold);
  if (targetGen > cache.currentGen) cache.rotate(tx.commitIndex);
}

// Each step optionally emits an `event(tx) → { title, html } | null` AFTER its
// action runs. The runtime appends this entry to tx.timeline, which the right
// panel renders chronologically. Steps that don't introduce new information
// (e.g. ①b/①c just relay the order, ②/③ are pure visualisation) skip `event`.
export const steps = [
  // ─── ① Ingress → preload → propose ────────────────────────────────────
  {
    title: "①a Client → gRPC Server",
    desc:  "The order arrives as a unary gRPC call on BucketService (port 8888). The server validates the proto payload — account names (with @ prefix), monetary amounts in fixed-size Uint256 wire format — then hands the command to the application layer.",
    color: COLORS.grpc,
    event: eventOrder,
    action: (tx) => anim(makeDot(tx.color), "e-client-grpc", 700),
  },
  {
    title: "①b gRPC Server → Routed Controller",
    desc:  "The Routed Controller checks Raft leadership. On a follower it transparently forwards the request to the current leader via the service connection pool. On the leader it stays local and proceeds with the default Controller.",
    color: COLORS.grpc,
    action: (tx) => anim(makeDot(tx.color), "e-grpc-ctrl", 280),
  },
  {
    title: "①c Routed Controller → Admission",
    desc:  "The default Controller calls Admission.Admit(). Admission scans the command and enumerates every key needed for the apply — for a simple send, that's the two account volumes for the involved asset plus the ledger boundary (used to allocate the next log id and transaction id). Those keys are what the preload payload will carry.",
    color: COLORS.grpc,
    action: (tx) => anim(makeDot(tx.color), "e-ctrl-adm", 280),
  },
  {
    title: "①d Admission ↔ Cache (CheckCache)",
    desc:  "Admission classifies each needed key against the dual-generation cache without taking a lock. Three outcomes: CacheGuaranteed (already in Gen0 — nothing to do), CacheNeedsTouch (in Gen1 — schedule a Gen1 → Gen0 promotion to survive rotation), CacheMiss (absent — must be fetched from Pebble).",
    color: COLORS.apply,
    event: (tx) => eventCheckCache(tx, cache, app.raft),
    action: (tx) => anim(makeDot(tx.color, 5),
      [{ id: "e-consult-cache" }, { id: "e-consult-cache", reverse: true }], 1000),
  },
  {
    title: "①e Admission ↔ Pebble (lazy miss load)",
    desc:  "Only CacheMiss keys hit Pebble. Reads happen in parallel (parallelism=16) and feed the preload payload. Volume keys for non-existent accounts skip the read entirely — a zero-value is injected directly. If CheckCache already covered every key, this step is bypassed entirely — that's the whole point of the cache.",
    color: COLORS.apply,
    skipIf: (tx) => tx.proposalPlan && tx.proposalPlan.miss.length === 0,
    event:  (tx) => eventPebbleLoads(tx, cache, app.raft),
    action: (tx) => anim(makeDot(tx.color, 5),
      [{ id: "e-consult-pebble" }, { id: "e-consult-pebble", reverse: true }], 1100),
  },
  {
    title: "①f Admission → Raft leader proposes",
    desc:  "Admit() does NOT propose one tx at a time — if the controller batched N requests in this call (admission.go:340 NewCommand(orders...)), all N orders collapse into a single Proposal here, hence one Raft entry per Admit() call. The command — now carrying its preload payload (preloads + Gen1→Gen0 touches) — is marshaled OUTSIDE the lock, then AcquireProposalGuard takes the tracker lock, revalidates gen(nextIndex), and conditionally rebuilds preloads if a rotation happened since BuildPreloads. Propose() runs under the same lock; the guard is released only after the entry is in the Raft pipeline. The caller then waits on fsmFuture until the FSM applies the entry.",
    color: COLORS.grpc,
    event:  (tx) => eventProposal(tx, cache, app.raft),
    action: (tx) => anim(makeDot(tx.color), "e-adm-leader", 700),
  },
  // ─── ② Local WAL + replication (parallel by design) ──────────────────
  {
    title: "② Leader: WAL append + AppendEntries",
    desc:  "On the leader's etcd/raft Ready loop tick, wal.Append(HardState, Entries) writes the new entry to the Raft log (Pebble-backed, fsync for durability) AND MsgApp messages are emitted to every follower over the Raft gRPC transport (port 7777). Both happen locally on the leader, no quorum gate. The propose-side raftLock is released as soon as the leader's own WAL write completes — the next proposal can enter ①f while AppendEntries to followers and the fsync acks pipeline behind it.",
    color: COLORS.raft,
    action: async (tx) => {
      // In-memory log append: etcd/raft accepts the proposal into raftLog
      // and assigns its index synchronously, before the Ready-loop tick that
      // will WAL-write and replicate it. Under raftLock so indices are
      // strictly sequential across concurrent proposals.
      app.raft.leaderIdx += 1;
      tx.commitTerm  = app.raft.term;
      tx.commitIndex = app.raft.leaderIdx;

      // All three dots travel concurrently — the WAL fsync and AppendEntries
      // dispatch live in the same Ready-loop tick on the real server.
      const walDone = anim(makeDot(tx.color, 5), "e-wal-leader", 320);
      const f1Done  = anim(makeDot(tx.color, 5), "e-leader-f1",  500);
      const f2Done  = anim(makeDot(tx.color, 5), "e-leader-f2",  900);

      // The leader is free to accept the NEXT proposal the instant its own
      // WAL batch is durable — etcd/raft serializes proposals only across the
      // wal.Append call, not across the full Ready cycle. Releasing here is
      // what gives the raft layer real pipelining (next tx enters ②, the
      // followers' fsync acks for this batch arrive concurrently as ③/④).
      await walDone;
      raftLock.release();

      // Step's promise stays pending until the AppendEntries dots actually
      // reach the follower boxes, so the visual stays coherent with the
      // rest-dot placement at the followers.
      await Promise.all([f1Done, f2Done]);
      // Note: cache rotation does NOT happen here. The real FSM
      // (internal/infra/state/machine.go CheckRotationNeeded around line 535)
      // runs the rotation inside the apply batch, after the entry is committed
      // and handed off to the FSM — we mirror that at ⑤a, not at the propose-side.
    },
  },
  // ─── ③ Followers fsync their own WAL ─────────────────────────────────
  {
    title: "③ Followers: WAL fsync",
    desc:  "On each follower's next Ready loop tick, wal.Append(HardState, Entries) persists the appended entries with fsync. Durability is per-node and independent. The WAL is a Raft side-persistence: invariant #3 says the FSM never reads from it on the hot path.",
    color: COLORS.raft,
    action: async (tx) => {
      await Promise.all([
        anim(makeDot(tx.color, 5), "e-wal-f1", 320),
        anim(makeDot(tx.color, 5), "e-wal-f2", 320),
      ]);
      app.raft.f1Match = app.raft.leaderIdx;
      app.raft.f2Match = app.raft.leaderIdx;
    },
  },
  // ─── ④ ACKs → quorum → commit ────────────────────────────────────────
  {
    title: "④ ACK → quorum → commit",
    desc:  "Each follower returns MsgAppResp once its WAL fsync completes. As soon as a quorum (majority including the leader's own write) has acked, etcd/raft on the leader advances the commit index. The new commit index is piggybacked onto the next AppendEntries to all followers.",
    color: COLORS.raft,
    action: (tx) => Promise.all([
      anim(makeDot(tx.color, 5), [{ id: "e-leader-f1", reverse: true }], 700),
      anim(makeDot(tx.color, 5), [{ id: "e-leader-f2", reverse: true }], 800),
    ]),
  },
  // ─── ⑤ FSM apply: arrival → preload materialization → fan-out ──────
  {
    title: "⑤a Committed entries → FSM",
    desc:  "Each node's Ready loop hands the now-committed entries to the FSM applier on the same node. Invariant #2: the FSM is deterministic — same input → identical output on every node, no randomness, no time-dependent logic, no node-local state. If the entry's index crosses a generation boundary, the FSM rotates the cache here (Gen0 → Gen1, new empty Gen0) BEFORE running MirrorPreload, atomically with the apply batch.",
    color: COLORS.apply,
    event: (tx) => eventCommitted(tx, cache, app.raft),
    action: async (tx) => {
      await Promise.all([
        anim(makeDot(tx.color, 5), "e-leader-fsm", 600),
        anim(makeDot(tx.color, 5), "e-f1-fsm",     700),
        anim(makeDot(tx.color, 5), "e-f2-fsm",     750),
      ]);
      // The FSM's first job inside the apply batch is CheckRotationNeeded
      // (machine.go:535). If the entry's index crosses a generation boundary,
      // rotate now — this is what makes rotation deterministic across nodes.
      maybeRotateCache(tx);
    },
  },
  {
    title: "⑤b Preload payload → Cache (FSM.Preload)",
    desc:  "Before the actual apply runs, the FSM pours the preload payload into Gen0 via cacheSnapshotter.MirrorPreload (writes) + MirrorTouch (Gen1 → Gen0 promotions). This is the contract that lets invariant #3 hold: every key the apply will read is now guaranteed to be in cache, so apply never needs Pebble.",
    color: COLORS.apply,
    event: (tx) => eventPreloadResult(tx, cache, app.raft),
    action: async (tx) => {
      await anim(makeDot(tx.color, 5), "e-fsm-cache", 380);
      const defs = txDefaults(tx);
      const plan = tx.proposalPlan || { miss: [], needsTouch: [] };
      // Only iterate keys that the proposal payload actually carried — i.e.
      // those admission classified as MISS (preloaded) or NEEDS_TOUCH (Gen1
      // promotion). GUARANTEED keys aren't in the payload and don't need any
      // FSM.Preload work. Each iteration records what MirrorPreload /
      // MirrorTouch decided, so the lifecycle panel can show stale-preload
      // rejection clearly.
      const decisions = [];
      for (const key of plan.miss) {
        const g0 = cache.gen0.get(key);
        if (g0 && !g0.deleted) {
          decisions.push({ key, op: "rejected", preload: defs[key], kept: g0.value });
          continue;
        }
        decisions.push({ key, op: "written", value: defs[key] });
        cache.put(key, defs[key]);
      }
      for (const key of plan.needsTouch) {
        const g0 = cache.gen0.get(key);
        if (g0 && !g0.deleted) {
          // Another tx already promoted (or wrote) this key — Touch is a no-op.
          decisions.push({ key, op: "rejected", preload: "Touch", kept: g0.value });
          continue;
        }
        const g1 = cache.gen1.get(key);
        if (g1 && !g1.deleted) {
          decisions.push({ key, op: "touched", value: g1.value });
          cache.touch(key);
          continue;
        }
        // Defensive: Gen1 disappeared between admission and apply — shouldn't
        // happen because rotations don't drop until next bump, but treat as
        // a fresh write to keep the apply invariant intact.
        decisions.push({ key, op: "written", value: defs[key] });
        cache.put(key, defs[key]);
      }
      // Bare-ref write so eventPreloadResult (called with the same ref a few
      // lines later in runStepsLoop) sees the decisions back.
      tx.preloadDecisions = decisions;
    },
  },
  {
    title: "⑤c FSM apply → single pebble.Batch (NoSync, pipelined)",
    desc:  "The FSM processes the command in a single pebble.Batch (cache writes via 0xFF mirror, log + audit, attribute deltas, optional rotation meta). batch.Commit(NoSync) returns quickly — durability comes from the upstream Raft WAL, not a local fsync. As soon as the in-memory side is done, the FSM releases its lock and starts applying the next entry: that's the apply pipeline. lastPersistedIndex (an in-memory atomic) bumps once batch.Commit returns.",
    color: COLORS.apply,
    // event runs AFTER the action so the IDs in the timeline entry reflect the
    // freshly-allocated values from the boundary bump.
    event:  (tx) => eventApplyResult(tx, cache),
    action: async (tx) => {
      // Snapshot the IDs the FSM will allocate BEFORE any anim. Gen0 already
      // reflects everything ⑤b did (preload mirror + touches) and fsmLock is
      // still held — no other tx can touch the cache here. Doing this early
      // makes the snapshot robust to any later timing weirdness during the
      // pipelined awaits.
      const sKey = `volumes(${tx.source}.${tx.asset})`;
      const dKey = `volumes(${tx.destination}.${tx.asset})`;
      const bKey = `boundary(${tx.ledger})`;
      const bndEarly = cache.gen0.get(bKey);
      if (bndEarly) {
        tx.appliedLogId        = bndEarly.value.nextLogId;
        tx.appliedTxId         = bndEarly.value.nextTxId;
        tx.appliedPostingCount = bndEarly.value.postingCount + 1;
      }

      // Both anims fire in parallel — cache+log+audit all live in the same
      // pebble.Batch. The cache anim is short (the in-memory side completes
      // quickly), the Pebble anim is longer (durability lag visualised).
      const cacheDone  = anim(makeDot(tx.color, 5), "e-fsm-cache",  380);
      const pebbleDone = anim(makeDot(tx.color, 5), "e-fsm-pebble", 720);

      // 1. Wait for the in-memory side (cache writes land in Gen0).
      await cacheDone;
      const src = cache.gen0.get(sKey);
      const dst = cache.gen0.get(dKey);
      const bnd = cache.gen0.get(bKey);
      if (src) cache.put(sKey, { in: src.value.in,            out: src.value.out + tx.amount });
      if (dst) cache.put(dKey, { in: dst.value.in + tx.amount, out: dst.value.out });
      if (bnd) cache.put(bKey, {
        nextLogId:    bnd.value.nextLogId + 1,
        nextTxId:     bnd.value.nextTxId  + 1,
        postingCount: bnd.value.postingCount + 1,
      });
      // 2. Release fsmLock NOW — the next entry's apply can pipeline while
      //    the Pebble durability tail of this entry is still in flight.
      //    The loop's "if (stepIndex === 12) fsmLock.release()" guard fires
      //    later but is idempotent (drain on empty queue is a no-op).
      fsmLock.release();
      // 3. Wait for the Pebble durability tail (the batch.Commit(NoSync)
      //    completing in Pebble's background WAL/memtable path). Only AFTER
      //    this do we publish the pebble.set values and bump
      //    lastPersistedIndex — those are observable from outside as
      //    "the entry is durable".
      await pebbleDone;
      if (cache.gen0.get(sKey)) pebble.set(sKey, cache.gen0.get(sKey).value);
      if (cache.gen0.get(dKey)) pebble.set(dKey, cache.gen0.get(dKey).value);
      if (cache.gen0.get(bKey)) pebble.set(bKey, cache.gen0.get(bKey).value);
      // Use THIS tx's commitIndex, not the live leaderIdx. In the pipelined
      // model later txs may already have bumped leaderIdx at step ② while
      // tx#N is still awaiting its Pebble tail — reading the live value
      // makes lastPersistedIndex leap to a number whose dot has not even
      // touched the Pebble box yet. Per-tx commitIndex advances the label
      // monotonically, in sync with the dot arrival.
      if (tx.commitIndex > app.raft.leaderApplied) {
        app.raft.leaderApplied = tx.commitIndex;
        app.raft.f1Applied     = tx.commitIndex;
        app.raft.f2Applied     = tx.commitIndex;
      }
    },
  },
  // ─── ⑥ Response + notifier broadcasts (signal.FanOut) ──────────────
  {
    title: "⑥ Response to client · notifier broadcasts (signal.FanOut)",
    desc:  "Two things happen after FSM apply commits. (1) The leader's controller resolves the pending future with the log entry and returns ApplyResponse to the client via gRPC. (2) machine.go calls notifier.NotifyLogsCommitted(lastSeq) ONCE — signal.FanOut hands that single notification to every subscriber (Index Builder, Event Sinks, Cache Mirror, Cold Storage dispatch, Sealer). Each subscriber wakes on its own buffered(1) channel + 100ms ticker fallback and tails the log in batches (Index Builder ~1000, Event Sinks ~64). The Archiver branch is a bit different — the FSM pushes ArchiveRequests directly on archiveRequestCh rather than tailing the log. ALL of this is off the gRPC critical path.",
    color: COLORS.resp,
    action: async (tx) => {
      // First : FSM → notifier (the trigger).
      await anim(makeDot(tx.color, 5), "e-fsm-notifier", 280);
      // Then : response back to client AND notifier fan-out to 5 subscribers,
      // all in parallel — the response and the worker broadcasts are truly
      // concurrent in the real system.
      await Promise.all([
        anim(makeDot(tx.color), [
          { id: "e-leader-adm" },
          { id: "e-ctrl-adm",    reverse: true },
          { id: "e-grpc-ctrl",   reverse: true },
          { id: "e-client-grpc", reverse: true },
        ], 1500),
        anim(makeDot(tx.color, 4), "e-notifier-w-index",    280),
        anim(makeDot(tx.color, 4), "e-notifier-w-sinks",    320),
        anim(makeDot(tx.color, 4), "e-notifier-w-archiver", 360),
        anim(makeDot(tx.color, 4), "e-notifier-w-sealer",   400),
      ]);
    },
    event: (tx) => eventResponse(tx, cache, app.raft),
  },
];

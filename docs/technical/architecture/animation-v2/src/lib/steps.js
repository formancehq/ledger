import { app, cache, pebble, proxyOf } from "./state.svelte.js";
import { travel, anim, makeDot } from "./anim.js";
import { COLORS } from "./colors.js";
import { generationFor, txKeys, txDefaults } from "./cache.js";
import { raftLock, fsmLock, channelLock, joinReadyTick } from "./gates.js";
import { STAGGER_MS, DUR_QUICK, DUR_HOP, DUR_FORWARD, DUR_RESPONSE } from "./geometry.js";
import {
  eventOrder, eventCheckCache, eventPebbleLoads, eventProposal,
  eventCommitted, eventPreloadResult, eventApplyResult, eventResponse,
  eventGuardRebuild, computePlan,
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

// Returns every tx whose order is bundled into THIS Raft entry. For an
// unbatched tx that's just [tx]; for a batched lead it's [tx, ...members].
// Members never call this directly because their loop short-circuits at
// raftLock.acquire and they never run ②/⑤b/⑤c on their own.
function batchTxs(tx) {
  return tx.batch ? [tx, ...tx.batch.members] : [tx];
}

// ─── ⑤b apply-loop building blocks ────────────────────────────────────
// machine.go's applyProposal decomposes into these four operations, run
// per-entry. The per-entry loop in step ⑤b just chains them; the heavy
// lifting lives here so the step body stays readable.

// AcquireProposalGuard equivalent — re-validate the plan against THIS
// entry's actual commitIndex gen. Real Go runs this at admission's
// AcquireProposalGuard under tracker.Lock right before Propose; for
// unbatched txs and leads that runs at ①f (see runCycle.js). Batched
// members had their plan frozen at their own ①d and never re-checked
// (their actual commitIndex is only assigned by the lead's step ②), so
// the rebuild lands here at apply time — equivalent semantics, just
// shifted to where the real index is finally known.
function maybeRebuildPlan(t) {
  if (!t.proposalPlan) return;
  const planGen   = generationFor(t.proposalPlan.futureIdx, cache.threshold);
  const actualGen = generationFor(t.commitIndex,           cache.threshold);
  if (planGen === actualGen) return;
  const stale = t.proposalPlan;
  const fresh = { guaranteed: [], needsTouch: [], miss: [], futureIdx: t.commitIndex };
  for (const key of txKeys(t)) {
    const g0 = cache.gen0.get(key);
    if (g0 && !g0.deleted) { fresh.guaranteed.push(key); continue; }
    const g1 = cache.gen1.get(key);
    if (g1 && !g1.deleted) { fresh.needsTouch.push(key); continue; }
    fresh.miss.push(key);
  }
  t.proposalPlan      = fresh;
  t.proposalPlanStale = stale;
  // Drop the rebuild warning on THIS entry's lifecycle (mirror of the one
  // the lead emits at ①f). For batched members this is the first time
  // their own timeline acknowledges the rebuild.
  const proxy = proxyOf(t);
  if (!proxy) return;
  const entry = eventGuardRebuild(t, stale, fresh);
  proxy.timeline = [...proxy.timeline, {
    stepIndex: 11,
    color:     COLORS.raft,
    ...entry,
  }];
}

// MirrorPreload + MirrorTouch — plan-driven. For misses we read from
// Pebble (the source of truth), not txDefaults — that's how the real
// preload payload is built: admission loads from Pebble at
// extractPreloadNeeds. Appends one decision per processed key so the
// ⑤b lifecycle event can show what happened (rejected / written / touched).
function applyMirrorPreload(t, decisions) {
  const defs = txDefaults(t);
  const plan = t.proposalPlan || { miss: [], needsTouch: [] };
  const preloadValue = (key) => {
    const fromPebble = pebble.get(key);
    return fromPebble ? fromPebble.value : defs[key];
  };
  for (const key of plan.miss) {
    const g0 = cache.gen0.get(key);
    if (g0 && !g0.deleted) {
      decisions.push({ key, op: "rejected", preload: preloadValue(key), kept: g0.value });
      continue;
    }
    const v = preloadValue(key);
    decisions.push({ key, op: "written", value: v });
    cache.put(key, v);
  }
  for (const key of plan.needsTouch) {
    const g0 = cache.gen0.get(key);
    if (g0 && !g0.deleted) {
      decisions.push({ key, op: "rejected", preload: "Touch", kept: g0.value });
      continue;
    }
    const g1 = cache.gen1.get(key);
    if (g1 && !g1.deleted) {
      decisions.push({ key, op: "touched", value: g1.value });
      cache.touch(key);
      continue;
    }
    const v = preloadValue(key);
    decisions.push({ key, op: "written", value: v });
    cache.put(key, v);
  }
}

// applyProposal business logic for a single order — allocate ids from the
// live boundary, mutate Gen0 volumes + boundary in place. Snapshotting
// from Gen0 here (not a pre-batch read) is what makes the boundary
// carried into Gen1 by the next rotation reflect entry t's apply, not
// entry t-N's.
function applyOrder(t) {
  const sKey = `volumes(${t.source}.${t.asset})`;
  const dKey = `volumes(${t.destination}.${t.asset})`;
  const bKey = `boundary(${t.ledger})`;
  const bnd = cache.gen0.get(bKey);
  if (bnd) {
    t.appliedLogId        = bnd.value.nextLogId;
    t.appliedTxId         = bnd.value.nextTxId;
    t.appliedPostingCount = bnd.value.postingCount + 1;
  }
  const src = cache.gen0.get(sKey);
  const dst = cache.gen0.get(dKey);
  if (src) cache.put(sKey, { in: src.value.in,            out: src.value.out + t.amount });
  if (dst) cache.put(dKey, { in: dst.value.in + t.amount, out: dst.value.out });
  if (bnd) cache.put(bKey, {
    nextLogId:    bnd.value.nextLogId + 1,
    nextTxId:     bnd.value.nextTxId  + 1,
    postingCount: bnd.value.postingCount + 1,
  });
}

// Publish a single order's mutated cache cells to Pebble (one pebble.Batch
// contains the log/audit entries + 0xFF cache mirror writes for every
// order in the batch). Read from Gen0 at write time so what lands in
// Pebble matches what's in the in-memory cache.
function publishOrderToPebble(t) {
  const sKey = `volumes(${t.source}.${t.asset})`;
  const dKey = `volumes(${t.destination}.${t.asset})`;
  const bKey = `boundary(${t.ledger})`;
  if (cache.gen0.get(sKey)) pebble.set(sKey, cache.gen0.get(sKey).value);
  if (cache.gen0.get(dKey)) pebble.set(dKey, cache.gen0.get(dKey).value);
  if (cache.gen0.get(bKey)) pebble.set(bKey, cache.gen0.get(bKey).value);
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
    action: (tx) => travel(tx, "e-client-grpc", DUR_FORWARD, 6),
  },
  {
    title: "①b gRPC Server → Routed Controller",
    desc:  "The Routed Controller checks Raft leadership. On a follower it transparently forwards the request to the current leader via the service connection pool. On the leader it stays local and proceeds with the default Controller.",
    color: COLORS.grpc,
    action: (tx) => travel(tx, "e-grpc-ctrl", DUR_QUICK, 6),
  },
  {
    title: "①c Routed Controller → Admission",
    desc:  "The default Controller calls Admission.Admit(). Admission scans the command and enumerates every key needed for the apply — for a simple send, that's the two account volumes for the involved asset plus the ledger boundary (used to allocate the next log id and transaction id). Those keys are what the preload payload will carry.",
    color: COLORS.grpc,
    action: (tx) => travel(tx, "e-ctrl-adm", DUR_QUICK, 6),
  },
  {
    title: "①d Admission ↔ Cache (CheckCache)",
    desc:  "Admission classifies each needed key against the dual-generation cache without taking a lock. Three outcomes: CacheGuaranteed (already in Gen0 — nothing to do), CacheNeedsTouch (in Gen1 — schedule a Gen1 → Gen0 promotion to survive rotation), CacheMiss (absent — must be fetched from Pebble).",
    color: COLORS.apply,
    event: (tx) => eventCheckCache(tx, cache, app.raft),
    action: (tx) => travel(tx, [{ id: "e-consult-cache" }, { id: "e-consult-cache", reverse: true }], 1000, 5),
  },
  {
    title: "①e Admission ↔ Pebble (lazy miss load)",
    desc:  "Only CacheMiss keys hit Pebble. Reads happen in parallel (parallelism=16) and feed the preload payload. Volume keys for non-existent accounts skip the read entirely — a zero-value is injected directly. If CheckCache already covered every key, this step is bypassed entirely — that's the whole point of the cache.",
    color: COLORS.apply,
    skipIf: (tx) => tx.proposalPlan && tx.proposalPlan.miss.length === 0,
    event:  (tx) => eventPebbleLoads(tx, cache, app.raft),
    action: (tx) => travel(tx, [{ id: "e-consult-pebble" }, { id: "e-consult-pebble", reverse: true }], 1100, 5),
  },
  {
    title: "①f Admission → Raft leader proposes",
    desc:  "Admit() does NOT propose one tx at a time — if the controller batched N requests in this call (admission.go:340 NewCommand(orders...)), all N orders collapse into a single Proposal here, hence one Raft entry per Admit() call. The command — now carrying its preload payload (preloads + Gen1→Gen0 touches) — is marshaled OUTSIDE the lock, then AcquireProposalGuard takes the tracker lock, revalidates gen(nextIndex), and conditionally rebuilds preloads if a rotation happened since BuildPreloads. Propose() runs under the same lock; the guard is released only after the entry is in the Raft pipeline. The caller then waits on fsmFuture until the FSM applies the entry.",
    color: COLORS.grpc,
    // AcquireProposalGuard rebuild check — preloader.go:209-236. Real Go
    // runs this under tracker.Lock right before Propose, but the lock is
    // uncontended at the read side (each goroutine recomputes its own plan
    // from a snapshot of the cache state). We mirror that lock-free here.
    // Admission is parallel on the server, each request flows through ①f
    // independently — no raftLock involved.
    beforeAction: (tx) => {
      if (!tx.proposalPlan) return;
      const currentFutureIdx = app.raft.leaderIdx + 1;
      const planGen    = generationFor(tx.proposalPlan.futureIdx, cache.threshold);
      const currentGen = generationFor(currentFutureIdx,          cache.threshold);
      if (planGen === currentGen) return;
      const stale = tx.proposalPlan;
      tx.proposalPlanStale = stale;
      tx.proposalPlan      = null;
      const fresh = computePlan(tx, cache, app.raft);
      const entry = eventGuardRebuild(tx, stale, fresh);
      const proxy = proxyOf(tx);
      if (proxy) {
        proxy.timeline = [...proxy.timeline,
          { stepIndex: tx.stepIndex, color: COLORS.raft, ...entry }];
      }
    },
    event:  (tx) => eventProposal(tx, cache, app.raft),
    action: (tx) => travel(tx, "e-adm-leader", DUR_FORWARD, 6),
  },
  // ─── ② Local WAL + replication (parallel by design) ──────────────────
  {
    title: "② Raft Node Ready tick: WAL append + AppendEntries",
    desc:  "Each tx that finished ①f independently arrived at the raft.Node's pending-proposals queue. The next Ready-loop tick picks up every queued proposal and processes them as ONE batch: wal.Append([entries…]) writes them all in one shot to the dedicated Raft WAL (independent of the FSM's Pebble store — its own files, its own fsync) AND a batched MsgApp is emitted to every follower over the Raft gRPC transport (port 7777). raftLock is held for the entire duration — the next batch's lead waits until WAL fsync AND AppendEntries dispatch are both done, so we never see two batches' dots travelling to the followers simultaneously.",
    color: COLORS.raft,
    acquires: raftLock,
    releases: raftLock,
    // Ready tick gate — mirror of raft.Node's pending-proposals queue. Each
    // tx arriving here after its own ①f waits for the next tick to fire.
    // The tick drains EVERYONE waiting at once and packs them into one
    // batched Raft entry; the first arrival becomes the lead, others are
    // batched members. Members short-circuit to ⑥b (jumpTo 12 lands on
    // ⑥a's skipIf → 13).
    beforeAction: async (tx) => {
      await joinReadyTick(tx);
      if (tx.batch && !tx.batchLead) {
        await tx.batch.applied;
        return { jumpTo: 12 };
      }
    },
    action: async (tx) => {
      // In-memory log append: etcd/raft accepts each proposal into raftLog
      // and assigns its own index, sequentially. Even when admission batches
      // requests, EVERY tx still gets its own Raft entry / commitIndex —
      // the batching happens at the WAL write and the FSM apply (one
      // pebble.Batch over N entries), not at the entry assignment.
      // Iterate batchTxs so leaderIdx bumps N times, once per tx.
      const allTxs = batchTxs(tx);
      for (const t of allTxs) {
        app.raft.leaderIdx += 1;
        t.commitTerm  = app.raft.term;
        t.commitIndex = app.raft.leaderIdx;
      }
      // WAL last index advances together with leaderIdx — wal.Append([entries…])
      // persists every entry produced by this Ready tick before AppendEntries
      // returns. firstIndex stays at 1 until truncation lands.
      app.wal.leaderLast = app.raft.leaderIdx;

      // All three dots travel concurrently — the WAL fsync and AppendEntries
      // dispatch live in the same Ready-loop tick on the real server. The
      // step's promise stays pending until all three land so the runner
      // doesn't free raftLock before the followers actually receive the
      // entries.
      await Promise.all([
        travel(tx, "e-wal-leader", 320, 5),
        travel(tx, "e-leader-f1", 500, 5),
        travel(tx, "e-leader-f2", 900, 5),
      ]);
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
        travel(tx, "e-wal-f1", 320, 5),
        travel(tx, "e-wal-f2", 320, 5),
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
      travel(tx, [{ id: "e-leader-f1", reverse: true }], 700, 5),
      travel(tx, [{ id: "e-leader-f2", reverse: true }], 800, 5),
    ]),
  },
  // ─── ⑤ FSM apply: arrival → preload materialization → fan-out ──────
  {
    title: "⑤a Committed entries → FSM",
    desc:  "Each node's Ready loop hands the now-committed entries to the FSM applier on the same node. Invariant #2: the FSM is deterministic — same input → identical output on every node, no randomness, no time-dependent logic, no node-local state. If the entry's index crosses a generation boundary, the FSM rotates the cache here (Gen0 → Gen1, new empty Gen0) BEFORE running MirrorPreload, atomically with the apply batch.",
    color: COLORS.apply,
    // Channel-aware submit. When the applier is idle (fsmLock free), the
    // lead's dot flies straight through to the FSM in one shot — no stop
    // at the channel slot. When the applier is busy, the dot lands at the
    // channel slot (midpoint), waits until the applier drains the previous
    // batch, then continues into the FSM. Mirrors a make(chan applyWork, 1)
    // submit: free slot → instant; full → block at submit.
    beforeAction: async (tx) => {
      // Submit to the leader→applier channel (capacity 1). If the slot is
      // taken by another batch, queue here — onBlock fires a red-pulse
      // blockedDot at the leader's right edge until the slot frees. Once
      // we own the slot, only animate to the midpoint if the applier is
      // ALSO busy (something to wait for); otherwise the applier will
      // drain us immediately in action() and the dot stays at the leader.
      await channelLock.acquire(tx);
      tx._heldChannelSlot = true;
      if (!fsmLock.busy) return;
      const dot = makeDot(tx.color, 5, tx.id);
      await anim(dot, "e-leader-fsm-1", 300);
      tx._channelDot = dot;
    },
    event: (tx) => eventCommitted(tx, cache, app.raft),
    action: async (tx) => {
      // Wait for the applier. We're already in the channel slot; this is
      // the applier reading from chan. When it acquires, release the slot
      // — the next batch parked at the leader's edge can now enter the
      // channel.
      await fsmLock.acquire(tx);
      tx._heldFsmLock = true;
      if (tx._heldChannelSlot) {
        channelLock.release();
        tx._heldChannelSlot = false;
      }
      let leaderLeg;
      if (tx._channelDot) {
        // Move the parked midpoint dot forward to the FSM — same element,
        // no extra circle, no orphan left at the slot.
        leaderLeg = anim(tx._channelDot, "e-leader-fsm-2", 300);
        tx._channelDot = null;
      } else {
        leaderLeg = travel(tx, ["e-leader-fsm-1", "e-leader-fsm-2"], 600, 5);
      }
      // Followers each have their own applier + chan, but we don't simulate
      // their busy state — treat them as always idle so their dots fly all
      // the way through to their FSM boxes. Matches the leader's idle path.
      await Promise.all([
        leaderLeg,
        travel(tx, ["e-f1-fsm-1", "e-f1-fsm-2"], 700, 5),
        travel(tx, ["e-f2-fsm-1", "e-f2-fsm-2"], 750, 5),
      ]);
      // No rotation here — CheckRotationNeeded is interleaved with the
      // per-entry MirrorPreload loop at ⑤b. Running all rotations upfront
      // on a multi-entry batch cascades (each subsequent rotation moves an
      // already-empty Gen0 to Gen1), and Gen1 ends up wiped instead of
      // holding the last pre-batch snapshot.
    },
  },
  {
    title: "⑤b FSM apply loop — per entry: rotate → preload → mutations",
    desc:  "machine.go walks the committed entries of this Ready tick one by one. For each: CheckRotationNeeded(entry.Index) fires first (rotates Gen0 → Gen1 atomically if a generation boundary is crossed), then applyProposal runs MirrorPreload + MirrorTouch (populating Gen0 with the preload payload), then the business logic (numscript / posting validation) runs and the resulting volumes / boundary / log mutations land in Gen0. Crucially mutations happen BETWEEN rotations within the batch — that's why Gen1, after the apply, holds the cumulative state of the previous generation, not a pre-batch snapshot.",
    color: COLORS.apply,
    event: (tx) => eventPreloadResult(tx, cache, app.raft),
    action: async (tx) => {
      // ⑤a already drove the leader's dot all the way into the FSM
      // (either straight through if the applier was idle, or via the
      // channel-slot park if it was busy). ⑤b just runs the per-entry
      // apply loop and animates the FSM↔Cache hop.
      await travel(tx, "e-fsm-cache", DUR_HOP, 5);
      // Per-entry pass — identical structure to machine.go's apply loop
      // (one fsm.Preload call per proposal, see machine.go:1100). Each
      // proposal == 1 tx in our model, so each tx in the batch gets its
      // OWN preload decisions stored on itself and its own ⑤b event in
      // its own lifecycle timeline. Mutations of entry N are visible to
      // the rotation check of entry N+1.
      for (const t of batchTxs(tx)) {
        maybeRotateCache(t);                       // 1. CheckRotationNeeded(entry.Index)
        maybeRebuildPlan(t);                       // 2. AcquireProposalGuard (per-entry)
        const entryDecisions = [];
        applyMirrorPreload(t, entryDecisions);     // 3. MirrorPreload + MirrorTouch
        t.preloadDecisions = entryDecisions;       //    store per-tx, not aggregated
        if (t !== tx) {
          // Inject a synthetic ⑤b event into the member's timeline so it
          // can show its OWN preload result (the runner only emits an
          // event for the lead, the tx it's actually walking through).
          const proxy = proxyOf(t);
          if (proxy) {
            proxy.timeline = [...proxy.timeline, {
              stepIndex: 10,
              color: COLORS.apply,
              ...eventPreloadResult(t),
            }];
          }
        }
        applyOrder(t);                             // 4. applyProposal business logic
      }
    },
  },
  {
    title: "⑤c pebble.Batch commit (NoSync)",
    desc:  "Once the in-memory FSM-apply loop has walked every entry of the Ready tick, machine.go calls batch.Commit(pebble.NoSync). That single batch holds: log/audit entries for each order, the 0xFF cache-mirror writes for the touched/written keys, and any rotation meta if a generation crossed. NoSync means the write is acknowledged once Pebble's memtable has it (no fsync to Pebble's internal WAL right away) — that's safe because the upstream Raft WAL (separate from Pebble) already holds the entry durably; on crash the FSM replays from there. The applier still holds the channel slot through this step; it's only released at ⑥a (when the ack starts walking back from Pebble), so the next batch's pop is visually synced with this batch's ack ascent.",
    color: COLORS.apply,
    // event runs AFTER the action so the IDs in the timeline entry reflect the
    // freshly-allocated values from the boundary bump (set at ⑤b).
    event:  (tx) => eventApplyResult(tx, cache),
    action: async (tx) => {
      const allTxs = batchTxs(tx);
      // In-memory writes were done at ⑤b; this step just animates the
      // single pebble.Batch.Commit (cache-mirror + Pebble durability).
      await Promise.all([
        travel(tx, "e-fsm-cache",  380, 5),
        travel(tx, "e-fsm-pebble", DUR_HOP, 5),
      ]);
      // Publish each order's mutated cells to Pebble — one pebble.Batch
      // contains the log/audit entries + 0xFF cache mirror writes for every
      // order in the entry.
      for (const t of allTxs) publishOrderToPebble(t);
      // lastPersistedIndex jumps to the HIGHEST commitIndex of the batch
      // (the last entry). The lead carries the lowest index — without this
      // the label would lag by (batch size − 1) entries.
      const topIdx = allTxs[allTxs.length - 1].commitIndex;
      if (topIdx > app.raft.leaderApplied) {
        app.raft.leaderApplied = topIdx;
        app.raft.f1Applied     = topIdx;
        app.raft.f2Applied     = topIdx;
      }
      // Wake every batched member parked at await tx.batch.applied. Stagger
      // them so the response dots leave the FSM area visibly spread out
      // (each member's runCycle consumes its staggerMs before kicking off
      // step ⑥). The pebble→leader ack and the notifier broadcast are
      // deferred to ⑥a (next click) — ⑤c ends here with just the cache +
      // pebble dots resting.
      if (tx.batch) {
        for (let i = 0; i < tx.batch.members.length; i++) {
          tx.batch.members[i].staggerMs = (i + 1) * STAGGER_MS;
        }
        tx.batch.resolveApplied();
      }
    },
  },
  // ─── ⑥a FSM ack: pebble → leader + notifier broadcast ────────────────
  {
    title: "⑥a FSM ack — pebble → leader + notifier broadcast",
    desc:  "Once batch.Commit returns, machine.go fires two things at once: (1) every fsmFuture in the batch resolves, sending an ack back to the leader's goroutines (visually: a single dot chain pebble → FSM → leader, ending on the leader). (2) notifier.NotifyLogsCommitted(lastSeq) emits a signal.FanOut to the four subscribers (visually: a separate dot from the FSM right edge to the notifier box). Members of a batched entry skip this step — there's only ONE ack per entry, the lead drives it. Responses to clients + worker fan-out happen at the next click (⑥b). The applier slot is freed exactly here, so the next batch waiting at the channel midpoint pops as the ack starts walking back.",
    color: COLORS.resp,
    // Members skip — the ack is per-batch, only the lead emits it.
    skipIf: (tx) => tx.batch && !tx.batchLead,
    action: async (tx) => {
      // Release the applier slot at the moment the ack begins climbing
      // back. The next batch parked at the channel midpoint pops here and
      // its dot crosses paths with this batch's ack on the second half of
      // the queue arc — matches the real applier reading the next chan
      // slot the instant it finishes the current batch's commit.
      if (tx._heldFsmLock) {
        // Call release on the module-scoped bare lock — Svelte 5 proxifies
        // anything stored on the tx state, and going through a stored ref
        // would call release on the proxy whose mutations may not surface
        // on the bare lock that the rest of the code awaits on.
        fsmLock.release();
        tx._heldFsmLock = false;
      }
      await Promise.all([
        travel(tx, [
          { id: "e-fsm-pebble",     reverse: true },
          { id: "e-leader-fsm-2",   reverse: true },
          { id: "e-leader-fsm-1",   reverse: true },
        ], 400, 5),
        travel(tx, "e-fsm-notifier", DUR_QUICK, 5),
      ]);
    },
  },
  // ─── ⑥b Responses + worker fan-out ────────────────────────────────────
  {
    title: "⑥b Response to client · worker fan-out",
    desc:  "Each tx's goroutine, now unblocked by its resolved fsmFuture, sends ApplyResponse back to its client via the gRPC reverse chain (leader → admission → controller → gRPC → client). In parallel, the notifier signal that landed at ⑥a fans out to every subscriber: Index Builder (batch ~1000 logs → ReadStore), Event Sinks (batch ~64 → Kafka / NATS / ClickHouse), Cold Storage (FSM-dispatched archive jobs), Sealer (BLAKE3 hash chain). Each subscriber wakes on its own buffered(1) channel + 100ms ticker fallback. For a batched entry, each member emits its own response in parallel; the worker fan-out fires ONCE for the whole batch (one notifier signal).",
    color: COLORS.resp,
    action: async (tx) => {
      // Batched member: only own ApplyResponse — the worker fan-out is a
      // per-batch thing and the lead drives it.
      if (tx.batch && !tx.batchLead) {
        await travel(tx, [
          { id: "e-leader-adm" },
          { id: "e-ctrl-adm",    reverse: true },
          { id: "e-grpc-ctrl",   reverse: true },
          { id: "e-client-grpc", reverse: true },
        ], 1500, 6);
        return;
      }
      // Lead (or unbatched): response + worker fan-out in parallel.
      await Promise.all([
        travel(tx, [
          { id: "e-leader-adm" },
          { id: "e-ctrl-adm",    reverse: true },
          { id: "e-grpc-ctrl",   reverse: true },
          { id: "e-client-grpc", reverse: true },
        ], 1500, 6),
        travel(tx, "e-notifier-w-index", 280, 4),
        travel(tx, "e-notifier-w-sinks", 320, 4),
        travel(tx, "e-notifier-w-archiver", 360, 4),
        travel(tx, "e-notifier-w-sealer", 400, 4),
      ]);
    },
    event: (tx) => eventResponse(tx, cache, app.raft),
  },
];

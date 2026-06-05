import { txKeys, txDefaults } from "./cache.js";

// Tagged template that wraps tokens with span classes. Each value may be:
//   • a 2-tuple ["k"|"s"|"n"|"c"|"h", text]  → wrapped span
//   • anything else                            → interpolated verbatim
export function paint(strings, ...values) {
  let out = "";
  strings.forEach((s, i) => {
    out += s;
    if (i >= values.length) return;
    const v = values[i];
    if (Array.isArray(v) && v.length === 2 && typeof v[0] === "string") {
      out += `<span class="${v[0]}">${v[1]}</span>`;
    } else {
      out += String(v);
    }
  });
  return out;
}
export const K   = v => ["k", v];
export const S   = v => ["s", v];
export const N   = v => ["n", v];
export const CMT = v => ["c", v];
export const H   = v => ["h", v];
export const R   = v => ["r", v];  // rejected — used by ⑤b "preload rejected, Gen0 already had it"
export const W   = v => ["w", v];  // warning — used by ProposalGuard rebuild at ①f

// Multi-line HTML indenter. paint`...` never emits a span that straddles a
// newline, so adding pad to each line is safe.
export function indentText(text, spaces) {
  const pad = " ".repeat(spaces);
  return text.split("\n").map(l => l.length === 0 ? l : pad + l).join("\n");
}

export function fmtValue(v) {
  if (v == null) return "—";
  if (typeof v === "object" && "in" in v && "out" in v)        return `{in:${v.in}, out:${v.out}}`;
  if (typeof v === "object" && "nextLogId" in v)
    return `{log:${v.nextLogId}, tx:${v.nextTxId}, post:${v.postingCount}}`;
  if (typeof v === "object" && Object.keys(v).length === 0)    return "{}";
  if (typeof v === "object")                                    return JSON.stringify(v).replace(/"/g, "");
  return String(v);
}

// Deterministic faux-sha256 prefix derived from log id (for visual variety).
export function hashFor(id) {
  const seed = (typeof id === "number" ? id : 1) * 2654435761;
  return ((seed >>> 0) % 0xffff_ffff).toString(16).padStart(8, "0");
}

// ── Builders ────────────────────────────────────────────────────────────
// Each takes a cluster snapshot { tx, cache, raft } so the output reflects the
// state at the moment the builder runs (i.e. the step that just ran for the tx).

export function buildOrderPayload(tx) {
  return paint`${H("Apply(CreateLog)")}  ${CMT("// gRPC unary call, BucketService :8888")}
${K("ledger")}:    ${S(`"${tx.ledger}"`)}
${K("type")}:      ${S("NEW_TRANSACTION")}
${K("postings")}:  [{ ${K("source")}: ${S(`"${tx.source}"`)}, ${K("destination")}: ${S(`"${tx.destination}"`)}, ${K("amount")}: ${N(String(tx.amount))}, ${K("asset")}: ${S(`"${tx.asset}"`)} }]
${K("reference")}: ${S(`"${tx.referenceId}"`)}`;
}

export const PAYLOAD_ACK = paint`${H("MsgAppResp")}  ${CMT("// each follower acks after fsync")}
${K("from")}:  F1, F2
${K("term")}:  ${N("1")}
${K("match")}: leader's last index   ${CMT("// = leaderIdx after step ②")}

${H("Leader observes quorum")} → advance ${K("commitIdx")}
${CMT("// new commitIdx will be piggybacked onto the next AppendEntries")}`;

// Plan-based builders: tx.proposalPlan is frozen at ①d. Builders that recompute
// against the live cache would lie after Previous or after a parallel tx
// updates the cache, so they all read from the snapshot.
export function planFor(tx, cache, raft) {
  if (tx.proposalPlan) return tx.proposalPlan;
  return computePlan(tx, cache, raft);
}

// Force-compute path used by the ProposalGuard rebuild at ①f when the cache
// generation has moved since admission's optimistic BuildPreloads.
export function computePlan(tx, cache, raft) {
  const futureIdx = raft.leaderIdx + 1;
  const plan = { guaranteed: [], needsTouch: [], miss: [], futureIdx };
  for (const key of txKeys(tx)) {
    const r = cache.checkCache(futureIdx, key);
    if (r === "GUARANTEED")        plan.guaranteed.push(key);
    else if (r === "NEEDS_TOUCH")  plan.needsTouch.push(key);
    else                           plan.miss.push(key);
  }
  tx.proposalPlan = plan;
  return plan;
}

export function buildCheckCachePayload(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  let body = paint`${H(`CheckCache(nextIdx=${plan.futureIdx}, …)`)}  ${CMT("// no lock, dual-generation lookup")}\n`;
  for (const key of txKeys(tx)) {
    let label, comment;
    if (plan.guaranteed.includes(key))      { label = "CacheGuaranteed"; comment = "already in Gen0"; }
    else if (plan.needsTouch.includes(key)) { label = "CacheNeedsTouch"; comment = "in Gen1, schedule promote"; }
    else                                    { label = "CacheMiss";       comment = "absent — needs Pebble lookup"; }
    body += paint`  • ${K(key)} → ${N(label)} ${CMT("(" + comment + ")")}\n`;
  }
  return `${buildOrderPayload(tx)}\n\n` + body.trimEnd();
}

export function buildPebbleLoadsPayload(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  if (plan.miss.length === 0) {
    return `${buildOrderPayload(tx)}\n\n` +
      paint`${H("Pebble lazy-load")}  ${CMT("// nothing to load — every key was Guaranteed or NeedsTouch")}`;
  }
  const defs = txDefaults(tx);
  let body = paint`${H("Pebble lazy-load")}  ${CMT("// only CacheMiss keys, parallelism=16")}\n`;
  for (const key of plan.miss) {
    const v = defs[key];
    const isZeroVolume = key.startsWith("volumes(") && v.in === 0 && v.out === 0;
    const note = isZeroVolume
      ? paint`new account → ${N("zero-value injected")}`
      : paint`Get(Pebble) returned ${S(fmtValue(v))}`;
    body += paint`  • ${K(key)} → ` + note + "\n";
  }
  return `${buildOrderPayload(tx)}\n\n` + body.trimEnd();
}

export function buildProposalPayload(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  const defs = txDefaults(tx);
  let body = paint`${H("+ Preload payload")}  ${CMT("// attached to the Raft command — apply will read these from cache only")}\n`;
  body += paint`${K("preloads")}: [`;
  if (plan.miss.length === 0) {
    body += paint`  ${CMT("(empty — all keys Guaranteed or NeedsTouch)")}`;
  } else {
    for (const key of plan.miss) {
      body += "\n  " + paint`${K(key)} → ${S(fmtValue(defs[key]))}`;
    }
    body += "\n";
  }
  body += paint`]\n${K("touches")}: [`;
  if (plan.needsTouch.length === 0) {
    body += paint`  ${CMT("(empty — nothing in Gen1 to promote)")}`;
  } else {
    for (const key of plan.needsTouch) {
      body += "\n  " + paint`${K(key)}   ${CMT("// Gen1 → Gen0 promotion")}`;
    }
    body += "\n";
  }
  body += "]";
  return `${buildOrderPayload(tx)}\n\n` + body;
}

export function buildCommittedPayload(tx, cache, raft) {
  const proposal = indentText(buildProposalPayload(tx, cache, raft), 4);
  return paint`${H("Committed Raft entry")}  ${CMT("// quorum reached — same proposal as ①f, now wrapped in a Raft envelope")}

${K("Entry")} {
  ${K("type")}:    LOG_CREATE
  ${K("term")}:    ${N(String(tx.commitTerm  ?? raft.term))}
  ${K("index")}:   ${N(String(tx.commitIndex ?? raft.leaderIdx))}

  ${K("command")}: ${K("Proposal")} {
${proposal}
  }
}`;
}

export function buildPreloadResultPayload(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  const defs = txDefaults(tx);
  let body = paint`${H("FSM.Preload(payload)")}  ${CMT("// runs BEFORE the deterministic apply — populates the cache")}\n\n`;
  body += paint`${H("MirrorPreload")} → Gen0:\n`;
  if (plan.miss.length === 0) body += paint`  ${CMT("(no entries — every key was already cached)")}\n`;
  else for (const key of plan.miss) {
    body += paint`  ${K(key)} = ${S(fmtValue(defs[key]))}\n`;
  }
  body += "\n" + paint`${H("MirrorTouch")} (Gen1 → Gen0):\n`;
  if (plan.needsTouch.length === 0) body += paint`  ${CMT("(no touches)")}\n`;
  else for (const key of plan.needsTouch) {
    body += paint`  ${K(key)}\n`;
  }
  body += "\n" + paint`${CMT("// invariant #3: from now on, the apply step reads the cache exclusively")}`;
  return body;
}

export function buildApplyPreviewPayload(tx, cache) {
  const bnd = cache.gen0.get(`boundary(${tx.ledger})`);
  const nextLog = bnd ? bnd.value.nextLogId : "?";
  const nextTx  = bnd ? bnd.value.nextTxId  : "?";
  return paint`${H("FSM deterministic apply running …")}  ${CMT("// processing committed entry inside Pebble batch")}

${CMT("// Reads from cache (Gen0) only — never Pebble.")}
${CMT("// Will allocate:")}
  ${K("Log.id")}   ← boundary.nextLogId  = ${N(String(nextLog))}
  ${K("Log.txid")} ← boundary.nextTxId   = ${N(String(nextTx))}

${CMT("// Will mutate:")}
  ${K(`volumes(${tx.source}.${tx.asset})`)}.out += ${N(String(tx.amount))}
  ${K(`volumes(${tx.destination}.${tx.asset})`)}.in += ${N(String(tx.amount))}
  ${K(`boundary(${tx.ledger})`)}.nextLogId++ · .nextTxId++ · .postingCount++`;
}

export function buildApplyResultPayload(tx, cache) {
  const bnd = cache.gen0.get(`boundary(${tx.ledger})`);
  const logId = bnd ? bnd.value.nextLogId - 1 : "?";
  const txId  = bnd ? bnd.value.nextTxId  - 1 : "?";
  const posts = bnd ? bnd.value.postingCount  : 0;
  return paint`${H("Apply result")}  ${CMT("// FSM has generated IDs and mutated cache + Pebble in one batch")}

${K("Log")} {
  ${K("id")}:     ${N(String(logId))},          ${CMT("// global log id, allocated from boundary.nextLogId")}
  ${K("type")}:   NEW_TRANSACTION,
  ${K("txid")}:   ${N(String(txId))},          ${CMT("// per-ledger transaction id")}
  ${K("hash")}:   ${S(`"sha256:${hashFor(logId)}…"`)}
}
${K("Δ volumes")}:  ${tx.source}.${tx.asset} −${tx.amount}  ·  ${tx.destination}.${tx.asset} +${tx.amount}
${K("boundary")}:  postingCount = ${N(String(posts))}  ${CMT("// per-ledger total since seal")}`;
}

// ── Event builders ──────────────────────────────────────────────────────
// Each step that pushes a lifecycle event uses one of these. They produce a
// COMPACT html block — no preamble — because the timeline view stitches them
// together in chronological order; the order details only appear once (at ①a)
// and downstream events focus on the delta of information they introduce.

export function eventOrder(tx) {
  return { title: "Order received by gRPC server", html: buildOrderPayload(tx) };
}

export function eventCheckCache(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  let body = paint`${H(`CheckCache(nextIdx=${plan.futureIdx}, …)`)}  ${CMT("// no lock, dual-generation lookup")}\n`;
  for (const key of txKeys(tx)) {
    let label, comment;
    if (plan.guaranteed.includes(key))      { label = "CacheGuaranteed"; comment = "already in Gen0"; }
    else if (plan.needsTouch.includes(key)) { label = "CacheNeedsTouch"; comment = "in Gen1, schedule promote"; }
    else                                    { label = "CacheMiss";       comment = "absent — needs Pebble lookup"; }
    body += paint`  • ${K(key)} → ${N(label)} ${CMT("(" + comment + ")")}\n`;
  }
  return { title: "Admission · CheckCache decision", html: body.trimEnd() };
}

export function eventPebbleLoads(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  if (plan.miss.length === 0) return null;   // step is skipped — no event
  const defs = txDefaults(tx);
  let body = paint`${H("Pebble lazy-load")}  ${CMT("// only CacheMiss keys, parallelism=16")}\n`;
  for (const key of plan.miss) {
    const v = defs[key];
    const isZeroVolume = key.startsWith("volumes(") && v.in === 0 && v.out === 0;
    const note = isZeroVolume
      ? paint`new account → ${N("zero-value injected")}`
      : paint`Get(Pebble) returned ${S(fmtValue(v))}`;
    body += paint`  • ${K(key)} → ` + note + "\n";
  }
  return { title: "Admission · Pebble lazy-load", html: body.trimEnd() };
}

export function eventProposal(tx, cache, raft) {
  const plan = planFor(tx, cache, raft);
  const defs = txDefaults(tx);
  let body = paint`${H("Proposal sent to Raft")}  ${CMT("// admission.go:294-500 — NewCommand(orders...) + marshal + AcquireProposalGuard + Propose + Wait FSM future")}\n\n`;
  body += paint`${CMT("// Admission pipeline (one Admit() call = one Raft entry, even if N orders batched):")}\n`;
  body += paint`${CMT("//   verifyAndResolveSignatures  → requestsToOrders → extractPreloadNeeds")}\n`;
  body += paint`${CMT("//   resolveScriptsAndEnrichNeeds (numscript orders only — skipped for simple sends)")}\n`;
  body += paint`${CMT("//   NewCommand(orders...) → BuildPreloads (no lock, parallel) → marshalCommand")}\n`;
  body += paint`${CMT("//   AcquireProposalGuard (tracker lock) → conditional re-marshal → Propose → release")}\n`;
  body += paint`${CMT("//   fsmFuture.Wait() blocks here until FSM apply completes\n")}\n`;
  body += paint`${K("preloads")}: [`;
  if (plan.miss.length === 0) {
    body += paint`  ${CMT("(empty — all keys Guaranteed or NeedsTouch)")}`;
  } else {
    for (const key of plan.miss) body += "\n  " + paint`${K(key)} → ${S(fmtValue(defs[key]))}`;
    body += "\n";
  }
  body += paint`]\n${K("touches")}: [`;
  if (plan.needsTouch.length === 0) {
    body += paint`  ${CMT("(empty — nothing in Gen1 to promote)")}`;
  } else {
    for (const key of plan.needsTouch) body += "\n  " + paint`${K(key)}   ${CMT("// Gen1 → Gen0 promotion")}`;
    body += "\n";
  }
  body += "]";
  return { title: "Proposal sent to Raft", html: body };
}

export function eventCommitted(tx, cache, raft) {
  const html = paint`${H("Committed Raft entry")}  ${CMT("// quorum reached; the proposal above is now durable in the cluster")}

${K("Entry")} {
  ${K("type")}:    LOG_CREATE
  ${K("term")}:    ${N(String(tx.commitTerm  ?? raft.term))}
  ${K("index")}:   ${N(String(tx.commitIndex ?? raft.leaderIdx))}
  ${K("command")}: → ${CMT("see 'Proposal sent to Raft' above")}
}`;
  return { title: "Raft · Committed", html };
}

export function eventPreloadResult(tx /*, cache, raft */) {
  // The action at ⑤b records exactly what MirrorPreload did with each
  // preload entry (tx.preloadDecisions). We replay that decision log here
  // instead of recomputing from plan.miss, so a STALE preload — one whose
  // value was rejected because Gen0 already held a fresher entry from a
  // prior tx's apply — is visible to the reader.
  const decisions = tx.preloadDecisions || [];
  let body = paint`${H("FSM.Preload(payload)")}  ${CMT("// runs BEFORE the deterministic apply — populates the cache for keys not yet there")}\n\n`;
  body += paint`${H("MirrorPreload")} → Gen0:\n`;
  if (decisions.length === 0) {
    body += paint`  ${CMT("(no preload payload — admission found every key Guaranteed in Gen0 at ①d, nothing to mirror)")}\n`;
  } else {
    for (const d of decisions) {
      if (d.op === "rejected") {
        body += paint`  ${R("✕")} ${K(d.key)}   ${CMT("REJECTED — preload " + fmtValue(d.preload) + " · Gen0 already holds")} ${S(fmtValue(d.kept))}\n`;
      } else if (d.op === "touched") {
        body += paint`  ${K(d.key)} = ${S(fmtValue(d.value))}   ${CMT("// promoted from Gen1 → Gen0 (MirrorTouch)")}\n`;
      } else if (d.op === "written") {
        body += paint`  ${K(d.key)} = ${S(fmtValue(d.value))}   ${CMT("// new entry from preload")}\n`;
      }
    }
  }
  body += "\n" + paint`${CMT("// invariant: preload never overwrites a Gen0 entry — that's how stale preloads stay harmless.")}`;
  return { title: "FSM · Preload merged into cache", html: body };
}

export function eventApplyResult(tx /*, cache */) {
  // Read from the tx snapshot (recorded by step ⑤c) — by the time this event
  // is emitted in multi-tx mode, a later rotation might have moved the
  // boundary out of Gen0, so reading the cache here would return "?".
  const logId = tx.appliedLogId ?? "?";
  const txId  = tx.appliedTxId  ?? "?";
  const posts = tx.appliedPostingCount ?? 0;
  const html = paint`${H("Apply result")}  ${CMT("// machine.go applyProposal — one pebble.Batch holds the entire entry, committed NoSync")}

${CMT("// FSM applyProposal sub-sequence (single batch):")}
${CMT("//   1. applyTechnicalUpdates  — cluster config / shared state")}
${CMT("//   2. MirrorPreload + MirrorTouch  — cache populated (already shown at ⑤b)")}
${CMT("//   3. HLC timestamp advance")}
${CMT("//   4. ensurePeriodBootstrapped")}
${CMT("//   5. WriteSet.Reset(effectiveDate)")}
${CMT("//   6. processor.ProcessOrders  — numscript + posting validation")}
${CMT("//   7. ComputeAuditHash  — blake3 over orders")}
${CMT("//   8. appendAuditEntries  — log + audit")}
${CMT("//   9. WriteSet.Merge  — volumes, boundary, metadata, references, ...")}

${K("Log")} {
  ${K("id")}:     ${N(String(logId))},          ${CMT("// allocated from boundary.nextLogId")}
  ${K("type")}:   NEW_TRANSACTION,
  ${K("txid")}:   ${N(String(txId))},          ${CMT("// per-ledger transaction id")}
  ${K("hash")}:   ${S(`"sha256:${hashFor(logId)}…"`)}
}
${K("Δ volumes")}:  ${tx.source}.${tx.asset} −${tx.amount}  ·  ${tx.destination}.${tx.asset} +${tx.amount}
${K("boundary")}:  postingCount = ${N(String(posts))}

${H("pebble.Batch writes (NoSync)")}:
  ${K("log/<id>")}          → audit + log entry (orders + posted hash)
  ${K("0xFF cache mirror")} → volumes(${tx.source}.${tx.asset}), volumes(${tx.destination}.${tx.asset}), boundary(${tx.ledger})
  ${K("rotation meta")}     ${CMT("// if a gen boundary was crossed, the rotation lands in this same batch")}

${K("lastPersistedIndex")} ← ${N(String(tx.commitIndex ?? "?"))}   ${CMT("// in-memory atomic, NOT persisted on disk — feeds WaitForApplied for linearizable reads")}
${CMT("// durability of the batch comes from the upstream Raft WAL, not from fsync here (pebble.NoSync)")}`;
  return { title: "FSM · Apply mutations committed", html };
}

export function eventResponse(tx, cache, raft) {
  const logId = tx.appliedLogId ?? "?";
  const txId  = tx.appliedTxId  ?? "?";
  const html = paint`${H("ApplyResponse → client + notifier broadcast")}  ${CMT("// machine.go:837 — fsm.notifier.NotifyLogsCommitted(lastSeq)")}

${K("ApplyResponse")} { ${K("id")}: ${N(String(logId))}, ${K("txid")}: ${N(String(txId))}, ${K("hash")}: ${S(`"sha256:${hashFor(logId)}…"`)} }
${K("status")}: ${N("OK")}  ${CMT("// gRPC unblocks once fsmFuture resolves; this happens before the notifier fans out")}

${H("notifier.NotifyLogsCommitted(lastSeq)")}  ${CMT("// single broadcast, signal.FanOut")}
${CMT("// Each subscriber owns a buffered(1) channel + 100 ms ticker fallback:")}
  → ${K("Index Builder")}     ${CMT("batch ~1000 logs → ReadStore writes")}
  → ${K("Event Sinks")}       ${CMT("batch ~64 → Kafka / NATS / ClickHouse")}
  → ${K("Cold Storage (S3)")} ${CMT("// note: FSM also pushes ArchiveRequests on archiveRequestCh directly")}
  → ${K("Sealer")}            ${CMT("BLAKE3 hash chain over committed period")}

${CMT("// Note: the cache mirror to Pebble's 0xFF prefix is NOT a downstream worker — it happens")}
${CMT("// inside the FSM batch at ⑤c, in the same pebble.Batch as the log+audit writes.")}
${CMT("// All four subscribers above run off the gRPC critical path — the client already has its answer.")}`;
  return { title: "Response sent · notifier fan-out", html };
}

// ProposalGuard rebuild — only emitted when the generation moved between
// admission's optimistic BuildPreloads (①d/①e) and AcquireProposalGuard (①f).
// Mirrors the rebuild path at internal/infra/preload/preloader.go:209-236.
export function eventGuardRebuild(tx, stalePlan, freshPlan) {
  const fmtKeys = (arr) => arr.length === 0 ? "[]" : "[ " + arr.join(", ") + " ]";
  const html = paint`${W("⚠ AcquireProposalGuard · preload REBUILT")}  ${CMT("// gen(nextIndex) changed under the tracker lock — preloader.go:209-236")}

${K("stale plan @ ①d")}: futureIdx=${N(String(stalePlan.futureIdx))}
  ${K("miss")}:        ${fmtKeys(stalePlan.miss)}
  ${K("needsTouch")}:  ${fmtKeys(stalePlan.needsTouch)}
  ${K("guaranteed")}:  ${fmtKeys(stalePlan.guaranteed)}

${K("rebuilt under lock")}: futureIdx=${N(String(freshPlan.futureIdx))}
  ${K("miss")}:        ${fmtKeys(freshPlan.miss)}
  ${K("needsTouch")}:  ${fmtKeys(freshPlan.needsTouch)}
  ${K("guaranteed")}:  ${fmtKeys(freshPlan.guaranteed)}

${CMT("// the proposal that lands at Raft carries the REBUILT preloads, not the stale ones")}`;
  return { title: "⚠ Proposal guard · preload rebuilt (stale)", html };
}

// ── (legacy single-payload builders kept below for any callers) ─────────

export function buildResponsePayload(tx, cache, raft) {
  const bnd = cache.gen0.get(`boundary(${tx.ledger})`);
  const logId = bnd ? bnd.value.nextLogId - 1 : "?";
  const txId  = bnd ? bnd.value.nextTxId  - 1 : "?";
  return paint`${H("ApplyResponse → client")}  ${CMT("// resolved on the leader after fan-out completes")}

${K("Log")} { ${K("id")}: ${N(String(logId))}, ${K("txid")}: ${N(String(txId))}, ${K("hash")}: ${S(`"sha256:${hashFor(logId)}…"`)} }
${K("status")}: ${N("OK")}  ${CMT("// gRPC OK, committed at lastPersistedIndex = " + raft.leaderApplied)}

${CMT("// In parallel, off the critical path:")}
${H("Workers")} tail the global log → Index Builder, Event Sinks, Archiver, Sealer.`;
}

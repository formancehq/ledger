import { EDGE_ID, NODE } from "../topology";
import type { Emit, Entry, Msg, NodeId, TickCtx } from "../types";
import { makeBatch } from "../types";
import { Node } from "./base";

// Leader — the heart of the Raft happy path. Receives Propose from
// admission, appends to its log, broadcasts AppendEntries to F1 & F2.
// Tracks matchIdx per follower; on each AppendResp checks quorum and
// advances commitIdx. When commitIdx advances, two independent
// emissions fire:
//   1. ApplyTrigger(commitIdx) to fsmL — leader's own apply.
//   2. AppendEntries(entries=[], leaderCommit=commitIdx) to F1 & F2 —
//      the heartbeat AE that carries the new commit to the followers
//      so they can advance their own commit and apply.
// FsmAck from fsmL closes the loop with a ClientResp routed back up
// the reverse chain.
//
// What's NOT modeled in Phase 1: batching window (each Propose triggers
// its own AE round), ApplyStager 4-tier (FSM is treated as always
// available), term/election (term stays at 1).

const TERM = 1;

const FOLLOWERS: readonly NodeId[] = [NODE.followerF1, NODE.followerF2];
const QUORUM_SIZE = 2;  // 3-node cluster; quorum is 2 (including self)

export interface LeaderState {
  log:        Entry[];
  commitIdx:  number;
  matchIdx:   Record<NodeId, number>;
  appliedIdx: number;
  // Map entry index → txId so we can later route the ClientResp.
  entryToTx:  Record<number, number>;
  // Entries newly appended during the current tick. The flush hook
  // drains this into ONE AppendEntries per follower at the end of the
  // tick — mirrors etcd/raft's Ready cycle where many Propose calls
  // collapse into a single batched outbound.
  pendingBatch: Entry[];
  // Watermark of the last apply batch dispatched to qLeaderFsmL.
  // Apply back-pressure is implicit: the leader can dispatch the
  // (dispatchedApplyUpTo, commitIdx] range as ONE batch whenever the
  // queue has room. Multiple commit advances while the queue is full
  // therefore coalesce into a single bigger batch on the next emit.
  dispatchedApplyUpTo: number;
  // The last leaderCommit value we sent to followers. Bumped when flush
  // emits AE (heartbeat or batch). Used to decide whether a standalone
  // heartbeat AE is needed when commit advanced but no new entries are
  // pending — keeps followers' commitIdx in sync.
  lastEmittedLeaderCommit: number;
}

export class LeaderNode extends Node<LeaderState> {
  readonly id = NODE.leader;

  initialState(): LeaderState {
    return {
      log:                     [],
      commitIdx:               0,
      matchIdx:                { [NODE.leader]: 0, [NODE.followerF1]: 0, [NODE.followerF2]: 0 },
      appliedIdx:              0,
      entryToTx:               {},
      pendingBatch:            [],
      dispatchedApplyUpTo:     0,
      lastEmittedLeaderCommit: 0,
    };
  }

  // Post-tick flush — the leader's "Ready" cycle. Bundles in ONE moment
  // (same scheduler iteration) the three actions a real raft node
  // performs after processing inbound msgs:
  //   1. AE to followers (heartbeat if no new entries, batch otherwise).
  //   2. WriteWAL for new entries (skipped if no new entries).
  //   3. ApplyTrigger to the applier for newly-committed entries.
  flush(ctx: TickCtx): { state: LeaderState; emit: Emit[] } {
    let state = this.state;
    const emit: Emit[] = [];

    const hasNewEntries  = state.pendingBatch.length > 0;
    const commitAdvanced = state.commitIdx > state.lastEmittedLeaderCommit;

    // 1 + 2: AE (batch or heartbeat) and WriteWAL for new entries.
    if (hasNewEntries) {
      const batch = makeBatch(state.pendingBatch);
      const ae: Msg = {
        kind:         "AppendEntries",
        batch,
        leaderCommit: state.commitIdx,
        term:         TERM,
        from:         NODE.leader,
      };
      for (const fid of FOLLOWERS) emit.push(emitTo(ctx, fid, ae));
      emit.push({
        to:  NODE.walLeader,
        via: ctx.pathFor(NODE.leader, NODE.walLeader),
        msg: { kind: "WriteWAL", batch, from: NODE.leader },
      });
      state = { ...state, pendingBatch: [], lastEmittedLeaderCommit: state.commitIdx };
    } else if (commitAdvanced) {
      // No new entries to replicate but commit moved — followers need a
      // heartbeat AE to learn the new leaderCommit. Carry the entries
      // that just became committed (lastEmittedLeaderCommit, commitIdx]
      // so the dot is tx-colored and the lifecycle surfaces the event
      // under each tx. Follower de-duplicates by index on append, so
      // these "already-known" entries don't grow its log a second time.
      const justCommitted = state.log.slice(state.lastEmittedLeaderCommit, state.commitIdx);
      const heartbeat: Msg = {
        kind:         "AppendEntries",
        batch:        makeBatch(justCommitted),
        leaderCommit: state.commitIdx,
        term:         TERM,
        from:         NODE.leader,
      };
      for (const fid of FOLLOWERS) emit.push(emitTo(ctx, fid, heartbeat));
      state = { ...state, lastEmittedLeaderCommit: state.commitIdx };
    }

    // 3: ApplyTrigger for any newly-committed entries the queue can take.
    const applyResult = tryDispatchApply(state, ctx);
    state = applyResult.state;
    for (const e of applyResult.emit) emit.push(e);

    return { state, emit };
  }

  handle(msg: Msg, ctx: TickCtx): { state: LeaderState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "Propose": {
        // Assign next index, append to log, queue the entry for flush.
        // matchIdx[leader] is NOT bumped here — it advances only when
        // the leader's own WAL ack lands (handle(WALAck) below). Real
        // Raft requires the leader to fsync its own log before
        // counting itself toward quorum.
        const index = state.log.length + 1;
        const entry: Entry = { term: TERM, index, txId: msg.txId, order: msg.order };
        return {
          state: {
            ...state,
            log:          [...state.log, entry],
            entryToTx:    { ...state.entryToTx, [index]: msg.txId },
            pendingBatch: [...state.pendingBatch, entry],
          },
          emit: [],
        };
      }

      case "AppendResp":
        // A follower has fsync'd up through msg.matchIdx.
        return processMatchUpdate(state, msg.from, msg.matchIdx);

      case "WALAck":
        // Leader's OWN WAL has fsync'd up through msg.batch.upTo —
        // count ourselves toward quorum.
        return processMatchUpdate(state, NODE.leader, msg.batch.upTo);

      case "FsmAck": {
        // Leader's FSM finished applying through `batch.upTo`. Fire a
        // ClientResp back up the chain for each newly-applied entry.
        // Back-pressure is regulated by the qLeaderFsmL queue itself —
        // no slot accounting needed here.
        const appliedIdx = msg.batch.upTo;
        const emit: Emit[] = [];
        for (let i = state.appliedIdx + 1; i <= appliedIdx; i++) {
          const txId = state.entryToTx[i];
          if (txId == null) continue;
          emit.push({
            to:  NODE.admission,
            via: ctx.pathFor(NODE.leader, NODE.admission),
            msg: { kind: "ClientResp", txId, ok: true },
          });
        }
        return { state: { ...state, appliedIdx }, emit };
      }

      default:
        return { state, emit: [] };
    }
  }

  // No per-tick hook — flush() already bundles the apply dispatch in
  // its third step. The default Node.tick is a no-op which is what we
  // want.
}

function emitTo(ctx: TickCtx, to: NodeId, msg: Msg): Emit {
  return { to, via: ctx.pathFor(NODE.leader, to), msg };
}

// Shared between AppendResp (follower acked) and WALAck (leader's own
// fsync). Pure state update: records the new matchIdx and recomputes
// quorum-based commit. The downstream emissions (heartbeat AE to
// followers, ApplyTrigger to applier) are all done by flush() in the
// same scheduler iteration — that's the leader's "Ready cycle".
function processMatchUpdate(state: LeaderState, nodeId: NodeId, matchIdx: number): { state: LeaderState; emit: Emit[] } {
  const newMatchIdx = { ...state.matchIdx, [nodeId]: matchIdx };
  const newCommit   = computeCommitIdx(newMatchIdx, state.commitIdx);
  return {
    state: { ...state, matchIdx: newMatchIdx, commitIdx: newCommit },
    emit:  [],
  };
}

// Coalescing dispatcher: if commit has moved past what we last sent to
// the FSM AND the leader→applier QueueEdge has room, ship ONE
// ApplyTrigger covering the entire (dispatchedApplyUpTo, commitIdx]
// range. Multiple commit advances while the queue was full therefore
// merge into a single bigger batch on the next emit — matches real
// Raft applier coalescing.
function tryDispatchApply(state: LeaderState, ctx: TickCtx): { state: LeaderState; emit: Emit[] } {
  if (state.commitIdx <= state.dispatchedApplyUpTo) return { state, emit: [] };
  const qState = ctx.peekEdge(EDGE_ID.leaderToApplierL);
  if (!qState) return { state, emit: [] };
  const used = qState.queue.length + qState.mailboxSize;
  if (used >= qState.capacity) return { state, emit: [] };
  const upTo  = state.commitIdx;
  const batch = makeBatch(state.log.slice(state.dispatchedApplyUpTo, upTo));
  return {
    state: { ...state, dispatchedApplyUpTo: upTo },
    emit: [{
      to:  NODE.applierL,
      via: ctx.pathFor(NODE.leader, NODE.applierL),
      msg: { kind: "ApplyTrigger", batch },
    }],
  };
}

// Quorum rule: with N peers (including self), commitIdx advances to the
// k-th highest matchIdx where k = floor(N/2)+1. For a 3-node cluster
// that's the second-highest (sorted desc: vals[N-QUORUM]).
function computeCommitIdx(matchIdx: Record<NodeId, number>, current: number): number {
  const vals = Object.values(matchIdx).sort((a, b) => b - a);
  const candidate = vals[QUORUM_SIZE - 1] ?? 0;
  return Math.max(current, candidate);
}

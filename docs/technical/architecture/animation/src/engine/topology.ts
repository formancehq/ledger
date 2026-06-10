import type { LeaderState } from "./nodes/leader";
import type { FollowerState } from "./nodes/follower";
import type { ApplierState } from "./nodes/applier";
import type { EdgePath, NodeId, NodeMarker, TickCtx } from "./types";
import { Edge } from "./edges/base";
import { DirectEdge } from "./edges/direct";
import { QueueEdge } from "./edges/queue";
import { ChannelEdge } from "./edges/channel";
import type { FsmState } from "./nodes/fsm";
import { BOXES } from "../lib/layout";

// Engine-side view of the visual graph. Node ids mirror BOXES.id from
// layout.ts; edges declare which (src, dst) pairs have a named SVG path
// so the scheduler can pick a `via` for each emit. Edges are first-class
// `Edge` instances (see edges/{base,direct,queue,reverse}.ts):
//   - DirectEdge "send": async network/queue hop (default). One-way
//     forward anim.
//   - DirectEdge "call": synchronous RPC. The scheduler invokes the
//     callee inline and the dot animates forward + reverse along the
//     same path, landing back at the caller with the response.
//   - QueueEdge:        bounded FIFO sitting on the wire. Drains
//     drainPerTick msgs per tick toward dst, with optional canDrain
//     gating. Replaces the legacy "queue node" idea — the queue lives
//     on the edge, not on a fake intermediate node.

// NOTE: the proposeCh / qLeaderFsmL / qFollower*FsmF* / qFsmLPebble
// entries used to be node ids; they survive here as STABLE EDGE IDS
// so the diagram + diagnostic tooling can address each queue by name
// (e.g., ctx.peekEdge("nodeL→applierL")). The edges' `id` field uses
// the canonical "src→dst" form which matches the EDGE map's keys.

export const NODE = {
  client:        "box-client",
  grpc:          "box-grpc",
  controller:    "box-ctrl",
  admission:     "box-adm",
  tracker:       "box-tracker",
  leader:        "nodeL",
  followerF1:    "nodeF1",
  followerF2:    "nodeF2",
  walLeader:     "walL",
  walF1:         "walF1",
  walF2:         "walF2",
  applierL:      "applierL",
  applierF1:     "applierF1",
  applierF2:     "applierF2",
  fsmL:          "fsmL",
  fsmF1:         "fsmF1",
  fsmF2:         "fsmF2",
  cache:         "box-cache",
  pebble:        "box-pebble",
  processing:    "box-processing",
  notifier:      "box-notifier",
  compactor:     "compactor",
  workerIndex:   "box-worker-index",
  workerSinks:   "box-worker-sinks",
  workerArch:    "box-worker-archiver",
  workerSealer:  "box-worker-sealer",
} as const satisfies Record<string, NodeId>;

// Stable edge ids for the queue edges. Producers and the diagram use
// these to look up edge snapshots via ctx.peekEdge / store.edges.
export const EDGE_ID = {
  admissionToLeader:    `${NODE.admission}→${NODE.leader}`,
  leaderToApplierL:     `${NODE.leader}→${NODE.applierL}`,
  followerF1ToApplier:  `${NODE.followerF1}→${NODE.applierF1}`,
  followerF2ToApplier:  `${NODE.followerF2}→${NODE.applierF2}`,
  applierLToPebble:     `${NODE.applierL}→${NODE.pebble}`,
} as const;

// proposeCh capacity (admission → leader). Preserved from the legacy
// proposeCh node — etcd/raft's buffered proposal channel.
export const PROPOSE_CH_CAPACITY = 8;

// Helpers — keep the topology declaration terse.
const send  = (id: string, path: EdgePath): DirectEdge => new DirectEdge(id, "send", path);
const call  = (id: string, path: EdgePath): DirectEdge => new DirectEdge(id, "call", path);

export function makeEdges(): Map<string, Edge> {
  const list: Edge[] = [
    // Ingress column (forward)
    send(`${NODE.client}→${NODE.grpc}`,        "e-client-grpc"),
    send(`${NODE.grpc}→${NODE.controller}`,    "e-grpc-ctrl"),
    send(`${NODE.controller}→${NODE.admission}`, "e-ctrl-adm"),
    // Response chain (async reverse hops)
    send(`${NODE.grpc}→${NODE.client}`,          { id: "e-client-grpc", reverse: true }),
    send(`${NODE.controller}→${NODE.grpc}`,      { id: "e-grpc-ctrl",   reverse: true }),
    send(`${NODE.admission}→${NODE.controller}`, { id: "e-ctrl-adm",    reverse: true }),
    // Admission peripherals — function-call edges (RPC)
    call(`${NODE.admission}→${NODE.tracker}`,  "e-adm-tracker"),
    call(`${NODE.admission}→${NODE.cache}`,    "e-consult-cache"),
    call(`${NODE.admission}→${NODE.pebble}`,   "e-consult-pebble"),
    // Admission → leader: QueueEdge encapsulating the proposeCh FIFO.
    // Two-segment path: admission → midpoint (queue badge) → leader.
    // cap-8, drain WHOLE queue per tick (matches etcd's Ready loop:
    // "pull everything available from proposeCh"), accept Propose.
    new QueueEdge({
      id:           EDGE_ID.admissionToLeader,
      capacity:     PROPOSE_CH_CAPACITY,
      drainPerTick: PROPOSE_CH_CAPACITY,
      accept:       "Propose",
      junction: {
        pathIds:    ["e-adm-leader-1", "e-adm-leader-2"],
        x: 440, y: 408,
        // Y values pinned to the original proposeCh arc — admission and
        // leader boxes' centers are vertically offset from this row, so
        // we don't use `rightMid`/`leftMid` here.
        fromAnchor: () => ({ x: BOXES.adm.x + BOXES.adm.w, y: 405 }),
        toAnchor:   () => ({ x: BOXES.leader.x,            y: 410 }),
        accent: "#82aaff",
        title:  s => `proposeCh — buffered proposal channel (cap ${s.capacity})`,
        // Parallèle : admission émet sans gate, surplus dans `snap.held`.
        held:   (_, snap) => snap.held.length,
      },
    }),
    // Leader-side direct return path to admission (ClientResp).
    send(`${NODE.leader}→${NODE.admission}`, "e-leader-adm"),
    // Raft replication (forward) + Ack (async reverse)
    send(`${NODE.leader}→${NODE.followerF1}`,  "e-leader-f1"),
    send(`${NODE.leader}→${NODE.followerF2}`,  "e-leader-f2"),
    send(`${NODE.followerF1}→${NODE.leader}`,  { id: "e-leader-f1", reverse: true }),
    send(`${NODE.followerF2}→${NODE.leader}`,  { id: "e-leader-f2", reverse: true }),
    // WAL fsync — call edge (round-trip).
    call(`${NODE.leader}→${NODE.walLeader}`, "e-wal-leader"),
    call(`${NODE.followerF1}→${NODE.walF1}`, "e-wal-f1"),
    call(`${NODE.followerF2}→${NODE.walF2}`, "e-wal-f2"),
    // Apply path: raft node → applier. ChannelEdge models the Go
    // `chan applyWork, 1`: when the applier is idle (canDrain ok), the
    // second-hop fires synchronously on enqueue — no clock-tick wait.
    new ChannelEdge({
      id:    EDGE_ID.leaderToApplierL,
      accept: "ApplyTrigger",
      // Hold the drain when the leader's FSM has work in-flight (its
      // gestating slot is occupied) or a token is already heading
      // toward the applier — mailbox-collision avoidance.
      canDrain: (ctx) => {
        const fsm = ctx.peek<FsmState>(NODE.fsmL);
        if (!fsm) return true;
        const inFlight = ctx.upstreamPressure(NODE.applierL);
        return fsm.gestating == null && inFlight === 0;
      },
      junction: {
        pathIds:    ["e-leader-fsm-1", "e-leader-fsm-2"],
        x: 705, y: 400,
        // Source pinned to the applier's Y so the arc reads horizontal
        // even when the leader box is much taller than the applier rect.
        fromAnchor: () => ({ x: BOXES.leader.x + BOXES.leader.w, y: BOXES.applierL.center().y }),
        toAnchor:   () => BOXES.applierL.leftMid(),
        accent: "#82aaff",
        title:  s => `leader→fsm channel slot (cap ${s.capacity})`,
        // Séquentiel : le leader gate sur capacity, retient en interne
        // les commits non encore dispatchés.
        held: (peek) => {
          const s = peek<LeaderState>(NODE.leader);
          return s ? Math.max(0, s.commitIdx - s.dispatchedApplyUpTo) : 0;
        },
      },
    }),
    new ChannelEdge({
      id:    EDGE_ID.followerF1ToApplier,
      accept: "ApplyTrigger",
      canDrain: (ctx) => ctx.upstreamPressure(NODE.applierF1) === 0,
      junction: {
        pathIds:    ["e-f1-fsm-1", "e-f1-fsm-2"],
        x: 705, y: 270,
        fromAnchor: () => ({ x: BOXES.followerF1.x + BOXES.followerF1.w, y: BOXES.applierF1.center().y }),
        toAnchor:   () => BOXES.applierF1.leftMid(),
        accent: "#82aaff",
        title:  s => `F1→fsm channel slot (cap ${s.capacity})`,
        held: (peek) => {
          const s = peek<FollowerState>(NODE.followerF1);
          return s ? Math.max(0, s.commitIdx - s.dispatchedApplyUpTo) : 0;
        },
      },
    }),
    new ChannelEdge({
      id:    EDGE_ID.followerF2ToApplier,
      accept: "ApplyTrigger",
      canDrain: (ctx) => ctx.upstreamPressure(NODE.applierF2) === 0,
      junction: {
        pathIds:    ["e-f2-fsm-1", "e-f2-fsm-2"],
        x: 705, y: 142,
        fromAnchor: () => ({ x: BOXES.followerF2.x + BOXES.followerF2.w, y: BOXES.applierF2.center().y }),
        toAnchor:   () => BOXES.applierF2.leftMid(),
        accent: "#82aaff",
        title:  s => `F2→fsm channel slot (cap ${s.capacity})`,
        held: (peek) => {
          const s = peek<FollowerState>(NODE.followerF2);
          return s ? Math.max(0, s.commitIdx - s.dispatchedApplyUpTo) : 0;
        },
      },
    }),
    // applier → fsm — call edge (round-trip with ApplyTriggerAck).
    call(`${NODE.applierL}→${NODE.fsmL}`,   "e-applierL-fsm"),
    call(`${NODE.applierF1}→${NODE.fsmF1}`, "e-applierF1-fsm"),
    call(`${NODE.applierF2}→${NODE.fsmF2}`, "e-applierF2-fsm"),
    // fsm → applier (async FsmAck / PreparedBatch — reverse anim).
    send(`${NODE.fsmL}→${NODE.applierL}`,   { id: "e-applierL-fsm",  reverse: true }),
    send(`${NODE.fsmF1}→${NODE.applierF1}`, { id: "e-applierF1-fsm", reverse: true }),
    send(`${NODE.fsmF2}→${NODE.applierF2}`, { id: "e-applierF2-fsm", reverse: true }),
    // applier → raft (FsmAck forwarding). Routed via a DEDICATED return
    // arc (not the apply ChannelEdge in reverse) because semantically
    // this is an async future being resolved in the background, not a
    // hop through the cap-1 channel slot.
    send(`${NODE.applierL}→${NODE.leader}`,      "e-applierL-leader-ack"),
    send(`${NODE.applierF1}→${NODE.followerF1}`, "e-applierF1-followerF1-ack"),
    send(`${NODE.applierF2}→${NODE.followerF2}`, "e-applierF2-followerF2-ack"),
    // FSM-side peripherals (PrepareEntries internal phases).
    call(`${NODE.fsmL}→${NODE.cache}`,      "e-fsm-cache"),
    call(`${NODE.fsmL}→${NODE.processing}`, "e-fsm-processing"),
    // applier → pebble: ChannelEdge for the cap-1 committer slot.
    // PebbleAck flows back via DirectEdge (reverse).
    new ChannelEdge({
      id:    EDGE_ID.applierLToPebble,
      accept: "WritePebble",
      junction: {
        pathIds:    ["e-fsm-pebble-1", "e-fsm-pebble-2"],
        x: 778, y: 453,
        fromAnchor: () => BOXES.applierL.bottomMid(),
        toAnchor:   () => BOXES.pebble.topMid(),
        accent: "#ff6b6b",
        title:  s => `applier→pebble committer queue (cap ${s.capacity})`,
        // Séquentiel : l'applier ne retient au plus qu'un PreparedBatch
        // en attente (`inFlightCommit`) avant que la slot Pebble libère.
        held: (peek) => {
          const s = peek<ApplierState>(NODE.applierL);
          return s?.inFlightCommit ? 1 : 0;
        },
      },
    }),
    // pebble → applier (PebbleAck) on a DEDICATED return arc — same
    // rationale as the applier→raft ack: async future, not a queue hop.
    send(`${NODE.pebble}→${NODE.applierL}`, "e-pebble-applierL-ack"),
    // applier → notifier (NotifyLogs after pebble commit acks).
    send(`${NODE.applierL}→${NODE.notifier}`, "e-fsm-notifier"),
    // Notifier fan-out to workers
    send(`${NODE.notifier}→${NODE.workerIndex}`,  "e-notifier-w-index"),
    send(`${NODE.notifier}→${NODE.workerSinks}`,  "e-notifier-w-sinks"),
    send(`${NODE.notifier}→${NODE.workerArch}`,   "e-notifier-w-archiver"),
    send(`${NODE.notifier}→${NODE.workerSealer}`, "e-notifier-w-sealer"),
    // Compactor — both reads are synchronous (function calls).
    call(`${NODE.compactor}→${NODE.pebble}`,    "e-compactor-pebble"),
    call(`${NODE.compactor}→${NODE.walLeader}`, "e-compactor-wal"),
  ];
  const map = new Map<string, Edge>();
  for (const edge of list) map.set(edge.id, edge);
  return map;
}

// Re-export so existing TickCtx callers don't need an extra import for
// the gate predicate's narrow types.
export type { TickCtx };

// Buffering edges that carry a `junction` — the diagram iterates this
// list to render arcs + midpoint badges automatically. Takes the edges
// map as an explicit arg (the engine handle owns it) so no module-level
// singleton is needed.
export function junctionEdges(edges: Map<string, Edge>): ReadonlyArray<QueueEdge | ChannelEdge> {
  const out: (QueueEdge | ChannelEdge)[] = [];
  for (const edge of edges.values()) {
    if (edge instanceof QueueEdge || edge instanceof ChannelEdge) out.push(edge);
  }
  return out;
}

// NodeMarkers — vide. Tous les markers précédents (held proposes,
// leader/F1/F2 apply backlog) ont été absorbés par `Junction.held` sur
// les buffering edges correspondantes. La déclaration reste exportée
// pour préserver l'interface au cas où on rajoute un marker vraiment
// non-edge-lié plus tard ; EngineDiagram itère dessus tel quel.
export const NODE_MARKERS: ReadonlyArray<NodeMarker> = [];

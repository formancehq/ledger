import type { Edge } from "./edges/base";
import { makeEdges, NODE } from "./topology";
import { Scheduler } from "./scheduler";
import { EngineStore } from "./store";
import type { Order } from "./types";
import { DotsLayer } from "../lib/anim";
import { ClientNode } from "./nodes/client";
import { GrpcNode } from "./nodes/grpc";
import { ControllerNode } from "./nodes/controller";
import { AdmissionNode } from "./nodes/admission";
import { ApplierNode } from "./nodes/applier";
import { LeaderNode } from "./nodes/leader";
import { FollowerNode } from "./nodes/follower";
import { FsmPipelinedNode, FsmSimpleNode } from "./nodes/fsm";
import { CacheNode } from "./nodes/cache";
import { PebbleNode } from "./nodes/pebble";
import { ProcessingNode } from "./nodes/processing";
import { NotifierNode } from "./nodes/notifier";
import { TrackerNode } from "./nodes/tracker";
import { CompactorNode } from "./nodes/compactor";
import { WalNode } from "./nodes/wal";
import { WorkerNode } from "./nodes/worker";

// Per-instance handle returned by createEngine(). Bundles every mutable
// engine artefact so consumers (React components via context, tests via
// direct creation, side-by-side animations) operate on a self-contained
// graph instead of touching module-level singletons.
//
// The class methods (sendTx, resetEngine, triggerCompaction) are
// closures around the handle's own scheduler so they Just Work without
// the caller threading scheduler + nextTxId state manually.
export interface EngineHandle {
  store:     EngineStore;
  scheduler: Scheduler;
  edges:     Map<string, Edge>;
  dotsLayer: DotsLayer;
  sendTx:    (order: Order) => void;
  resetEngine: () => void;
  triggerCompaction: () => void;
}

// Build a brand-new engine: store + edges + scheduler + every node
// registered + initial snapshots published. Calling this twice returns
// two fully-isolated engines (no shared state).
export function createEngine(): EngineHandle {
  const store     = new EngineStore();
  const edges     = makeEdges();
  const scheduler = new Scheduler(store, edges);
  const dotsLayer = new DotsLayer();

  // Register every node — same set as the legacy bootEngine().
  scheduler.register(new ClientNode());
  scheduler.register(new GrpcNode());
  scheduler.register(new ControllerNode());
  scheduler.register(new ApplierNode(NODE.applierL,  NODE.fsmL,  NODE.leader));
  scheduler.register(new ApplierNode(NODE.applierF1, NODE.fsmF1, NODE.followerF1));
  scheduler.register(new ApplierNode(NODE.applierF2, NODE.fsmF2, NODE.followerF2));
  scheduler.register(new AdmissionNode());
  scheduler.register(new LeaderNode());
  scheduler.register(new FollowerNode(NODE.followerF1, NODE.fsmF1, NODE.walF1, NODE.applierF1));
  scheduler.register(new FollowerNode(NODE.followerF2, NODE.fsmF2, NODE.walF2, NODE.applierF2));
  scheduler.register(new FsmPipelinedNode(NODE.fsmL,  NODE.applierL));
  scheduler.register(new FsmSimpleNode(NODE.fsmF1,    NODE.applierF1));
  scheduler.register(new FsmSimpleNode(NODE.fsmF2,    NODE.applierF2));
  scheduler.register(new TrackerNode());
  scheduler.register(new CacheNode());
  scheduler.register(new PebbleNode());
  scheduler.register(new ProcessingNode());
  scheduler.register(new NotifierNode());
  scheduler.register(new WorkerNode(NODE.workerIndex));
  scheduler.register(new WorkerNode(NODE.workerSinks));
  scheduler.register(new WorkerNode(NODE.workerArch));
  scheduler.register(new WorkerNode(NODE.workerSealer));
  scheduler.register(new WalNode(NODE.walLeader, NODE.leader));
  scheduler.register(new WalNode(NODE.walF1,     NODE.followerF1));
  scheduler.register(new WalNode(NODE.walF2,     NODE.followerF2));
  scheduler.register(new CompactorNode());

  // Seed the store with each QueueEdge's initial (empty) snapshot so
  // queue badges render `cap N / 0 queued` before the first tick fires.
  scheduler.publishInitialEdgeSnapshots();

  let nextTxId = 1;

  return {
    store,
    scheduler,
    edges,
    dotsLayer,
    sendTx: (order: Order) => {
      const txId = nextTxId++;
      scheduler.inject(NODE.client, { kind: "Propose", txId, order });
      // Drain the client immediately so the client→grpc hop fires
      // without requiring a tick (paused-mode visual).
      scheduler.flushNode(NODE.client);
    },
    resetEngine: () => {
      scheduler.reset();
      nextTxId = 1;
    },
    triggerCompaction: () => {
      scheduler.inject(NODE.compactor, { kind: "Compact" });
    },
  };
}

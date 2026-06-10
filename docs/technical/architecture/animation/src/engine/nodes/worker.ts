import type { Emit, Msg, NodeId } from "../types";
import { Node } from "./base";

// Worker — instantiated for each of the four subscribers (Index Builder,
// Event Sinks, Cold Storage, Sealer). All four share the same trivial
// behavior: count NotifyLogs landings. Real workers would each have
// their own consumer logic; that's Phase 3+.

export interface WorkerState {
  processed:  number;
  highestSeq: number;
}

export class WorkerNode extends Node<WorkerState> {
  readonly id: NodeId;

  constructor(id: NodeId) {
    super();
    this.id = id;
  }

  initialState(): WorkerState {
    return { processed: 0, highestSeq: 0 };
  }

  handle(msg: Msg): { state: WorkerState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      case "NotifyLogs":
        return {
          state: {
            processed:  state.processed + 1,
            highestSeq: Math.max(state.highestSeq, msg.batch.upTo),
          },
          emit: [],
        };
      default:
        return { state, emit: [] };
    }
  }
}

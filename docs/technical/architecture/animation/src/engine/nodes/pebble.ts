import { NODE } from "../topology";
import type { Emit, Entry, Msg, TickCtx } from "../types";
import { Node } from "./base";

// Pebble — durable KV store. The FSM writes a batch here at the end of
// each apply (batch.Commit). We mock keys derived from each applied
// entry — log:{idx} for the entry meta, volumes:{src/dst}:{asset}
// for the audit-trail balances. Real Raft would store actual values
// computed by applyOrder; here we annotate with `tx#{txId} @{tick}`.

const FLASH_TTL = 2;   // ticks (flush invocations) a flash stays lit.

export interface PebbleEntry {
  value:    string;
  flashAge: number;
}

export interface PebbleState {
  writesServed:        number;
  highestPersistedIdx: number;
  // Plain Record for Immer-friendly shallow copying.
  map:                 Record<string, PebbleEntry>;
}

export class PebbleNode extends Node<PebbleState> {
  readonly id = NODE.pebble;

  initialState(): PebbleState {
    return { writesServed: 0, highestPersistedIdx: 0, map: {} };
  }

  // Age the flash flags once per tick when this node had work.
  flush(): { state: PebbleState; emit: Emit[] } {
    const state = this.state;
    return { state: { ...state, map: ageFlash(state.map) }, emit: [] };
  }

  handle(msg: Msg, ctx: TickCtx): { state: PebbleState; emit: Emit[] } {
    const state = this.state;
    switch (msg.kind) {
      // FSM-side: batch.Commit at end of apply. Each Entry produces
      // three keys: the log entry itself + a volume row per endpoint.
      case "WritePebble": {
        let map = state.map;
        for (const entry of msg.batch.entries) {
          map = installEntry(map, entry);
        }
        return {
          state: {
            ...state,
            writesServed:        state.writesServed + 1,
            highestPersistedIdx: Math.max(state.highestPersistedIdx, msg.batch.upTo),
            map,
          },
          emit: [{
            to:  msg.from,
            via: ctx.pathFor(NODE.pebble, msg.from),
            // Pass-through the batch so the ack hop keeps tx colouring.
            msg: { kind: "PebbleAck", batch: msg.batch },
          }],
        };
      }
      // Admission-side: lazy-miss load — pure read.
      case "ReadPebble":
        return {
          state,
          emit: [{
            to:  msg.from,
            via: ctx.pathFor(NODE.pebble, msg.from),
            msg: { kind: "ReadPebbleResp", txId: msg.txId },
          }],
        };
      // Compactor-side: peek highest persisted idx (drives WAL truncate).
      case "ReadPersisted":
        return {
          state,
          emit: [{
            to:  msg.from,
            via: ctx.pathFor(NODE.pebble, msg.from),
            msg: { kind: "ReadPersistedResp", upTo: state.highestPersistedIdx },
          }],
        };
      default:
        return { state, emit: [] };
    }
  }
}

function installEntry(map: Record<string, PebbleEntry>, entry: Entry): Record<string, PebbleEntry> {
  const log = `log:${entry.index}`;
  const src = `volumes:${entry.order.source}:${entry.order.asset}`;
  const dst = `volumes:${entry.order.destination}:${entry.order.asset}`;
  return {
    ...map,
    [log]: { value: `tx#${entry.txId} ${entry.order.source}→${entry.order.destination} ${entry.order.amount}`, flashAge: FLASH_TTL },
    [src]: { value: `@${entry.index}`, flashAge: FLASH_TTL },
    [dst]: { value: `@${entry.index}`, flashAge: FLASH_TTL },
  };
}

function ageFlash(map: Record<string, PebbleEntry>): Record<string, PebbleEntry> {
  const out: Record<string, PebbleEntry> = {};
  for (const [k, e] of Object.entries(map)) {
    out[k] = e.flashAge <= 0 ? e : { ...e, flashAge: e.flashAge - 1 };
  }
  return out;
}

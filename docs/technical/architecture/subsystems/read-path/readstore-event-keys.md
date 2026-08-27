# Read-store event keys

The metadata value index (`0x01`) and the entity-existence index (`0x02`) are
append-only event logs. A mutation never deletes a row: it appends an event
stamped with the raft sequence that produced it, and a reader resolves the
membership that held at its own pinned sequence.

This is what lets a filtered read answer from exactly one committed state.
Everything else in the read path can be aligned by ordering (open the main
handle first, wait for the fold to reach it — see
[read-snapshot-consistency.md](read-snapshot-consistency.md)), but a metadata
value can be *revoked*, and no ordering recovers a row the fold has already
deleted. Only history does.

## Key layout

```
[prefix][ledger 64B][ns][metadataKey\x00][version 4B][encodedValue][entity]\x00[raftSeq 8B BE][op 1B]
```

`op` is `ADD` (`0x01`) or `DEL` (`0x00`). The exists index uses the same shape
with the null flag in place of the encoded value.

Two details are load-bearing:

- **The `\x00` terminator after the entity.** Entities are variable-length, so
  without it the sequence bytes of entity `a` would sort into the events of
  entity `ab`. Entity ids cannot contain NUL (account addresses are
  `[a-zA-Z0-9:_-]+`; transaction ids are their fixed 8-byte encoding).
- **`DEL` sorts before `ADD`.** A same-sequence pair resolves to `ADD`, which
  is what makes a rewrite racing a live dual-write at one sequence harmless.

## Resolution

Events of one `(value, entity)` group are key-adjacent and sequence-ascending,
so a reader resolves in a single forward pass: within a group, the last event
at or below the pin decides — `ADD` means the entity matches, `DEL` or nothing
means it does not. Groups never consult each other, so iteration stays one
ordered scan (`iterator_event_resolve.go`).

A transition away from a value therefore writes into *two* ranges: an `ADD` in
the new value's range and a `DEL` in the old value's own range. Without the
second write, "does E match V at P?" could not be answered from V's range
alone.

```
seq 10:  set  k=red  on E   →  [red][E][10][ADD]
seq 25:  set  k=blue on E   →  [blue][E][25][ADD] + [red][E][25][DEL]
seq 40:  unset k     on E   →  [blue][E][40][DEL]

pin 15 → red        pin 30 → blue (red's own range says DEL@25)        pin 45 → neither
```

## Reclamation

History is not kept forever. `GCEventZone` walks a zone incrementally under
the builder's tick budget and applies one rule per group: every event below
the watermark is droppable **except** the latest, and that one survives only
if it is an `ADD` (a `DEL` decides the same verdict as absence, so the whole
dead group goes). Verdicts at every admissible pin are unchanged by a pass —
that is a test, not a claim.

The watermark is the part that needs care. A pin is read from a main-store
handle *before* it can be registered, so it can be arbitrarily older than the
fold cursor, and a pass that swept at the cursor would reclaim beneath a pin
about to be used. `LeaseRegistry` closes that under one lock
(`read_lease.go`):

- `BeginGC(cursor)` lowers the proposed watermark to the minimum live pin and
  publishes the result as a monotone **reclaim floor**;
- `Acquire(seq)` refuses any pin below that floor, and the reader re-reads its
  handle — which only moves the pin forward — instead of resolving a group
  whose history is already gone.

A reader racing a pass therefore either registers first (and is covered by the
watermark the pass sweeps with) or arrives after (and is refused). Publishing
the floor after the walk would reopen exactly that gap.

## Version activation

A schema rewrite builds a new version's keyspace and stamps **every** event it
writes with the single FSM sequence it read from. A reader pinned below that
sequence would find no event at or below its pin for any entity, and resolve a
fully populated index as empty — indistinguishable from "nothing matches".

`IndexVersionState.ActivationSequence` records the sequence a promoted version
became resolvable at, and `PinnedVersionResolver` reports such a version as not
yet live for pins below it, so the query surfaces `ErrIndexBuilding` instead of
an empty page. A version built by an initial backfill carries no activation
sequence: its events hold the sequences of the logs they were folded from, so
every pin resolves them correctly.

## What still uses plain keys

Add-only limbs — timestamp, inserted_at, address→tx (all roles), reference,
has-asset, reverted_at — keep their plain rows. Membership there is never
revoked, so fold-ahead can only *add* rows, and the per-row keep against the
pinned main handle (`MainHorizonKeep`) restores exactness without a format
change. The reverse map (`0x03`) also keeps prompt deletion: it is builder
bookkeeping, never query-facing, and the checker's reverse-map pass reads it
as-is.

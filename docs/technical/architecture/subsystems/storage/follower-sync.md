# Follower snapshot synchronization

When a Raft follower can no longer catch up from retained log entries, it
restores a Pebble checkpoint from a peer through the snapshot gRPC service.
The protocol is session based so multiple checkpoint files can be transferred
in parallel.

## Protocol

1. `PrepareSnapshot` waits for the serving FSM to reach the requested Raft
   index, then creates a temporary Pebble checkpoint and returns a session ID
   plus a manifest containing only each regular file's relative path and size.
   Identified retries from the same follower share one in-flight preparation
   when its applied-index floor is sufficient; a higher requested floor waits
   for that preparation and creates a fresh checkpoint. Requests without a
   follower identity are never coalesced. All preparations still share one
   global checkpoint slot because Pebble's synchronous `Checkpoint` cannot be
   interrupted once it has started. A server whose own store is behind its
   Raft log (out of sync, fetching a checkpoint, or installing a Raft snapshot)
   refuses with `FailedPrecondition` before waiting: it reaches the requested
   index only after fetching a checkpoint itself. The fetcher treats that code
   as final and the requester moves on to another voter (see [Donor
   Selection](../../data-flows.md#donor-selection)).
2. `FetchFile` streams one manifest file in chunks. The server computes SHA-256
   while reading the file and puts the lowercase hexadecimal digest on the EOF
   response.
3. The follower writes chunks to a `.tmp` file while computing the same digest.
   It requires the exact manifest byte count and a valid, matching SHA-256
   digest before atomically renaming the temporary file.
4. `CloseSession` retires the session. Session expiry provides a backstop when
   the caller disappears. Either event rejects new `FetchFile` calls
   immediately, but an acquired `FetchFile` pins the temporary checkpoint until
   that operation finishes. Service shutdown follows the same rule: it retires
   every session immediately and the last active user removes the checkpoint.

Manifest construction never reads file contents. Its cost therefore scales
with the number of checkpoint files rather than the checkpoint size. The file
contents are read and hashed exactly once, during transfer.

This protocol revision deliberately does not negotiate checksum behavior and
does not support mixed-version snapshot synchronization. Deploy it as a
coordinated, non-rolling upgrade of every node in the cluster: every peer must
run a version that sends and verifies the EOF digest before normal operation
resumes.

## Retries and staging

A transient `FetchFile` failure retries that file within the current session.
Each attempt creates the same `.tmp` path with truncation and initializes fresh
byte-count and SHA-256 state. A regression test delivers a partial first
attempt, injects one transient stream failure, then verifies that the second
attempt produces only the complete file. A new session fetches every manifest
file again: without a content digest in the manifest, a file left by an earlier
checkpoint cannot be trusted from path and size alone.

Transient means `Unavailable`, `Aborted`, `DeadlineExceeded`, `Internal`, and
`Canceled` while the sync's own context is still live. The fetcher talks to the
selected donor over the Raft transport's shared peer connection, which the
transport may restart under it; calls on the closed connection fail with
`Canceled`. The fetcher resolves the client from the transport before every
session and file attempt, so the retry runs on the re-dialed connection. A
`Canceled` caused by the sync's own context (interrupted sync, leader change)
ends the fetch without retrying.

### Donor fallback

The current leader is tried first, followed by the other members in the
committed `ConfState` (with the local node excluded). A donor is selected
lazily so an unavailable peer does not prevent the recovery task from trying
the next candidate.

Fallback happens only between complete sessions. A `session_id`, manifest, and
temporary checkpoint belong to one donor and are never mixed with files from a
different donor. If a session fails, the staging directory and progress
counters are reset before the next candidate is attempted. Transport failures,
expired sessions, and donor-side size/hash/manifest failures are candidate
failures; local staging or restore errors stop the recovery instead of being
masked by another donor.

The fetcher first calls `CheckSnapshotReadiness` with a five-second timeout.
A stale or resynchronizing donor is rejected immediately, without creating a
checkpoint, and the next candidate is tried. A transient probe failure follows
the normal retry/backoff budget; if that budget is exhausted, the donor is
abandoned. Donors that return `Unimplemented` use the compatibility fallback
to `PrepareSnapshot`. That call remains the final eligibility barrier: the
serving FSM must have applied at least the requested snapshot index before the
temporary checkpoint is created. A future protocol revision may expose the
checkpoint index and source generation explicitly, but changing donors within
one session is never valid.

If a caller cancels while its preparation is queued for the global checkpoint
slot, that preparation is discarded. A live retry from the same follower may
replace it; it never inherits the canceled caller's error. Cancellation after
Pebble has entered `Checkpoint` only cancels the RPC wait, because that Pebble
operation is synchronous and does not accept a context.

The final rename is the trust boundary. Truncated content, excess content, a
missing or malformed digest, and a digest mismatch all leave the final path
untouched.

## Filesystem confinement

Paths in `FetchFileRequest` and `SnapshotManifest` cross the peer-network trust
boundary. String normalization alone is not containment: joining a checkpoint
root with `../secret` and calculating the relative path returns the same
traversal string.

The serving RPC rejects non-local paths with `filepath.IsLocal`, then
`streamOneFile` opens the file through an `os.Root` bound to the prepared
checkpoint. The rooted open is the authoritative defense against traversal,
symlink escape, and validation/open races.

The follower applies the symmetric controls. It validates every manifest path
before scheduling downloads, validates again in the per-file fetcher, and uses
an `os.Root` bound to the staging directory for parent creation, temporary-file
creation, cleanup, and final rename. A peer therefore cannot use a manifest
path or a pre-existing staging symlink to write outside the staging directory.

## Manifest path uniqueness

Locality is not sufficient on its own, because downloads run in parallel and
each one writes through `path + ".tmp"` before renaming that name onto its
final path. The follower therefore validates the complete manifest up front and
rejects, on cleaned paths, two classes of collision:

- duplicate entries, which would race on one staging name and one rename;
- any entry whose path equals another entry's staging path, such as `a` and
  `a.tmp`. There the `a.tmp` transfer can rename its own bytes onto the staging
  file the `a` transfer already opened. The `a` transfer then verifies its
  unlinked descriptor and installs the other transfer's bytes as `a`, so both
  transfers report success for content that was never verified against the
  entry it was installed under.

Rejecting the manifest before the first `FetchFile` makes the outcome
independent of transfer interleaving.

Tests must preserve both sides of this boundary: valid root and nested files,
parent traversal, absolute paths, symlink escapes, duplicate manifest paths,
and staging-path collisions. See `docs/technical/contributing/testing.md`.

## WAL reclamation after restore

Installing a received Raft snapshot first replaces the in-memory snapshot
pointer, discards the entire cached entry slice, and raises the in-memory
HardState commit when necessary. It then writes the full snapshot file and a
durable guard snapshot record before buffering the HardState WAL record. A
second identical snapshot record supplies the sync barrier for the HardState.
This ordering makes every durable prefix recoverable: the old HardState ignores
an ahead-of-commit guard record, while any durable new HardState already has a
matching durable snapshot record. After cleaning old snapshot files, the node
drops the in-memory WAL mutex and releases etcd WAL segment locks through the
installed index. A durable-write failure is returned but does not roll back the
earlier in-memory replacement and cache clear.

Every runtime path that creates, updates, or installs a snapshot serializes the
complete snapshot-file and WAL-record persistence sequence. The persistence
lock is acquired before the in-memory WAL lock so snapshots are captured and
made durable in the same order. This prevents concurrent membership updates
from racing on the deterministic `.snap.tmp` name and keeps the final snapshot
file aligned with the latest WAL snapshot record.

Normal maintenance remains an independent reclamation trigger: it creates a
snapshot and calls `Compact`, which releases locks through the compaction index.
Regression coverage must retain both the steady-state `Compact` trigger and the
received-snapshot trigger.

Lock release is best effort: failure is logged but cannot roll back an already
persisted snapshot or completed in-memory compaction. The background WAL purger
removes unlocked segment files.

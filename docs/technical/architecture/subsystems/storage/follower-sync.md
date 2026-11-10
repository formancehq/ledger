# Follower snapshot synchronization

When a Raft follower can no longer catch up from retained log entries, it
restores a Pebble checkpoint from a peer through the snapshot gRPC service.
The protocol is session based so multiple checkpoint files can be transferred
in parallel.

## Protocol

1. `PrepareSnapshot` waits for the serving FSM to reach the requested Raft
   index, creates a temporary Pebble checkpoint, and returns a session ID plus
   a manifest containing only each regular file's relative path and size. A
   server whose own store is behind its Raft log (out of sync, fetching a
   checkpoint, or installing a Raft snapshot) refuses with
   `FailedPrecondition` before waiting: it reaches the requested index only
   after fetching a checkpoint itself. The fetcher treats that code as final
   and the requester moves on to another voter (see
   [Donor Selection](../../data-flows.md#donor-selection)).
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

A session retry against the same donor first empties the staging directory and
resets the progress counters, exactly like donor fallback. Re-fetching the
manifest files is not enough: the donor may have rebuilt its checkpoint with a
different file set (for example after a restart that flushed its WAL into a new
SST), and any file the new manifest does not list would survive. Pebble would
then open a mix of both checkpoints. Both sessions ship a
`marker.manifest.000001.*` file. With equal iterations, Pebble keeps the first
marker in the unsorted directory listing, so it can pick the old `MANIFEST`. It
then ignores the new SSTs and replays the old WAL together with the new one.
The result is an internally inconsistent store: a committed range of entries is
missing, and only the keys a later entry rewrote are current. The Antithesis
idempotence run `9fda11eff3a174b358e484f62b63021b-64-4` produced
`:divergent-records` this way. A regression test stages a failed first session
and checks that only the second session's files remain.

After a session's downloads finish, the fetcher also checks that staging holds
exactly the manifest's files and their parent directories. The check walks a
root-scoped view of staging and rejects any other file, directory, symlink or
leftover `.tmp`, and any missing manifest file. A failure here is a local
staging fault, not a donor fault: it stops the recovery instead of moving to
another donor, and the next sync starts from freshly prepared staging.

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

After Raft accepts a received snapshot, the node skips the ordinary Ready
append to the old WAL. `ApplySnapshot` writes the full
snapshot file. It prepares and syncs `etcd.next/` with the snapshot record,
HardState, and only the Ready entries after the snapshot index. The old WAL
remains the recovery source until the directory switch begins. Before the
switch, the node stops the old WAL purger and closes the old WAL while its
original path still names it. The node then renames the old directory to
`etcd.previous/` and the candidate to `etcd/`, syncing the parent directory
after each rename.

The node opens the selected WAL and reads back its HardState and entry suffix.
Only after validating both does it replace the in-memory snapshot, HardState,
and cached entries. It then removes older snapshot files and inactive WAL
directories. A failure before the first rename leaves the in-memory state
unchanged, although the old WAL may already be closed. A failure during or
after the switch stops Ready processing. Restart selects a complete WAL from
the durable directories. No received-snapshot path calls `ReleaseLockTo` on
the replaced WAL.

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

Lock release during normal `Compact` is best effort: failure is logged but
cannot roll back an already persisted snapshot or completed in-memory
compaction. The background WAL purger removes unlocked segment files.

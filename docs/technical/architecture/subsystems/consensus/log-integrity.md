# Cross-replica log integrity observation

## Need and boundary

An audit-chain hash binds the committed order, while the persisted global `Log`
records the result produced by FSM application. Equal audit hashes therefore
do not by themselves show that every node produced the same result. Operators
need a prompt signal when replicas persist different results at the same global
log sequence. The on-demand checker remains the authority for comparing the
implemented audit-derived projections with audited history; this observer provides a narrower,
continuous cross-replica signal.

The mechanism adds no persisted field, hash chain, Raft entry, or client API.
Existing snapshots and backups remain readable. It reads existing primary
Pebble log rows (`ZoneHistory/SubHistoryLog`) and runs outside admission and FSM
apply. Its cost is proportional to the number of logs inspected and peers
compared, not to writes on the request path.

## Digest and comparison

`internal/application/logintegrity.DigestRange` computes XXH3-128 over a versioned
domain tag, the inclusive first/last sequences, then every sequence, length,
and deterministic protobuf encoding of its stored `Log`. The key sequence must
equal the value sequence; holes and malformed rows fail instead of producing
a digest of a shorter range. Protobuf map keys are serialized deterministically.
`response_signature` is normally added only to a returned copy; if it is
present in Pebble, it is included in the digest like every other persisted
field. Digest version 1 is fixed by a golden test. Changing which
fields or encoding contribute requires a new version.
The unkeyed digest is a fast comparison checksum for accidental divergence
detection, not independent tamper evidence. It is deliberately non-cryptographic;
the audit chain and checker remain the integrity authorities.

The internal Raft listener serves `LogIntegrityService.GetRangeDigest` to
members using the existing inter-node authentication and connection pool. A
request is limited to 4096 logs and carries the requester's **durable applied
Raft index**, sampled with its log head from one Pebble snapshot. The receiver
waits for that index, opens its own pinned snapshot, verifies the snapshot's
applied marker, and hashes the requested range. A follower that has not caught
up returns a timeout or unavailable result, not a divergence. At the required
horizon, an absent log or malformed row is reported as data loss.

Only the current leader coordinates comparisons. It samples a coherent local
head and checks each member in parallel every 250 ms. Each peer has its own
cursor; an unavailable range stays queued for a later attempt, while a
confirmed discrepancy is reported and the cursor advances so later results
remain observable. The
first observation after leadership starts covers the newest 256 logs, then
new logs are checked in bounded ranges. This favors prompt detection for new
results; it does **not** retrospectively verify older history. A future
background scrub must cover that separate requirement. A newly joined peer is
similarly first compared over the recent window.

## Operational interpretation

`log_integrity.divergences` counts newly observed discrepancies by peer.
`log_integrity.local_data_loss` counts local range checks that cannot be
decoded or are incomplete at the sampled horizon.
The error log includes peer ID, range, applied index, and both digests for a
digest mismatch. After that first alarm, the observer bisects the range using
the remaining RPC deadline and logs the first differing sequence when it can
locate one. `log_integrity.unavailable_attempts` counts comparisons that
could not be completed (including lag, network failure, and an old peer without
the RPC), labeled by peer and gRPC status or connection reason.
`log_integrity.checked_logs` counts successful peer-log
comparisons; `log_integrity.comparison.duration` measures the time from
local hashing through the successful peer response. Alert on divergence;
investigate sustained unavailable counts as a loss of coverage. The intended
latency target is under 500 ms p99 **after all compared nodes have applied the
sampled index**; replication lag is outside that target and appears as an
incomplete comparison. That target requires representative cluster measurement
before it can be claimed as achieved. A synthetic in-memory Pebble benchmark on
Apple M5 Pro hashes 256 small logs in about 0.10 ms and 4096 in about 1.4 ms;
this is evidence for the CPU cost shape, not cluster latency.

A mismatch is evidence that replicas disagree, not proof of which replica is
correct. With two replicas there is no majority to identify the faulty one.
Alert on local data loss as well. Do not automatically quarantine or repair a
node based on this observer.
Inspect the audit chain and run the full checker to qualify the incident. An
identical wrong result on every replica, corruption of another projection,
rejected proposals or technical entries that produce no `Log`, and history
outside the observed window remain outside its detection scope.

Rolling upgrades are compatible: an older peer returns `Unimplemented` for the
new internal RPC and counts as unavailable. Its cursor remains queued; once
the peer is upgraded, the leader resumes comparisons from that cursor. A
digest-version mismatch is also unavailable, rather than falsely treated as
corruption. The frozen public service protocol revision is unaffected.

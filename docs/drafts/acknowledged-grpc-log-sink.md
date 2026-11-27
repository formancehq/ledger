# Acknowledged gRPC log sink (design draft)

Status: implementation in progress in PR #2662. Principal binding is selected;
the initial cursor remains open. The current code starts at the beginning of
the configured ledger's history.

## Need and scope

A client reads one ledger with `BucketService.ListLogs` and builds events from its logs. Polling adds latency. The requested alternative is a client-initiated, long-lived gRPC stream that delivers the same `common.Log` messages and retains a durable server-side cursor only after explicit client acknowledgement. The commercial impact, target latency, expected throughput, and deployment date are still unknown.

One configured subscription identifies one `(ledger, client_id)` pair and an authenticated owner principal, and has a unique sink name. Each creation receives a fresh, immutable incarnation ID persisted with the configuration; recreating a removed name never reuses its previous incarnation. Operators create and remove subscriptions through the existing sink lifecycle. Connecting with an unconfigured name fails without creating persisted state. Configuration is separate from a connection: disconnecting does not delete the cursor. A client ID is an identity label, not a credential; the bearer token must have the ledger-read authorization required by the stream.

The stream sends only the per-ledger Apply logs returned by `ListLogs`, in increasing ledger-local `LedgerLog.id` order. It carries the unchanged `common.Log` envelope. The current implementation starts at the beginning on first connection. A head-start option remains a product decision and would require an explicit stored creation-time position; it cannot be silently inferred from an absent cursor. No event transformation or event-type filtering occurs.

## Candidate wire contract

Add one bidirectional `BucketService.SubscribeLedgerLogs` RPC under protocol revision 40. The first client message is `Open { sink_name, client_id }`; subsequent client messages are `Ack { log_id }`. Server messages are `Opened { acknowledged_log_id, cluster_id }`, `Log { common.Log }`, and `Acked { log_id }`. No existing RPC or field changes meaning. The stream is bound to the ledger in the stored sink configuration. An `Ack` refers to `payload.apply.log.id`, not `common.Log.sequence` or the global event-sink sequence.

The server may send at most a configured number of unacknowledged logs. It validates that an ack is monotone, no greater than the highest log sent on this connection, and belongs to the configured ledger. An ack of the already durable cursor is idempotent. An ack for a later log is proposed as a Raft technical update carrying the stream's persisted incarnation ID; the server sends an `Acked { log_id }` response only after successful apply. Repeated or cumulative acks can be coalesced before proposing, but the server must never acknowledge an uncommitted cursor. The client should retain its own last processed ID until it receives `Acked`.

The server reads committed logs through the aligned `ListLogs` read path, first catching up from the persisted cursor and then rescanning every 250 ms from the last sent ID. Each read is bounded by the unacknowledged-message window and an 8 MiB page budget (or one larger log). A commit between an empty read and the next rescan remains visible because the cursor never advances on an empty read. The server retains no unbounded queue. A post-commit notification fan-out could replace the periodic rescan if the measured read load or latency warrants it.

Only one active connection may own a configured sink name. A second connection receives `AlreadyExists`. On leadership loss or shutdown, the stream ends with `Unavailable`; the client reconnects to the new leader. The next leader resumes from the last durably acknowledged ID, so logs sent but not acknowledged may repeat. Delivery is at least once. The client must process idempotently using `(source cluster identity, ledger, ledger-local log_id)`; a transport send is not an acknowledgement.

## Persisted state and RC rollout

The existing event-sink cursor is a global sequence keyed by sink name. Reusing it as a ledger-local ID would change its meaning and break existing sinks. The new subscription needs a distinct, versioned cursor key scoped to its incarnation and a Raft technical update whose apply is deterministic and monotone. At apply, every node checks the update's incarnation against the current committed configuration through declared coverage and preload; a missing subscription or mismatch rejects the update before any cursor write. This check is authoritative even if the originating leader observed the old configuration before a concurrent removal or recreation. Configuration, cursor, and deletion must be classified for checkpoint backup and restore. On cross-cluster restore, preserve the cursor only when the same subscription and log history are restored; document the source cluster identity exposed to clients.

Adding a new `SinkConfig` oneof arm is additive on the service wire but also changes a persisted message. An older RC voter or learner may decode the new arm as unknown and cannot be assumed to apply a new cursor update identically. This feature uses the repository's coordinated stopped upgrade: stop and drain every voter and learner, retain their persisted state, upgrade the complete cluster, restart it, and require committed capability support evidence from every current voter and learner before activating admission of the new sink type or acknowledgement updates. Membership changes remain fenced by the active capability. Replay of a non-empty pre-upgrade log on the upgraded binary must preserve the old interpretation. A partially upgraded cluster cannot admit these entries. Neither a protocol-version bump nor a protobuf reservation solves this.

Adding an inert configured sink must not cause the existing events Manager to start an outbound emitter. Removal invalidates its active stream and makes later acks fail; the stream ends with `Unavailable` when it observes the removal. A same-name recreation starts with its own cursor and incarnation, regardless of whether old cursor records are retained for cleanup. The apply-time incarnation check also fences an ack already submitted before the stream closes but ordered after recreation. Registration records the ledger's numeric ID in the audited creation log; if that ledger is removed and its name reused, the old sink refuses the new ledger until recreated.

## Security and resource limits

The stream requires the existing ledger-read scope and checks the configured ledger. Registration and removal retain their existing write permission. At registration, an operator must set `grpc_log.owner` to the intended authenticated `CallerIdentity`: OIDC issuer and subject, or static key ID and subject. The owner is part of the audited configuration and cannot be supplied at Open. At connection, reject a different client ID or principal; auth-disabled and anonymous callers cannot connect. Changing the owner requires removing and recreating the sink, which also resets its cursor. A client ID remains a label, not a credential.

Bound active and pending streams globally and active streams per principal, unacknowledged messages per stream, catch-up read size, and time without ack. The current limits are 8 streams overall, including streams that have not sent Open, 2 active streams per token principal, 16 unacknowledged IDs per stream, an 8 MiB page budget (or one larger log), 10 seconds to send Open, 30 seconds without an ack while logs are outstanding, and a 15-minute maximum lifetime capped at token expiry. The per-principal resource limit does not decide whether that principal owns the configured `client_id`. Do not impose an individual log limit below the size of any `common.Log` that `ListLogs` can deliver: the stream transport allowance includes the protobuf wrapper around the full log. If the transport cannot carry a historical `ListLogs`-deliverable log, the implementation needs lossless chunking before launch; closing and retrying at the same cursor would permanently block later logs. When another limit is reached, close with a documented retryable status; do not discard or advance the durable cursor.

## Validation before a functional PR is ready

- Create a configured subscription, connect, catch up, acknowledge, disconnect, and resume from the durable cursor.
- Repeat after leadership transfer, shutdown, and restart; verify no lost log and allowed redelivery.
- Remove and recreate a sink under the same name, then apply an in-flight ack from the old stream after recreation; verify that the old update is rejected and the new cursor stays at its initial position.
- Interleave catch-up and commit to prove the read/wait handoff.
- Reject out-of-order, future, wrong-ledger, wrong-client, and unauthenticated acknowledgements.
- Exercise slow clients, connection limits, duplicate connections, cancellation, and token renewal.
- Prove old persisted data remains readable and validate mixed-version RC apply and checkpoint backup/restore.
- Verify activation remains blocked with upgraded voters and one older learner; run the coordinated stopped upgrade with retained state and non-empty replay.
- Catch up through a historical log at the existing `ListLogs` message-size boundary followed by another log; verify both can be acknowledged in order.
- Run `just generate-proto` immediately after any proto edit, `bash scripts/agent-check`, targeted suites, and `agent-check-pr` against the exact base SHA.

## Product decisions still needed

1. Exact target latency, throughput, number of subscriptions, and impact if polling remains.
2. Whether a second connection rejects or takes over an existing session.
3. Initial position: full history or current head, chosen explicitly at registration.
4. Retention/deletion policy for inactive subscriptions.

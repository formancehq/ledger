# Sensitive data exposure audit evidence contract

The [manifest](sensitive-data-exposure-boundaries.json) defines a reusable P1
audit of where secret-bearing values may leave their intended boundary. It owns
confidentiality of representations and propagation paths across public reads,
diagnostics, observability, exports and operator surfaces. It does not decide
who may call an operation, whether a path is root-confined, or whether a
downstream system is trustworthy.

## Preparation and execution gate

This change prepares the domain only. **Do not run the product audit as part of
manifest preparation or merely because this PR merges.** Pull requests #1963
and #1977 currently reshape the sensitive read-projection and sink-diagnostic
surfaces. After that stack and its dependencies stabilize, refresh the field,
route, RPC, codec, factory and test inventory against the new exact clean HEAD,
update stale paths and deduplication context, and obtain a separate execution
decision.

At execution time, the trusted outer workflow runs `bash scripts/ai-audit
sensitive-data-exposure-boundaries`, then independently qualifies the raw result
at the same HEAD with `bash scripts/ai-audit-challenge <audit-result>`. Providers
remain read-only leaf workers. They do not launch either workflow, create Jira
issues, change code or publish credentials. Use only synthetic canaries; real
secrets and deployed services are outside this contract.

## Evidence required

Every finding must establish the complete chain:

1. The exact sensitive value class and a unique synthetic representative.
2. Its reachable production ingress or persisted source.
3. Every transformation, alias, wrapper and codec between source and output.
4. The registered public, diagnostic, telemetry, artifact or operator surface.
5. The observer that receives the raw value and why that observer is outside the
   value's intended boundary.
6. The emitted bytes or structured fields containing the raw or reversibly
   encoded canary, paired with a non-sensitive control that proves the intended
   path executed.
7. The invariant and owning root cause, including deduplication against active
   work and neighboring audit domains.
8. A focused reproduction plan and the exact existing coverage gap.

A sensitive-looking field name, a broad manifest glob, an object implementing
`Stringer`, a missing helper call, or an upstream library error that might
contain credentials is not a finding. A test that receives no output also does
not prove redaction; retain a harmless marker to demonstrate that the same
representation path executed. P0/P1/P2 findings require a concrete reachable
path and observable bytes or structured values. When sensitivity or intended
visibility is undocumented, emit an audit question.

## Source-backed contracts

These anchors describe the preparation base. Reconcile them with code, tests,
generated descriptors and authoritative documentation at the audited SHA.

| Contract | Established rule and source anchors | Limits |
| --- | --- | --- |
| A — Sensitivity taxonomy | Secrets include bearer/PAT tokens, OAuth client secrets, SASL/database/object-store passwords, webhook and cluster shared secrets, private signing/TLS key material, and credential-bearing URL components. `misc/proto/common.proto`, `internal/bootstrap/config.go`, sink/mirror/backup configs and operator `Secret` wiring provide concrete fields. | Names, endpoints, key IDs, public keys, certificate chains, secret references and configured-presence state are not automatically secret. Generic PII discovery and arbitrary business payload classification require a separate policy. |
| B — Boundary and reachability | A disclosure exists only when a real registration, error path, serializer, exporter, artifact writer or reconciler moves the value to an observer outside its intended boundary. Follow registrations in HTTP/gRPC bootstrap, sink factories, telemetry modules, CLI commands and operator controllers. | In-memory possession by the component that needs a credential is not exposure. Do not claim that reflection or debug formatting is reachable without a production caller. |
| C — Public projection | API docs define public error sanitization; handlers and service RPCs return ledger, sink, mirror, audit and log representations. The active projection stack is expected to centralize secret removal before wire encoding. Inventory get/list, HTTP/gRPC, unary/streaming and local/forwarded paths separately. | Authorization is owned by `authentication-authorization-boundaries`. API field presence, numbering and HTTP/gRPC parity unrelated to secrecy are owned by `api-boundary-contracts`. An authorized caller still must not receive fields the public contract redacts. |
| D — Diagnostics and telemetry | `http-api.md` permits raw causes in correlated server diagnostics, but this is not permission to record credentials. `internal/adapter/apitrace`, HTTP recovery/error handling, gRPC encoding, sink failure state, logger adapters and `internal/infra/monitoring` are the propagation anchors. | Useful error context and correlation identifiers should remain. Generic log quality, cardinality and observability completeness are outside scope unless they create a confidentiality path. |
| E — Authoritative versus projected data | Audit entries and ledger logs are chain-bound business truth; events and mirror configuration may derive from the authoritative form. Read projections must be detached and must not mutate signed, hashed, persisted, replayed or subsequently emitted objects. Use deterministic audit bytes/hash and later-consumer observations as controls. | Audit/checker soundness, event acknowledgement/cursor behavior and mirror source equivalence retain their existing owners unless the failure is caused by redaction or disclosure. |
| F — Backup and export | Backup documentation defines checkpoints, exported deltas and restore metadata. Inspect whether broad configuration or wrapped errors introduce runtime credentials into artifacts, manifests or status surfaces, and distinguish this from explicitly durable business configuration. | Restore parity belongs to `persistence-restore-replay`; lexical and final-operation path containment belongs to `filesystem-confinement-contracts`. Do not turn content inspection into archive traversal testing. |
| G — Operator secret surfaces | Kubernetes `Secret` objects and `kubectl ledger credentials get-key` intentionally carry secret bytes. Ordinary CR spec/status, Events, Conditions, ConfigMaps, pod metadata, controller logs and general display commands should expose only references, public material, hashes with a justified internal purpose or documented presence markers. | Whether a Kubernetes identity may read a Secret is an RBAC/auth question. Secret creation, rotation and reconciliation durability remain operator/auth/configuration concerns unless they emit the value to an unintended surface. |
| H — Completeness and non-reversibility | The preparation base's `Config.redactedCopy`, JSON/YAML marshalers and redaction regression tests demonstrate value/pointer and absent/present requirements. The same deny-by-default principle must cover every registered variant and actual encoder. | Omission, a fixed placeholder or a presence boolean may be valid. Secret-dependent public lengths, prefixes, encodings or guessable digests are disclosures when they materially reveal the value; internal rollout hashes are not public output by default. |
| I — Root-cause ownership | Assign the finding to the earliest component that wrongly serializes, retains in public state or passes the secret into an unsafe diagnostic. Later transports are evidence, not duplicate causes. | Auth decisions, filesystem containment, general API parity and event delivery have dedicated manifests. Cross-domain paths are valid evidence but must not multiply one defect into several findings. |

### Operator backup credential oracle (EN-2060)

`BackupDestination.s3AccessKeyIdFrom` and `s3SecretAccessKeyFrom` carry only
same-namespace Secret name/key references. `buildBackupJob` emits non-optional
`secretKeyRef` environment entries. Exercise full and incremental reconciliation
and JSON/YAML serialization of Backup, BackupRun, Job and Pod template with
separate synthetic key canaries and a retained bucket control. Require no raw
or serialized/encoded credentials in these resources or S3 command arguments.
Then exercise the production `ledgerctl` environment binder and storage protobuf
builder to prove the credential reaches its intended RPC input. Omitted references
must preserve the server's default AWS credential chain. Fake clients prove the
rendered Pod template, not kubelet resolution or actual Pod propagation.

## Surface inventory required at execution

Before testing, build a matrix with one row per sensitive field and columns for
source, authoritative storage, safe projection, encoders, registered outputs,
diagnostic failures, telemetry, backup/export, CLI and operator rendering. Derive
the rows from current protobuf descriptors and Go structs rather than this
document's historical examples. Derive outputs from router/RPC registration,
factory registries and command wiring rather than filenames alone.

Use a different high-entropy canary for each field. Search emitted raw bytes and
the common reversible encodings actually used by the path (URL escaping,
base64, hex and structured JSON/YAML). Do not spray arbitrary encodings and call
coincidental substrings evidence. An output must also retain a harmless field or
marker proving that the intended path ran.

The matrix must include at least:

- cluster/inter-node authentication and response-signing configuration;
- HTTP, Kafka, NATS, ClickHouse and Databricks sink credentials and failures;
- HTTP/PostgreSQL mirror credentials and projections;
- S3/Azure backup configuration, artifacts and errors;
- ledger, sink, mirror, audit and log get/list representations;
- HTTP/gRPC public errors, correlated logs and span error recording;
- startup/debug config rendering, OTLP, metrics, profiling and flight recorder;
- ledgerctl profiles/config exports/errors and operator CR/status/Event/log paths;
- explicit intended secret outputs as negative controls.

### Pyroscope operator credential oracle (EN-2061)

`Cluster.spec.monitoring.pyroscope` accepts optional `authTokenFrom` and
`basicAuthPasswordFrom` Secret references, never literal credentials. Exercise
neither, either and both references with profiling enabled and disabled. Inspect
serialized Cluster and StatefulSet/Pod templates in JSON and YAML for credential
canary absence, exact required `secretKeyRef` delivery, and non-secret profiling
controls. Inspect both generated and Helm CRDs for removal of literal credential
fields. Reference changes must alter the Pod template without mutating the
source Cluster. Secret content rotation requires an explicit Pod restart; the
operator does not read these secrets or automatically roll on content changes.
The focused oracle lives in `misc/operator/internal/controller/pyroscope_test.go`.

## Rejection and deduplication rules

Reject or downgrade a hypothesis when:

- no registered production path reaches the formatter or observer;
- only a test helper, `%+v` thought experiment or unused codec exposes the value;
- the observed field is a public key, identifier, reference or documented
  presence marker rather than secret material;
- output is the explicit secret retrieval surface being invoked for that purpose;
- the canary appears only in the controlled downstream request that must carry
  it, not in logs, errors, telemetry, responses or artifacts;
- the claim is solely that an unauthorized caller can access an otherwise valid
  representation, or solely that an untrusted path escapes a filesystem root;
- active or landed work already fixes the identical root cause and no distinct
  reachable surface remains at the audited SHA.

Do not reject a disclosure merely because the observer is an operator, a server
log or a trace backend. Those are separate trust domains and often have broader
retention and readership than the source credential. Conversely, do not require
all diagnostics to become empty: preserve non-sensitive operational context and
prove the specific secret is absent.

## Severity guidance

- **P0:** a broadly reachable disclosure of production credentials enabling
  immediate cross-tenant or cluster compromise, with a demonstrated path.
- **P1:** raw reusable credentials or private key material exposed through a
  normal public/operator/diagnostic surface, or included in durable broadly
  distributed artifacts.
- **P2:** a narrower authenticated, failure-only or environment-constrained
  disclosure with concrete useful secret material.
- **P3:** low-impact metadata leakage that still violates an explicit contract.

Missing tests, imperfect abstraction and speculative library behavior are not
severity-bearing findings. Record residual risk when a tagged integration or
external library path cannot be executed, and keep the reproduction plan exact.

## Domain boundaries

The owning audit is determined by the violated oracle:

| Question | Owning domain |
| --- | --- |
| Can this caller enter the route/RPC or obtain this scope? | `authentication-authorization-boundaries` |
| Does a permitted representation expose secret-bearing content? | `sensitive-data-exposure-boundaries` |
| Does HTTP/gRPC/CLI preserve non-secret shape, presence or error semantics? | `api-boundary-contracts` |
| Does a path/archive/peer-controlled name escape its allowed filesystem root? | `filesystem-confinement-contracts` |
| Is an event acknowledged, retried or cursor-advanced correctly? | `event-delivery-contracts` |
| Does backup/restore/replay reproduce the authoritative state? | `persistence-restore-replay` |
| Does the operator converge secret references and rotations durably? | `operator-reconciliation-durability` or `configuration-startup-contracts` |

When one failure crosses several rows, report one root cause here only if
confidentiality is violated. Cite the neighboring effect as impact or evidence
and let the challenge pass reject duplicate ownership.

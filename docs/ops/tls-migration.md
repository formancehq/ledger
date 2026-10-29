# TLS migration

This page describes how to enable or disable inter-node TLS on a running
Raft cluster managed by the Formance ledger operator, without downtime.

## User-facing model

The `Cluster` CRD exposes a single boolean:

```yaml
spec:
  tls:
    enabled: true            # or false
    secretName: my-tls-certs # retained while disabling TLS
    caSecretKey: ca.crt      # optional
```

To enable or disable TLS you flip `tls.enabled`. Removing `spec.tls` also
requests disabled mode. The operator handles the rollout. Keep the referenced
certificate Secret available until the disabled rollout completes, even when
removing the TLS configuration from the desired spec.

## What happens under the hood

The ledger server has three internal TLS modes, exposed to the binary via
the `--tls-mode` flag and the `TLS_MODE` environment variable:

| Mode       | Server listener                   | Client dialer                   |
|------------|-----------------------------------|---------------------------------|
| `disabled` | Plaintext only                    | Plaintext only                  |
| `optional` | TLS + plaintext (via cmux)        | Probe TLS, fall back to plaintext per peer |
| `required` | TLS only                          | TLS only                        |

The CRD does **not** expose `optional`. The operator uses it as a
transitional state during a TLS toggle.

### Toggle flow (enable: `disabled` → `required`)

1. User edits the CR: `tls.enabled: true`.
2. The operator detects the transition, sets `TLS_MODE=optional` on the
   StatefulSet pod template, and creates the `cluster-secret` Secret.
3. The StatefulSet rolling update walks pods through `optional`. Pods
   that have been updated already speak TLS to each other and still accept
   plaintext from peers that haven't been updated yet.
4. Once the rollout converges (all pods updated and ready), the operator
   sets `TLS_MODE=required` and triggers a second rolling update. Every
   peer already accepts TLS, so this step is safe.

`status.tlsMigrationPhase` reflects progress:

| Phase                            | Meaning                                          |
|----------------------------------|--------------------------------------------------|
| `disabled`                       | Disabled template selected; rollout may still be in progress |
| `transitioning-to-required`      | Phase 1 of an enable: rolling out `optional`     |
| `required`                       | Required template selected; rollout may still be in progress |
| `transitioning-to-disabled`      | Phase 1 of a disable: rolling out `optional`     |

You can watch the migration with:

```
kubectl get cluster my-ledger -w \
  -o jsonpath='{.status.tlsMigrationPhase}{"\n"}'
```

### Toggle flow (disable: `required` → `disabled`)

1. User sets `tls.enabled: false` (with or without `secretName`) or removes
   `spec.tls`. External authentication must be disabled before TLS can be
   disabled; `auth.enabled: true` still requires TLS.
2. Before changing or deleting the StatefulSet, the operator persists
   `status.tlsRollout` with the rollout mode, certificate Secret name and CA
   key. Existing installations recover these references from the live
   StatefulSet. A failed status write stops the workload change.
3. The operator renders `optional` with those retained references, including
   the certificate mount and server/client CA configuration. It leaves the
   desired spec unchanged. Old TLS-only peers can communicate with optional
   peers throughout this first rolling update.
4. After optional rollout convergence, the operator persists the next rollout
   mode and updates the template to `disabled`, removing its certificate mount and
   certificate/CA environment. Once the template no longer references the
   operator-managed `cluster-secret`, that Secret can be deleted. This is
   separate from the user-owned certificate Secret, which the operator never
   deletes.
5. The operator retains `status.tlsRollout` until the disabled template has
   converged, then clears it. Convergence requires the StatefulSet to have
   observed its current generation and all desired replicas to be updated
   and ready. `status.tlsMigrationPhase: disabled` indicates the selected
   template mode; by itself it does not prove rollout completion.

If reconciliation or the operator stops after a status write, a replacement
operator resumes from the persisted rollout state. If StatefulSet recreation
is interrupted after orphan deletion, the missing StatefulSet is reconstructed
using the retained mode and references rather than treated as a new plaintext
cluster. A failure after the workload update leaves retained recovery state
available for the next reconciliation. Status stores references only, never
certificate or private-key bytes.

The certificate Secret remains user-owned. Removing it during the transition
can prevent replacement optional-mode pods from starting; restore that Secret
and let reconciliation continue. Remove it yourself only after the disabled
rollout has converged and `status.tlsRollout` has been cleared.

### In-pod ledgerctl during workload recreation

Ledger and EventSink reconciliation read the current StatefulSet TLS mode
before executing ledgerctl. If the StatefulSet is temporarily absent during
orphan/recreate, they return a lookup error and retry through their normal
reconciliation paths without executing a command. Surviving pods may still
require TLS, so absence must not select plaintext. Once the StatefulSet
returns, the next attempt reads its current TLS mode.

### Backup Jobs during migration

When provisioning a new backup Job, the operator reads TLS mode and
certificate/CA references together from the current StatefulSet template owned
by the Cluster. Desired `spec.tls` can already be absent while that template
still requires TLS. The Job therefore retains the live template's trust
configuration instead of falling back to plaintext or losing its CA mount.
If the StatefulSet is temporarily absent during recreation, Job provisioning
returns a retryable error and resumes once the owned workload is available.
A same-name workload owned by another Cluster is rejected. This template
snapshot selects transport configuration; it does not prove every Pod has
finished rolling. An already persisted backup Job keeps its original template.

### Cluster secret coupling

The cluster secret is a static bearer token used for inter-node
authentication. It is independent of `--auth-enabled`: when set, it protects
the Raft port on every node. When `--auth-enabled` is also set, followers use
it to forward writes to the leader. The operator creates it only when TLS is
at least partially active (`mode != disabled`). Sending a
static token over plaintext is an anti-pattern, so the secret only exists
when the wire is encrypted.

Operator maintenance commands use an Ed25519 credential from the `Credentials`
CRD. The private seed remains in the Credentials Secret and the public key is
mounted into Ledger through `AUTH_ED25519_KEYS`; the operator signs short-lived
JWTs for `ledgerctl` and backup Jobs. The Raft `cluster-secret` is never exposed
as a direct client token.

This means **authentication requires TLS**. Note the two guards differ in
strictness:

- `--cluster-secret` (inter-node static bearer token) requires `mode != disabled`,
  i.e. `optional` or `required` — it must tolerate `optional` because the
  operator drives the migration *through* that transitional mode.
- `--auth-enabled` (external service API JWT/Ed25519) requires `--tls-mode=required`.
  `optional` is rejected because its dual listener still accepts plaintext client
  connections, so bearer tokens could travel in the clear. Bring the cluster to
  `required` before enabling `--auth-enabled`.

The server enforces these invariants at startup:

```
--cluster-secret requires TLS (set --tls-mode to optional or required
and provide --tls-cert-file / --tls-key-file)

--auth-enabled requires --tls-mode=required (with --tls-cert-file /
--tls-key-file)
```

Because `--auth-enabled` demands `required`, the operator coordinates auth
with the TLS state machine so a single CR edit that turns on both TLS and
auth does not deadlock:

- If you set `auth.enabled: true` while `tls.enabled` is false, the operator
  **rejects** the CR (it would otherwise reconcile into a silently
  unauthenticated cluster). Enable TLS first.
- When both are enabled together on an existing plaintext cluster, the
  operator holds auth **off** while the StatefulSet rolls through the
  transitional `optional` mode, then enables auth once TLS has converged to
  `required`. The API is briefly unauthenticated during that window — no
  worse than the plaintext state you are migrating away from.

## Direct (non-operator) deployments

When running the ledger binary directly (no operator), use:

```
ledger-server run \
  --tls-mode required \
  --tls-cert-file /etc/tls/tls.crt \
  --tls-key-file /etc/tls/tls.key \
  --tls-ca-cert-file /etc/tls/ca.crt
```

To migrate a hand-managed cluster:

1. Distribute certs to every node.
2. Restart every node with `--tls-mode=optional`. Order does not matter —
   in this mode every node accepts both TLS and plaintext.
3. Once all nodes are running `optional`, restart them with
   `--tls-mode=required`.

The unauthenticated dialer probes each peer the first time it connects in
`optional` mode and re-probes on persistent connection failures, so a peer
flipping between modes during the migration is followed automatically. A pool
configured with a cluster secret refuses optional/plaintext transport: the
secret is sent only after strict TLS has converged. Authenticated deployments
must therefore complete the transition before enabling peer credentials.

## Troubleshooting

- `--cluster-secret requires TLS` at server startup: the cluster secret
  must not be set when TLS is disabled. Either enable TLS or remove the
  flag.
- A migration that appears stuck on `transitioning-to-*` usually means the
  rolling update itself is stuck on a pod readiness probe. Check
  `kubectl rollout status statefulset/<name>`.
- The dialer logs `grpc_connection_created` events with `tls=true|false`
  per peer (via the Antithesis lifecycle SDK) — useful for confirming
  which security level each connection settled on in the optional phase.

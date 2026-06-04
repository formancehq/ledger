# TLS migration

This page describes how to enable or disable inter-node TLS on a running
Raft cluster managed by the Formance ledger operator, without downtime.

## User-facing model

The `LedgerService` CRD exposes a single boolean:

```yaml
spec:
  tls:
    enabled: true            # or false
    secretName: my-tls-certs # only used when enabled is true
    caSecretKey: ca.crt      # optional
```

To enable or disable TLS you flip `tls.enabled`. The operator handles the
rest.

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
| `disabled`                       | Cluster runs plaintext, no migration in flight   |
| `transitioning-to-required`      | Phase 1 of an enable: rolling out `optional`     |
| `required`                       | Cluster runs TLS only                            |
| `transitioning-to-disabled`      | Phase 1 of a disable: rolling out `optional`     |

You can watch the migration with:

```
kubectl get ledgerservice my-ledger -w \
  -o jsonpath='{.status.tlsMigrationPhase}{"\n"}'
```

### Toggle flow (disable: `required` → `disabled`)

Symmetric to the enable path. The operator drives the StatefulSet through
`optional` first so that pods restarting in plaintext can still be reached
by peers that haven't been updated yet. After convergence, the operator
moves to `disabled` and deletes the `cluster-secret` Secret.

### Cluster secret coupling

The cluster secret is a static bearer token used for inter-node
authentication when `--auth-enabled` is set. The operator creates it only
when TLS is at least partially active (`mode != disabled`). Sending a
static token over plaintext is an anti-pattern, so the secret only exists
when the wire is encrypted.

This means **authentication requires TLS**. If you intend to enable
`--auth-enabled`, enable TLS first.

The server enforces this invariant at startup:

```
--cluster-secret requires TLS (set --tls-mode to optional or required
and provide --tls-cert-file / --tls-key-file)
```

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

The dialer probes each peer the first time it connects in `optional` mode
and re-probes on persistent connection failures, so a peer flipping
between modes during the migration is followed automatically.

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

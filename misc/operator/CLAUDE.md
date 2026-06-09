# Ledger Operator

## Development Environment

All commands (build, test, lint, etc.) MUST be run through `nix develop` to ensure correct toolchain versions. Use:

```sh
nix develop --command <cmd>
```

For example:
```sh
nix develop --command go build ./...
nix develop --command go test ./...
nix develop --command golangci-lint run
```

## ClusterConfig Reconciliation

`spec.cache.rotationThreshold` and every `spec.bloom.*` field are part of
`computeSpecHash` (see `hash.go`). Any change to these fields:

1. Bumps the `ledger.formance.com/spec-hash` pod-template annotation.
2. Triggers a `RollingUpdate` of the StatefulSet — explicit `Partition: 0`
   strategy in `reconcile_statefulset.go::buildStatefulSetSpec`.
3. Each new pod boots with the updated env vars (mapped from CLI flags by
   `service.BindEnvToCommand` in go-libs).
4. When the new template's eventual leader fires `LeaderReadyEvent`,
   `internal/bootstrap/module.go::proposeClusterConfigIfNeeded` diffs the
   CLI-derived config against the persisted state and proposes a Raft entry
   carrying `ClusterConfig`.
5. The FSM applies via `applyClusterConfig` — deterministic across all nodes
   (cache reset + bloom rebuild). The "Cache is the source of authority"
   invariant from `CLAUDE.md` holds because the apply is replicated, not
   per-node.

Convergence is therefore bounded by: time to roll the STS + one election cycle.

Validation lives in `ledger_controller.go::validateClusterConfig`. Defaults
remain server-side (`cmd/server/server.go::loadBloomConfig`) — do not mirror
defaults in the operator.

Antithesis exercises this end-to-end with the singleton driver
`singleton_driver_config_change` (`tests/antithesis/workload/bin/cmds/...`).

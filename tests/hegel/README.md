# Hegel pilot

The initial standalone pilot has been integrated into the Antithesis workload
module. The maintained tests now exercise its shared generators against a real
local Ledger, with an independent accounting oracle and fresh business state
for every Hegel invocation.

See [the integration guide](../antithesis/HEGEL.md) and
[`internal/hegelintegration`](../antithesis/workload/internal/hegelintegration).

Run the local property and boundary tests:

```bash
nix develop --command go -C tests/antithesis/workload test \
  ./internal/hegelintegration -count=1 -v -timeout=5m
```

The opt-in sensitivity test deliberately omits a revert. It must fail and
report a reduced counterexample; this is a harness check, not a Ledger defect:

```bash
nix develop --command env HEGEL_INTEGRATION_OMIT_REVERT=1 \
  go -C tests/antithesis/workload test ./internal/hegelintegration \
  -run TestMissingRevertSensitivity -count=1 -v -timeout=5m
```

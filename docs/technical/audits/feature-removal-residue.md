# Removed-feature residue: evidence and ownership

The [manifest](feature-removal-residue.json) applies when a feature has been
removed. Establish the specific removal decision, its exact code range, and
the surviving behavior before searching for residue. A vocabulary match alone
is not a defect.

## Evidence contract

Build an inventory of the removed feature's identifiers across production
paths, protobuf and OpenAPI surfaces, generated and hand-written bindings,
configuration, operator/deployment files, tests, workloads and documentation.
For every candidate, show a reachable caller or externally observable surface
that should have disappeared. A dormant test fixture or unrelated ordinary use
of the same word is not enough.

Trace surviving shared paths to their new authoritative source. Use positive
and negative witnesses: a removed operation must be unavailable, and retained
operations such as revert must still work without the removed sidecar. Check
that test assertions remain reachable and non-vacuous. Record exact searches,
commands and observed outputs separately from proposed checks.

## Ownership boundaries

This domain owns the completeness of a removal and regression of behavior
that shared its implementation. A remaining accounting error belongs to
`accounting-invariants`; a wire codec defect unrelated to the removal belongs
to `api-boundary-contracts`; test collection enforcement belongs to
`test-reachability-enforcement`. One root cause receives one finding.

The manifest's examples are historical leads, not a request to remove more
features. Establish current policy and code before classifying residue.

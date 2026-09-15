# fctl SDK snapshot

`fctl-v2-poc/` contains a minimal Go SDK snapshot and the public plugin WIT
copied from `formancehq/fctl-v2-poc` commit
`226e3211c9eb04593992c76c2b395cf42a0bf1be`. The included production files are
the union of the fctl packages reached by `go list -deps` for the Ledger plugin
in its normal and `fctl_component_guest` builds. Go module manifests and the
public WIT are included in addition to that measured dependency graph; SDK
tests, test data, and TypeScript sources are not consumed and are omitted.

The source repository is private, while Ledger CI receives a token scoped to
the Ledger repository. Keeping this minimal snapshot in the Ledger tree makes
the plugin gates self-contained without adding a cross-repository credential.
`../fctl-sdk.lock.json` is the fail-closed provenance manifest: the plugin
wrapper verifies the module path, exact Nix content hash of the complete
minimal snapshot, and WIT SHA-256 before any tidy, test, or component build
consumes it. Adding, removing, or changing a file therefore requires an
explicit manifest reseal.

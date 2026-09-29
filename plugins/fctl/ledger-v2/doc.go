// Package ledgerv2 is the portable fctl command plugin for Ledger product
// major 2.
//
// It publishes the 22 historical `fctl ledger …` commands recorded in
// inventory.json, binds each to one audited `openapi/v2.yaml` operation
// through the public generated-client HTTP policy, and reaches the product only
// through the host-owned request boundary. The plugin resolves no
// endpoint, no credential and no target, declares no signing capability, and
// names no `Ledger.V1` operation.
//
// The compatibility documents in this directory are the authoritative contract.
// inventory.json is the oracle the descriptor tests assert against.
package ledgerv2

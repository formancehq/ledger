// Package metadatalimits owns Ledger's semantic metadata size contract.
//
// Callers use the effective committed policy, not DefaultMetadataLimits, to
// qualify writes. Defaults are startup proposal values only. Validation here
// does not discover policy, check metadata shape, encode a request, or replace
// Ledger's admission and apply checks.
//
// Generic map functions accept the caller's existing metadata values and a pure
// semantic measurement function, avoiding a second metadata representation or
// any dependency on Ledger's internal protobufs. Typed clients remain responsible
// for measuring their value variants according to the exported weights.
package metadatalimits

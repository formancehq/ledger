package celrewrite

import "github.com/formancehq/invariants"

// validateKey checks a metadata key against the platform's charset rules. It
// exists so bindings and the admission-time literal walk share the exact same
// rule, and to isolate the ledger's invariants dependency to one file.
func validateKey(key string) error {
	return invariants.ValidateMetadataKey(key)
}

// validateValue checks a metadata string value against the platform's rules.
func validateValue(value string) error {
	return invariants.ValidateMetadataString(value)
}

// validateAccountAddress checks a rewritten account address is valid.
func validateAccountAddress(addr string) error {
	return invariants.ValidateLedgerAccountAddress(addr)
}

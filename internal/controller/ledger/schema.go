package ledger

import "fmt"

type SchemaEnforcementMode string

const (
	// emit error on failing validation & missing schema
	SchemaEnforcementStrict SchemaEnforcementMode = "strict"
	// only emit warnings on failing validation & missing schema
	SchemaEnforcementAudit SchemaEnforcementMode = "audit"
)

// Validate returns an error unless the mode is one of the accepted values.
// Matching is case-sensitive: any other value used to silently behave as audit.
func (m SchemaEnforcementMode) Validate() error {
	switch m {
	case SchemaEnforcementAudit, SchemaEnforcementStrict:
		return nil
	default:
		return fmt.Errorf("invalid schema-enforcement-mode %q: expected %q or %q", string(m), SchemaEnforcementAudit, SchemaEnforcementStrict)
	}
}

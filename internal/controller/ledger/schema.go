package ledger

type SchemaEnforcementMode string

const (
	// emit error on failing validation & missing schema
	SchemaEnforcementStrict SchemaEnforcementMode = "strict"
	// only emit warnings on failing validation & missing schema
	SchemaEnforcementAudit SchemaEnforcementMode = "audit"
)

package ledger

type Parameters[INPUT any] struct {
	DryRun         bool
	IdempotencyKey string
	Input          INPUT
	SchemaVersion string
}

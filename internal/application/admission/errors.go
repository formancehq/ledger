package admission

import "github.com/formancehq/ledger/v3/internal/domain"

// Sentinel errors for admission-layer validation that does not belong to the
// domain (e.g. integration-config invariants like AWS RDS IAM auth fields).
// Domain errors stay in internal/domain; cross-cutting application errors
// live here so the domain package keeps its narrow ledger-semantics scope.
var (
	// ErrMirrorIAMRegionRequired is raised when a CreateLedger order carries a
	// Postgres mirror source with awsIamAuth set but an empty region. The
	// region is mandatory to sign the SigV4 IAM auth token, and validating it
	// at admission (rather than only at mirror-worker startup) avoids
	// persisting a malformed mirror config in the audit chain.
	ErrMirrorIAMRegionRequired = domain.NewValidationSentinel("mirrorSource.postgres.awsIamAuth.region is required when awsIamAuth is set")
)

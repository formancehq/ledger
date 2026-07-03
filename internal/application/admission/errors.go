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

	// ErrMirrorIAMRequiresTLS rejects mirror configs that pair AWS RDS IAM
	// authentication with an sslmode that allows cleartext (disable, allow,
	// prefer, or unset -- libpq's default "prefer" falls back to non-TLS).
	// The SigV4 token in cc.Password is a short-lived bearer credential and
	// must not transit cleartext.
	ErrMirrorIAMRequiresTLS = domain.NewValidationSentinel("mirrorSource.postgres: awsIamAuth requires sslmode in {require, verify-ca, verify-full}")

	// ErrMirrorAddressRewritePatternInvalid rejects a mirror config whose
	// addressRewriteRules contain a pattern that is empty or not a valid Go (RE2)
	// regular expression. An empty pattern matches at every boundary and would
	// silently rewrite every address (including "world"); both are rejected at
	// admission so a malformed rule fails fast before the config is persisted,
	// instead of stalling — or corrupting — the mirror on every batch.
	ErrMirrorAddressRewritePatternInvalid = domain.NewValidationSentinel("mirrorSource.addressRewriteRules: pattern must be a non-empty valid regular expression")
)

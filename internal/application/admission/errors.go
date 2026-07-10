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

	// ErrMirrorRewriteRuleInvalid rejects a mirror config whose rewriteRules
	// contain a rule that fails to compile: invalid CEL syntax, a match that is
	// not boolean, a cel expression that does not return a transaction, an empty
	// cel expression, or a rule set that exceeds the static caps. Rejected at
	// admission so a malformed rule fails fast before the config is persisted,
	// instead of stalling — or corrupting — the mirror on every batch.
	ErrMirrorRewriteRuleInvalid = domain.NewValidationSentinel("mirrorSource.rewriteRules: each rule must have a boolean match and a cel expression returning a transaction")

	// ErrLedgerNameReserved rejects a CreateLedger order whose name collides with
	// a top-level static HTTP route segment (see reservedLedgerNames). Such a
	// ledger would be permanently unreachable over REST: the chi router matches
	// the static segment before the `/v3/{ledgerName}` wildcard, so
	// `GET /v3/<reserved>` and `GET /v3/<reserved>/<x>` bind to the reserved
	// handler instead of the ledger. Reserving the name at admission (create
	// time) is cheaper and safer than special-casing the router, and keeps the
	// name available should a future release want to migrate the endpoint.
	ErrLedgerNameReserved = domain.NewValidationSentinel("ledger name is reserved and cannot be used (it collides with a top-level API route)")
)

// reservedLedgerNames is the set of ledger names that collide with top-level
// static HTTP routes served directly under the /v3 prefix (as opposed to under
// /v3/{ledgerName}/...). A ledger with one of these names would be shadowed by
// the static route and unreachable over REST, so creation is refused.
//
// Keep this in sync with the top-level static routes registered in
// internal/adapter/http/handler.go. Today only the audit read routes
// (/v3/audit-entries, EN-1481) live at that level; extend the set when new
// top-level static routes are added.
var reservedLedgerNames = map[string]struct{}{
	"audit-entries": {},
}

// IsReservedLedgerName reports whether name collides with a top-level static
// HTTP route segment and therefore cannot be used as a ledger name.
func IsReservedLedgerName(name string) bool {
	_, reserved := reservedLedgerNames[name]

	return reserved
}

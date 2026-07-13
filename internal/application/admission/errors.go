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

	// ErrLedgerNameReservedPrefix rejects the ledger name "_". That single
	// segment is reserved for system / non-ledger HTTP routes, which all live
	// under /v3/_/… (e.g. GET /v3/_/audit-entries, /v3/_/chapters, /v3/_/indexes,
	// /v3/_/events-sinks, /v3/_/signing-keys) and share the /v3/{ledgerName} path
	// namespace. A ledger named "_" would be shadowed by that fixed segment.
	// Reserving the one segment "_" — rather than an ever-growing list of
	// individual reserved words — keeps the guard in lock-step with the route
	// table for free and applies uniformly across transports (see
	// internal/adapter/http/handler.go). Enforced on every ledger-scoped order
	// (validateOrderLedgerName), not just CreateLedger, so a "_" ledger can never
	// be created or written to.
	ErrLedgerNameReservedPrefix = domain.NewValidationSentinel("ledger name \"_\" is reserved for system API routes")

	// ErrIndexTargetUnsupported rejects a CreateIndex order for an IndexID the
	// builder has no backfill path for: a metadata target other than
	// ACCOUNT/TRANSACTION (e.g. LEDGER), an account builtin other than ASSET
	// (e.g. the UNSPECIFIED sentinel), a log builtin other than DATE, or an
	// out-of-range builtin enum. Such an index would be persisted in the
	// registry but never built, so it is rejected at admission (covering gRPC
	// and HTTP) rather than silently creating a permanently-unbuilt index.
	// See indexes.Supported.
	ErrIndexTargetUnsupported = domain.NewValidationSentinel("index target not supported (metadata: ACCOUNT/TRANSACTION; account builtin: ASSET; log builtin: DATE)")
)

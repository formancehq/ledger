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

	// ErrLedgerNameReservedPrefix rejects the exact names "_" (system API
	// routes) and "_system" (system events) on every ledger-scoped order.
	// Other underscore-prefixed names remain valid.
	ErrLedgerNameReservedPrefix = domain.NewValidationSentinel("ledger names \"_\" and \"_system\" are reserved for system use")

	// ErrIndexTargetUnsupported rejects a CreateIndex order for an IndexID the
	// builder has no backfill path for: a metadata target other than
	// ACCOUNT/TRANSACTION (e.g. LEDGER), an account builtin other than ASSET
	// (e.g. the UNSPECIFIED sentinel), a log builtin other than DATE, or an
	// out-of-range builtin enum. Such an index would be persisted in the
	// registry but never built, so it is rejected at admission (covering gRPC
	// and HTTP) rather than silently creating a permanently-unbuilt index.
	// See indexes.Supported.
	ErrIndexTargetUnsupported = domain.NewValidationSentinel("index target not supported (metadata: ACCOUNT/TRANSACTION; account builtin: ASSET; log builtin: DATE)")

	// ErrSigningKeyInvalidLength rejects a RegisterSigningKey order whose
	// public_key is not exactly an Ed25519 public key (32 bytes). This is the only
	// length gate on the server write path: the FSM validates the key ID and
	// nothing else, and SaveSigningKey stores the bytes unchanged, so without it a
	// raw gRPC client can persist a row too short for query.ReadSigningKeys to
	// decode. Validated here rather than in the FSM so the rejection happens
	// pre-Raft and cannot diverge state across a mixed-binary rolling upgrade.
	ErrSigningKeyInvalidLength = domain.NewValidationSentinel("signing key public_key must be exactly 32 bytes (Ed25519)")
)

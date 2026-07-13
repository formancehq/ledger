package domain

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ErrNotFound is a sentinel error for missing records in storage lookups. It
// is an internal marker, NOT a Describable — it never reaches the API edge
// because callers always convert it to one of the typed Describable errors
// below (ErrLedgerNotFound, ErrTransactionNotFound, etc.) before returning.
var ErrNotFound = errors.New("not found")

// ErrorKind classifies a domain error semantically — independent of any
// transport (gRPC, HTTP, CLI). Adapters translate Kind → their own status
// code via an exhaustive switch; adding a new Kind without updating every
// switch fails the build (golangci-lint `exhaustive` rule).
//
// Kind is NOT declared per error type: it is derived from the error's Reason
// via KindForReason (one exhaustive switch — the single source of truth for
// reason→kind), and read through domain.Kind(d). A new domain error declares
// only its Reason; its kind follows. Anything outside BusinessError (signing.*,
// raft.*, ctx errors, AWS smithy, etc.) is sanitised by convertToGRPCError's
// defence-in-depth default branch. See #431.
type ErrorKind int

const (
	// KindValidation: the caller sent a syntactically/structurally invalid
	// request. gRPC: InvalidArgument. HTTP: 400.
	KindValidation ErrorKind = iota + 1

	// KindNotFound: the named resource does not exist. gRPC: NotFound.
	// HTTP: 404.
	KindNotFound

	// KindAlreadyExists: the resource exists and the caller asked to create
	// it. gRPC: AlreadyExists. HTTP: 409.
	KindAlreadyExists

	// KindConflict: the request collides with current state in a way that
	// could be reconciled by inspecting/modifying that state (e.g. already
	// reverted, ledger deleted, mirror-mode write attempt). gRPC:
	// FailedPrecondition. HTTP: 409.
	KindConflict

	// KindPrecondition: a precondition the caller can in principle satisfy
	// is not met (e.g. insufficient funds, index missing, chapter not
	// closing). gRPC: FailedPrecondition. HTTP: 400.
	KindPrecondition

	// KindUnavailable: the server cannot serve the request right now but
	// the caller should retry (e.g. maintenance mode, stale proposal, lost
	// leadership). gRPC: Unavailable. HTTP: 503.
	KindUnavailable

	// KindUnauthenticated: the caller is not authenticated. gRPC:
	// Unauthenticated. HTTP: 401.
	KindUnauthenticated

	// KindPermissionDenied: the caller is authenticated but lacks the right
	// to perform this action. gRPC: PermissionDenied. HTTP: 403.
	KindPermissionDenied

	// KindInternal: an invariant on the server side is broken. The caller
	// cannot fix it. gRPC: Internal. HTTP: 500.
	KindInternal

	// KindResourceExhausted maps to gRPC ResourceExhausted / HTTP 429. Used for
	// limit/quota conditions that will not clear on a fast retry (e.g. disk-full
	// write gate), signalling clients to back off rather than hammer-retry.
	KindResourceExhausted
)

// String returns the canonical name of the kind. Used in tests and logging,
// not part of the wire contract — Reason() is the client-facing identifier.
func (k ErrorKind) String() string {
	switch k {
	case KindValidation:
		return "Validation"
	case KindNotFound:
		return "NotFound"
	case KindAlreadyExists:
		return "AlreadyExists"
	case KindConflict:
		return "Conflict"
	case KindPrecondition:
		return "Precondition"
	case KindUnavailable:
		return "Unavailable"
	case KindUnauthenticated:
		return "Unauthenticated"
	case KindPermissionDenied:
		return "PermissionDenied"
	case KindInternal:
		return "Internal"
	case KindResourceExhausted:
		return "ResourceExhausted"
	default:
		return fmt.Sprintf("ErrorKind(%d)", int(k))
	}
}

// Describable is the contract every domain business error must satisfy.
// Adapters derive a transport status code from domain.Kind(d) — a function of
// Reason — read Reason() for the stable client-facing identifier, and
// Metadata() for the structured error-info payload (field values that let the
// client format a precise user message — names, IDs, etc.).
//
// The interface embeds `error` so `errors.As(err, &target Describable)` works
// transparently from any chain. Implementations should be value-comparable
// when stateless (so `errors.Is(err, sentinel)` works) and pointer-typed
// when they carry per-occurrence data (so the metadata is preserved).
type Describable interface {
	error

	// Reason returns a stable, client-facing identifier (UPPER_SNAKE_CASE).
	// Clients pattern-match on this string; never rename a Reason once
	// shipped — add a new Describable type instead. The error's semantic
	// kind is derived from it via domain.Kind / KindForReason.
	Reason() string

	// Metadata returns structured context the client uses to format a
	// user-facing message (e.g. {"name": "default"}). Return nil when
	// there is no per-occurrence context.
	Metadata() map[string]string
}

// ReplayedFailure is a Describable reconstructed from a stored idempotency
// outcome. When a retried idempotency key's first apply was a definitive
// business rejection, the FSM replays this so every duplicate observes the same
// error instead of re-executing in a changed context.
type ReplayedFailure struct {
	ErrReason string
	Msg       string
	Meta      map[string]string
}

func (e *ReplayedFailure) Error() string               { return e.Msg }
func (e *ReplayedFailure) Reason() string              { return e.ErrReason }
func (e *ReplayedFailure) Metadata() map[string]string { return e.Meta }

// IsFreezableFailure reports whether a rejection is a definitive, deterministic
// business outcome safe to freeze against an idempotency key. Retryable /
// infrastructure kinds (Unavailable, Internal, auth) are never frozen — they did
// not produce a definitive business result.
func IsFreezableFailure(k ErrorKind) bool {
	switch k {
	case KindValidation, KindNotFound, KindAlreadyExists, KindConflict, KindPrecondition:
		return true
	default:
		return false
	}
}

// Reason constants shared between server and client. Wire contract: do NOT
// rename an existing constant. Each constant appears in exactly one
// Reason() implementation below.
const (
	ErrReasonLedgerAlreadyExists           = "LEDGER_ALREADY_EXISTS"
	ErrReasonLedgerNotFound                = "LEDGER_NOT_FOUND"
	ErrReasonLedgerDeleted                 = "LEDGER_DELETED"
	ErrReasonIdempotencyKeyConflict        = "IDEMPOTENCY_KEY_CONFLICT"
	ErrReasonTransactionReferenceConflict  = "TRANSACTION_REFERENCE_CONFLICT"
	ErrReasonTransactionReferenceNotFound  = "TRANSACTION_REFERENCE_NOT_FOUND"
	ErrReasonTransactionNotFound           = "TRANSACTION_NOT_FOUND"
	ErrReasonTransactionAlreadyReverted    = "TRANSACTION_ALREADY_REVERTED"
	ErrReasonInsufficientFunds             = "INSUFFICIENT_FUNDS"
	ErrReasonVolumeOverflow                = "VOLUME_OVERFLOW"
	ErrReasonBalanceNotPreloaded           = "BALANCE_NOT_PRELOADED"
	ErrReasonNumscriptParseError           = "NUMSCRIPT_PARSE_ERROR"
	ErrReasonValidation                    = "VALIDATION"
	ErrReasonAuditDisabled                 = "AUDIT_DISABLED"
	ErrReasonSinkAlreadyExists             = "SINK_ALREADY_EXISTS"
	ErrReasonSinkNotFound                  = "SINK_NOT_FOUND"
	ErrReasonSinkBatchSizeTooLarge         = "SINK_BATCH_SIZE_TOO_LARGE"
	ErrReasonNoChapterOpen                 = "NO_CHAPTER_OPEN"
	ErrReasonChapterNotFound               = "CHAPTER_NOT_FOUND"
	ErrReasonChapterNotClosing             = "CHAPTER_NOT_CLOSING"
	ErrReasonChapterNotClosed              = "CHAPTER_NOT_CLOSED"
	ErrReasonChapterNotArchiving           = "CHAPTER_NOT_ARCHIVING"
	ErrReasonMetadataNotFound              = "METADATA_NOT_FOUND"
	ErrReasonInvalidReceipt                = "INVALID_RECEIPT"
	ErrReasonMaintenanceMode               = "MAINTENANCE_MODE"
	ErrReasonStaleProposal                 = "STALE_PROPOSAL"
	ErrReasonInvalidCronExpression         = "INVALID_CRON_EXPRESSION"
	ErrReasonLedgerInMirrorMode            = "LEDGER_IN_MIRROR_MODE"
	ErrReasonLedgerNotInMirrorMode         = "LEDGER_NOT_IN_MIRROR_MODE"
	ErrReasonPreparedQueryAlreadyExists    = "PREPARED_QUERY_ALREADY_EXISTS"
	ErrReasonPreparedQueryNotFound         = "PREPARED_QUERY_NOT_FOUND"
	ErrReasonIndexNotFound                 = "INDEX_NOT_FOUND"
	ErrReasonIndexBuilding                 = "INDEX_BUILDING"
	ErrReasonIndexInconsistent             = "INDEX_INCONSISTENT"
	ErrReasonMetadataFieldNotInSchema      = "METADATA_FIELD_NOT_IN_SCHEMA"
	ErrReasonNumscriptNotFound             = "NUMSCRIPT_NOT_FOUND"
	ErrReasonNumscriptVersionAlreadyExists = "NUMSCRIPT_VERSION_ALREADY_EXISTS"
	ErrReasonNumscriptInvalidVersion       = "NUMSCRIPT_INVALID_VERSION"
	ErrReasonAccountNotMatchingType        = "ACCOUNT_NOT_MATCHING_TYPE"
	ErrReasonAccountTypeNotFound           = "ACCOUNT_TYPE_NOT_FOUND"
	ErrReasonAccountTypeAlreadyExists      = "ACCOUNT_TYPE_ALREADY_EXISTS"
	ErrReasonInvalidPattern                = "INVALID_PATTERN"
	ErrReasonAccountTypeHasAccounts        = "ACCOUNT_TYPE_HAS_ACCOUNTS"
	ErrReasonAccountTypeConflict           = "ACCOUNT_TYPE_CONFLICT"
	ErrReasonColdStorageDisabled           = "COLD_STORAGE_DISABLED"
	ErrReasonTransientAccountNonZero       = "TRANSIENT_ACCOUNT_NON_ZERO"
	ErrReasonFilterCompilation             = "FILTER_COMPILATION_ERROR"
	ErrReasonInvalidOrderType              = "INVALID_ORDER_TYPE"
	ErrReasonInvalidApplyType              = "INVALID_APPLY_TYPE"
	ErrReasonInvalidExecutionPlan          = "INVALID_EXECUTION_PLAN"
	ErrReasonExecutionPlanTooLarge         = "EXECUTION_PLAN_TOO_LARGE"
	ErrReasonCoverageMiss                  = "COVERAGE_MISS"
	ErrReasonIdempotencyCheckFailed        = "IDEMPOTENCY_CHECK_FAILED"
	ErrReasonStorageOperation              = "STORAGE_OPERATION_FAILED"
	ErrReasonTransactionStateInconsistent  = "TRANSACTION_STATE_INCONSISTENT"
	ErrReasonCheckpointIDRequired          = "CHECKPOINT_ID_REQUIRED"
	ErrReasonCheckpointNotReady            = "CHECKPOINT_NOT_READY"
	ErrReasonNumscriptRuntime              = "NUMSCRIPT_RUNTIME"
	ErrReasonVolumeNotMaterialized         = "VOLUME_NOT_MATERIALIZED"
	ErrReasonNonDeterministicScript        = "NON_DETERMINISTIC_SCRIPT"

	// ErrReasonWritesBlockedDiskFull signals that the write gate rejected the
	// request because disk usage is at or above the configured block threshold.
	// Maps to gRPC ResourceExhausted / HTTP 429.
	ErrReasonWritesBlockedDiskFull = "WRITES_BLOCKED_DISK_FULL"

	// ErrReasonWritesBlockedClockSkew signals that the write gate rejected the
	// request because cluster clock skew exceeds the configured threshold.
	// Maps to gRPC Unavailable / HTTP 503.
	ErrReasonWritesBlockedClockSkew = "WRITES_BLOCKED_CLOCK_SKEW"
)

// BusinessError wraps a Describable so it can flow through code paths that
// only understand the standard `error` interface (futures, admission,
// controller) while still carrying the structured information adapters need
// at the API edge. The field is typed as Describable, NOT error — a new domain
// failure case that does not implement Describable (Error/Reason/Metadata) does
// not compile.
type BusinessError struct {
	Err Describable
}

func (e *BusinessError) Error() string               { return e.Err.Error() }
func (e *BusinessError) Unwrap() error               { return e.Err }
func (e *BusinessError) Reason() string              { return e.Err.Reason() }
func (e *BusinessError) Metadata() map[string]string { return e.Err.Metadata() }

// WrapCompileError propagates errors returned by query.Compile through to the
// caller. Typed BusinessErrors (ErrIndexNotFound from missing-index paths,
// ErrFilterCompilation from validation/parameter-parse paths produced by
// NewFilterCompilationError at the source) flow through verbatim and reach
// the gRPC adapter's typed mappings. Other errors — iterator creation, Pebble
// existence checks, raw fmt.Errorf paths — pass through unchanged so the
// convertToGRPCError default branch sanitises them as codes.Unknown with a
// correlation ID, preventing internal storage/cache details from leaking to
// API clients (#326). Kept as a single edge function so future call sites do
// not accidentally re-introduce the leak by re-wrapping every cause.
func WrapCompileError(err error) error {
	return err
}

// NewFilterCompilationError wraps a client-actionable filter compilation
// detail (schema-type mismatch, parameter parse failure) into a typed
// BusinessError so the gRPC adapter maps it to InvalidArgument with the
// detail preserved in ErrorInfo.metadata. Use this ONLY at the source —
// inside query.Compile's validation helpers — never at the boundary, so
// non-validation errors (iterator creation, Pebble lookups) reach the
// sanitiser unchanged.
func NewFilterCompilationError(format string, args ...any) error {
	return &BusinessError{Err: &ErrFilterCompilation{Detail: fmt.Sprintf(format, args...)}}
}

// validationSentinel is a stateless validation Describable. Used for the
// many "must not be empty / must not contain null / must be valid format"
// failures that all map to the same Kind+Reason: a distinct *pointer*
// instance per failure mode (so errors.Is compares pointer identity) but no
// per-occurrence metadata.
type validationSentinel struct {
	msg string
}

func (e *validationSentinel) Error() string             { return e.msg }
func (*validationSentinel) Reason() string              { return ErrReasonValidation }
func (*validationSentinel) Metadata() map[string]string { return nil }

// NewValidationSentinel constructs a stateless validation sentinel. Use only
// at package init time; the returned pointer is the identity used by
// errors.Is at every call site.
//
// Exported so packages outside internal/domain (e.g. integration-config
// validators in the application layer) can build their own sentinels without
// piling integration-specific errors into the domain package.
func NewValidationSentinel(msg string) Describable {
	return &validationSentinel{msg: msg}
}

// ──────────────────────────────────────────────────────────────────────────
// Sentinel errors (zero-value structs). Constructed once and shared; comparable
// via errors.Is. Each implements Describable so wrapping them in
// BusinessError compiles.
// ──────────────────────────────────────────────────────────────────────────

// ErrColdStorageDisabled — archiving is attempted but cold storage is not configured.
type errColdStorageDisabled struct{}

func (errColdStorageDisabled) Error() string {
	return "cold storage is disabled: archiving is not available"
}
func (errColdStorageDisabled) Reason() string              { return ErrReasonColdStorageDisabled }
func (errColdStorageDisabled) Metadata() map[string]string { return nil }

var ErrColdStorageDisabled Describable = errColdStorageDisabled{}

// ErrAuditDisabled — audit log is disabled on this server.
type errAuditDisabled struct{}

func (errAuditDisabled) Error() string               { return "audit log is disabled on this server" }
func (errAuditDisabled) Reason() string              { return ErrReasonAuditDisabled }
func (errAuditDisabled) Metadata() map[string]string { return nil }

var ErrAuditDisabled Describable = errAuditDisabled{}

// ErrMaintenanceMode — cluster is in maintenance mode, writes are blocked.
type errMaintenanceMode struct{}

func (errMaintenanceMode) Error() string {
	return "cluster is in maintenance mode: write operations are blocked"
}
func (errMaintenanceMode) Reason() string              { return ErrReasonMaintenanceMode }
func (errMaintenanceMode) Metadata() map[string]string { return nil }

var ErrMaintenanceMode Describable = errMaintenanceMode{}

// ErrStaleProposal — proposal rejected: predicted index mismatch after leadership transition.
type errStaleProposal struct{}

func (errStaleProposal) Error() string {
	return "proposal rejected: predicted index mismatch (stale tracker after leadership transition)"
}
func (errStaleProposal) Reason() string              { return ErrReasonStaleProposal }
func (errStaleProposal) Metadata() map[string]string { return nil }

var ErrStaleProposal Describable = errStaleProposal{}

// ErrWritesBlockedDiskFull is returned by the write gate when disk usage is at
// or above the configured block threshold. Maps to gRPC ResourceExhausted / HTTP 429.
type errWritesBlockedDiskFull struct{}

func (errWritesBlockedDiskFull) Error() string {
	return "writes blocked: disk usage exceeds threshold"
}
func (errWritesBlockedDiskFull) Reason() string              { return ErrReasonWritesBlockedDiskFull }
func (errWritesBlockedDiskFull) Metadata() map[string]string { return nil }

var ErrWritesBlockedDiskFull Describable = errWritesBlockedDiskFull{}

// ErrWritesBlockedClockSkew is returned by the write gate when cluster clock
// skew exceeds the configured threshold. Maps to gRPC Unavailable / HTTP 503.
type errWritesBlockedClockSkew struct{}

func (errWritesBlockedClockSkew) Error() string {
	return "writes blocked: clock skew exceeds threshold"
}
func (errWritesBlockedClockSkew) Kind() ErrorKind             { return KindUnavailable }
func (errWritesBlockedClockSkew) Reason() string              { return ErrReasonWritesBlockedClockSkew }
func (errWritesBlockedClockSkew) Metadata() map[string]string { return nil }

var ErrWritesBlockedClockSkew Describable = errWritesBlockedClockSkew{}

// ErrNoChapterOpen — no open chapter exists.
type errNoChapterOpen struct{}

func (errNoChapterOpen) Error() string               { return "no open chapter exists" }
func (errNoChapterOpen) Reason() string              { return ErrReasonNoChapterOpen }
func (errNoChapterOpen) Metadata() map[string]string { return nil }

var ErrNoChapterOpen Describable = errNoChapterOpen{}

// Validation sentinels — caller sent a request that fails a structural
// check. Each is a unique *validationSentinel pointer; errors.Is compares
// pointer identity (each call site references the exported variable, so
// the comparison is stable). No per-occurrence metadata — the message is
// the same every time and the Reason() is shared (ErrReasonValidation).
var (
	ErrTargetRequired             = NewValidationSentinel("target is required")
	ErrMetadataKeyRequired        = NewValidationSentinel("key is required")
	ErrNumscriptContentRequired   = NewValidationSentinel("numscript content is required")
	ErrScriptAndReferenceConflict = NewValidationSentinel("cannot specify both script and scriptReference")
	ErrEmptyTransaction           = NewValidationSentinel("transaction must produce at least one posting")
	ErrPostingsAndScriptConflict  = NewValidationSentinel("postings cannot be combined with script or scriptReference")
	ErrScriptRequired             = NewValidationSentinel("numscript: script is required")
	// Numscript identifier sentinels stay local: numscript is a
	// ledger-internal DSL, not part of the Formance-wide invariants in
	// github.com/formancehq/invariants.
	ErrNumscriptNameRequired    = NewValidationSentinel("numscript name is required")
	ErrNumscriptNameInvalidChar = NewValidationSentinel("numscript name must contain only printable ASCII (0x20–0x7E)")
	ErrNumscriptNameTooLong     = NewValidationSentinel("numscript name exceeds maximum length of 256 bytes")
	// Prepared-query identifier sentinels stay local: prepared queries are
	// a ledger-internal feature (CQRS read-side), not part of the
	// Formance-wide invariants in github.com/formancehq/invariants.
	ErrPreparedQueryRequired          = NewValidationSentinel("prepared query payload is required")
	ErrPreparedQueryNameRequired      = NewValidationSentinel("prepared query name is required")
	ErrPreparedQueryNameInvalidChar   = NewValidationSentinel("prepared query name must contain only printable ASCII (0x20–0x7E)")
	ErrPreparedQueryNameTooLong       = NewValidationSentinel("prepared query name exceeds maximum length of 256 bytes")
	ErrPreparedQueryTargetUnsupported = NewValidationSentinel("prepared query target is not supported (use ACCOUNTS, TRANSACTIONS or LOGS)")
	// Signing-key identifier sentinels stay local: request signing is a
	// ledger-internal feature, not part of the Formance-wide invariants in
	// github.com/formancehq/invariants.
	ErrSigningKeyIDRequired    = NewValidationSentinel("signing key id is required")
	ErrSigningKeyIDInvalidChar = NewValidationSentinel("signing key id must contain only printable ASCII (0x20–0x7E)")
	ErrSigningKeyIDTooLong     = NewValidationSentinel("signing key id exceeds maximum length of 256 bytes")
	// ErrLedgerNameRequired moved to validation.go; it wraps the
	// github.com/formancehq/invariants sentinel.
)

// ──────────────────────────────────────────────────────────────────────────
// Typed errors with per-occurrence data. Pointer receivers so each instance
// carries its own metadata.
// ──────────────────────────────────────────────────────────────────────────

// ErrLedgerAlreadyExists — attempting to create a ledger that already exists.
type ErrLedgerAlreadyExists struct {
	Name string
}

func (e *ErrLedgerAlreadyExists) Error() string { return "ledger already exists: " + e.Name }
func (*ErrLedgerAlreadyExists) Reason() string  { return ErrReasonLedgerAlreadyExists }
func (e *ErrLedgerAlreadyExists) Metadata() map[string]string {
	return map[string]string{"name": e.Name}
}

// ErrLedgerNotFound — referenced ledger does not exist.
type ErrLedgerNotFound struct {
	Name string
}

func (e *ErrLedgerNotFound) Error() string               { return "ledger does not exist: " + e.Name }
func (*ErrLedgerNotFound) Reason() string                { return ErrReasonLedgerNotFound }
func (e *ErrLedgerNotFound) Metadata() map[string]string { return map[string]string{"name": e.Name} }

// ErrLedgerDeleted — write targets a soft-deleted ledger.
type ErrLedgerDeleted struct {
	Name string
}

func (e *ErrLedgerDeleted) Error() string               { return "ledger has been deleted: " + e.Name }
func (*ErrLedgerDeleted) Reason() string                { return ErrReasonLedgerDeleted }
func (e *ErrLedgerDeleted) Metadata() map[string]string { return map[string]string{"name": e.Name} }

// ErrIdempotencyKeyConflict — idempotency key reused with different content.
type ErrIdempotencyKeyConflict struct {
	Key string
}

func (e *ErrIdempotencyKeyConflict) Error() string {
	return fmt.Sprintf("idempotency key conflict: key %q used with different request content", e.Key)
}
func (*ErrIdempotencyKeyConflict) Reason() string { return ErrReasonIdempotencyKeyConflict }
func (e *ErrIdempotencyKeyConflict) Metadata() map[string]string {
	return map[string]string{"key": e.Key}
}

// ErrTransactionReferenceConflict — reference already exists in the same ledger.
type ErrTransactionReferenceConflict struct {
	Ledger    string
	Reference string
	// ExistingTransactionID is the id of the transaction that already owned
	// the reference. Populated whenever the FSM detects the collision so
	// downstream consumers (Metadata(), and through it the OrderSkippedLog
	// context when the reason is whitelisted) can surface the existing tx
	// id to clients without an extra lookup. Optional for backwards-
	// compatible construction sites.
	ExistingTransactionID uint64
}

func (e *ErrTransactionReferenceConflict) Error() string {
	return fmt.Sprintf("transaction reference %q already exists in ledger %s", e.Reference, e.Ledger)
}
func (*ErrTransactionReferenceConflict) Reason() string { return ErrReasonTransactionReferenceConflict }
func (e *ErrTransactionReferenceConflict) Metadata() map[string]string {
	md := map[string]string{"ledger": e.Ledger, "reference": e.Reference}
	if e.ExistingTransactionID != 0 {
		md["existingTransactionId"] = strconv.FormatUint(e.ExistingTransactionID, 10)
	}

	return md
}

// ErrInvalidSkippableReason — the caller listed an ErrorReason in
// `skippable_reasons` that is not in the public business whitelist for the
// operation. Structural / internal reasons can never be skipped silently, so
// admission rejects them up front; this guarantees a poisoned admission
// cannot smuggle a structural skip into the FSM (the processor would refuse
// it too, but rejecting at the boundary gives the caller a clear 400-class
// error instead of a silent failure).
type ErrInvalidSkippableReason struct {
	// Provided is the offending ErrorReason value the caller submitted.
	// Carried as the typed enum (not a free string) so wire roundtrips are
	// lossless and the gRPC adapter renders the same identifier the client
	// shipped.
	Provided commonpb.ErrorReason
}

func (e *ErrInvalidSkippableReason) Error() string {
	return fmt.Sprintf("invalid skippable_reasons entry %q: not in the operation's business whitelist", ReasonString(e.Provided))
}
func (*ErrInvalidSkippableReason) Reason() string { return ErrReasonValidation }
func (e *ErrInvalidSkippableReason) Metadata() map[string]string {
	return map[string]string{"reason": ReasonString(e.Provided)}
}

// ErrTransactionNotFound — transaction ID is beyond the known range.
type ErrTransactionNotFound struct {
	TransactionID uint64
}

func (e *ErrTransactionNotFound) Error() string {
	return fmt.Sprintf("transaction %d does not exist", e.TransactionID)
}
func (*ErrTransactionNotFound) Reason() string { return ErrReasonTransactionNotFound }
func (e *ErrTransactionNotFound) Metadata() map[string]string {
	return map[string]string{"transactionId": strconv.FormatUint(e.TransactionID, 10)}
}

// ErrTransactionReferenceNotFound is returned when a transaction reference does not
// resolve to an existing transaction in the visible state (cache or current batch).
type ErrTransactionReferenceNotFound struct {
	Reference string
}

func (e *ErrTransactionReferenceNotFound) Error() string {
	return fmt.Sprintf("transaction with reference %q does not exist", e.Reference)
}
func (*ErrTransactionReferenceNotFound) Reason() string { return ErrReasonTransactionReferenceNotFound }
func (e *ErrTransactionReferenceNotFound) Metadata() map[string]string {
	return map[string]string{"reference": e.Reference}
}

// ErrTransactionTargetMissing is returned when a TargetTransaction is empty
// (neither id nor reference set).
var ErrTransactionTargetMissing = NewValidationSentinel("transaction target requires either id or reference")

// ErrTransactionAlreadyReverted — attempting to revert an already-reverted transaction.
type ErrTransactionAlreadyReverted struct {
	TransactionID uint64
}

func (e *ErrTransactionAlreadyReverted) Error() string {
	return fmt.Sprintf("transaction %d is already reverted", e.TransactionID)
}
func (*ErrTransactionAlreadyReverted) Reason() string { return ErrReasonTransactionAlreadyReverted }
func (e *ErrTransactionAlreadyReverted) Metadata() map[string]string {
	return map[string]string{"transactionId": strconv.FormatUint(e.TransactionID, 10)}
}

// ErrInsufficientFunds — source account does not have enough balance.
type ErrInsufficientFunds struct {
	Account string
	Asset   string
	Amount  string // requested amount (decimal string)
	Balance string // available balance (decimal string)
}

func (e *ErrInsufficientFunds) Error() string {
	return fmt.Sprintf(
		"insufficient funds on account %q for asset %s: needed %s, available %s",
		e.Account, e.Asset, e.Amount, e.Balance,
	)
}
func (*ErrInsufficientFunds) Reason() string { return ErrReasonInsufficientFunds }
func (e *ErrInsufficientFunds) Metadata() map[string]string {
	return map[string]string{"account": e.Account, "asset": e.Asset, "amount": e.Amount, "balance": e.Balance}
}

// ErrVolumeOverflow — a posting would push an account's volume past 2^256.
// Posting amounts are unbounded 256-bit values supplied by the API and the
// cumulative volumes are themselves uint256, so the addition can wrap
// silently. A wrap on the destination Input or on a `world` / `Force=true`
// source Output would silently create or destroy funds; the FSM rejects the
// order instead (#321).
type ErrVolumeOverflow struct {
	Account string
	Asset   string
	Side    string // "input" or "output"
	Amount  string // requested amount (decimal string)
	Current string // current volume on that side (decimal string)
}

func (e *ErrVolumeOverflow) Error() string {
	return fmt.Sprintf(
		"%s volume overflow on account %q for asset %s: current=%s + amount=%s exceeds 2^256",
		e.Side, e.Account, e.Asset, e.Current, e.Amount,
	)
}
func (*ErrVolumeOverflow) Reason() string { return ErrReasonVolumeOverflow }
func (e *ErrVolumeOverflow) Metadata() map[string]string {
	return map[string]string{
		"account": e.Account,
		"asset":   e.Asset,
		"side":    e.Side,
		"amount":  e.Amount,
		"current": e.Current,
	}
}

// ErrSinkAlreadyExists — adding a sink that already exists.
type ErrSinkAlreadyExists struct {
	Name string
}

func (e *ErrSinkAlreadyExists) Error() string               { return "event sink already exists: " + e.Name }
func (*ErrSinkAlreadyExists) Reason() string                { return ErrReasonSinkAlreadyExists }
func (e *ErrSinkAlreadyExists) Metadata() map[string]string { return map[string]string{"name": e.Name} }

// MaxSinkBatchSize is the server-side cap on SinkConfig.BatchSize. The
// emitter uses BatchSize as the capacity of a per-flush slice; without a
// bound, a misconfigured value drives one large allocation per flush on
// the leader. 100_000 is several orders of magnitude beyond any sane sink
// throughput and is intended only to catch fat-finger CLI input or
// adversarial admin commands — not to enforce policy.
const MaxSinkBatchSize int32 = 100_000

// ErrSinkBatchSizeTooLarge — sink configured with batch size > MaxSinkBatchSize.
type ErrSinkBatchSizeTooLarge struct {
	Name      string
	BatchSize int32
	Max       int32
}

func (e *ErrSinkBatchSizeTooLarge) Error() string {
	return fmt.Sprintf("event sink %q has batchSize=%d, exceeds maximum %d",
		e.Name, e.BatchSize, e.Max)
}
func (*ErrSinkBatchSizeTooLarge) Reason() string { return ErrReasonSinkBatchSizeTooLarge }
func (e *ErrSinkBatchSizeTooLarge) Metadata() map[string]string {
	return map[string]string{
		"name":      e.Name,
		"batchSize": strconv.Itoa(int(e.BatchSize)),
		"max":       strconv.Itoa(int(e.Max)),
	}
}

// ErrMetadataNotFound — deleting a metadata key that does not exist.
type ErrMetadataNotFound struct {
	Target string
	Key    string
}

func (e *ErrMetadataNotFound) Error() string {
	return fmt.Sprintf("metadata key %q not found on %s", e.Key, e.Target)
}
func (*ErrMetadataNotFound) Reason() string { return ErrReasonMetadataNotFound }
func (e *ErrMetadataNotFound) Metadata() map[string]string {
	return map[string]string{"target": e.Target, "key": e.Key}
}

// ErrSinkNotFound — removing a sink that does not exist.
type ErrSinkNotFound struct {
	Name string
}

func (e *ErrSinkNotFound) Error() string               { return "event sink not found: " + e.Name }
func (*ErrSinkNotFound) Reason() string                { return ErrReasonSinkNotFound }
func (e *ErrSinkNotFound) Metadata() map[string]string { return map[string]string{"name": e.Name} }

// ErrChapterNotFound — chapter ID does not match any known chapter.
type ErrChapterNotFound struct {
	ChapterID uint64
}

func (e *ErrChapterNotFound) Error() string { return fmt.Sprintf("chapter %d not found", e.ChapterID) }
func (*ErrChapterNotFound) Reason() string  { return ErrReasonChapterNotFound }
func (e *ErrChapterNotFound) Metadata() map[string]string {
	return map[string]string{"chapterId": strconv.FormatUint(e.ChapterID, 10)}
}

// ErrChapterNotClosing — attempting to seal a chapter not in CLOSING state.
type ErrChapterNotClosing struct {
	ChapterID uint64
}

func (e *ErrChapterNotClosing) Error() string {
	return fmt.Sprintf("chapter %d is not in CLOSING state", e.ChapterID)
}
func (*ErrChapterNotClosing) Reason() string { return ErrReasonChapterNotClosing }
func (e *ErrChapterNotClosing) Metadata() map[string]string {
	return map[string]string{"chapterId": strconv.FormatUint(e.ChapterID, 10)}
}

// ErrChapterNotClosed — attempting to archive a chapter not in CLOSED state.
type ErrChapterNotClosed struct {
	ChapterID uint64
}

func (e *ErrChapterNotClosed) Error() string {
	return fmt.Sprintf("chapter %d is not in CLOSED state", e.ChapterID)
}
func (*ErrChapterNotClosed) Reason() string { return ErrReasonChapterNotClosed }
func (e *ErrChapterNotClosed) Metadata() map[string]string {
	return map[string]string{"chapterId": strconv.FormatUint(e.ChapterID, 10)}
}

// ErrChapterNotArchiving — attempting to confirm archive of a chapter not in ARCHIVING state.
type ErrChapterNotArchiving struct {
	ChapterID uint64
}

func (e *ErrChapterNotArchiving) Error() string {
	return fmt.Sprintf("chapter %d is not in ARCHIVING state", e.ChapterID)
}
func (*ErrChapterNotArchiving) Reason() string { return ErrReasonChapterNotArchiving }
func (e *ErrChapterNotArchiving) Metadata() map[string]string {
	return map[string]string{"chapterId": strconv.FormatUint(e.ChapterID, 10)}
}

// ErrInvalidCronExpression — cron expression is invalid.
type ErrInvalidCronExpression struct {
	Expression string
	Details    string
}

func (e *ErrInvalidCronExpression) Error() string {
	return fmt.Sprintf("invalid cron expression %q: %s", e.Expression, e.Details)
}
func (*ErrInvalidCronExpression) Reason() string { return ErrReasonInvalidCronExpression }
func (e *ErrInvalidCronExpression) Metadata() map[string]string {
	return map[string]string{"expression": e.Expression, "details": e.Details}
}

// ErrLedgerInMirrorMode — write attempted on a mirror-mode ledger.
type ErrLedgerInMirrorMode struct {
	Name string
}

func (e *ErrLedgerInMirrorMode) Error() string {
	return fmt.Sprintf("ledger %s is in mirror mode: write operations are blocked", e.Name)
}
func (*ErrLedgerInMirrorMode) Reason() string { return ErrReasonLedgerInMirrorMode }
func (e *ErrLedgerInMirrorMode) Metadata() map[string]string {
	return map[string]string{"name": e.Name}
}

// ErrLedgerNotInMirrorMode — mirror-only operation attempted on a normal-mode ledger.
type ErrLedgerNotInMirrorMode struct {
	Name string
}

func (e *ErrLedgerNotInMirrorMode) Error() string {
	return fmt.Sprintf("ledger %s is not in mirror mode", e.Name)
}
func (*ErrLedgerNotInMirrorMode) Reason() string { return ErrReasonLedgerNotInMirrorMode }
func (e *ErrLedgerNotInMirrorMode) Metadata() map[string]string {
	return map[string]string{"name": e.Name}
}

// ErrPreparedQueryAlreadyExists — creating a prepared query that already exists.
type ErrPreparedQueryAlreadyExists struct {
	Ledger string
	Name   string
}

func (e *ErrPreparedQueryAlreadyExists) Error() string {
	return fmt.Sprintf("prepared query %s/%s already exists", e.Ledger, e.Name)
}
func (*ErrPreparedQueryAlreadyExists) Reason() string { return ErrReasonPreparedQueryAlreadyExists }
func (e *ErrPreparedQueryAlreadyExists) Metadata() map[string]string {
	return map[string]string{"ledger": e.Ledger, "name": e.Name}
}

// ErrPreparedQueryNotFound — prepared query does not exist.
type ErrPreparedQueryNotFound struct {
	Ledger string
	Name   string
}

func (e *ErrPreparedQueryNotFound) Error() string {
	return fmt.Sprintf("prepared query %s/%s not found", e.Ledger, e.Name)
}
func (*ErrPreparedQueryNotFound) Reason() string { return ErrReasonPreparedQueryNotFound }
func (e *ErrPreparedQueryNotFound) Metadata() map[string]string {
	return map[string]string{"ledger": e.Ledger, "name": e.Name}
}

// ErrIndexNotFound — query references an index that does not exist.
type ErrIndexNotFound struct {
	Index string
}

func (e *ErrIndexNotFound) Error() string               { return "index not found: " + e.Index }
func (*ErrIndexNotFound) Reason() string                { return ErrReasonIndexNotFound }
func (e *ErrIndexNotFound) Metadata() map[string]string { return map[string]string{"index": e.Index} }

// ErrMetadataFieldNotInSchema — CreateIndex targets a metadata key that
// has not been declared via SetMetadataFieldType first.
type ErrMetadataFieldNotInSchema struct {
	Target string
	Key    string
}

func (e *ErrMetadataFieldNotInSchema) Error() string {
	return "metadata field not declared in schema: " + e.Target + "/" + e.Key
}
func (*ErrMetadataFieldNotInSchema) Reason() string { return ErrReasonMetadataFieldNotInSchema }
func (e *ErrMetadataFieldNotInSchema) Metadata() map[string]string {
	return map[string]string{"target": e.Target, "key": e.Key}
}

// ErrIndexBuilding — query references an index that is still being built.
// Transient: the caller should retry once the build completes.
type ErrIndexBuilding struct {
	Index string
}

func (e *ErrIndexBuilding) Error() string               { return "index is still building: " + e.Index }
func (*ErrIndexBuilding) Reason() string                { return ErrReasonIndexBuilding }
func (e *ErrIndexBuilding) Metadata() map[string]string { return map[string]string{"index": e.Index} }

// ErrCheckpointNotReady — a read targets a query checkpoint whose read index
// has not been materialized yet. CreateQueryCheckpoint returns the checkpoint
// ID as soon as the Raft log is applied, but the physical read-index directory
// is created asynchronously by the index builder, and on a follower node the
// builder may simply not have caught up to the checkpoint's log sequence. Both
// are transient: the caller should retry until the directory exists. Mirrors
// ErrIndexBuilding — maps to KindUnavailable so gRPC clients retry deterministically
// instead of receiving an opaque, non-retryable Unknown.
type ErrCheckpointNotReady struct {
	CheckpointID uint64
}

func (e *ErrCheckpointNotReady) Error() string {
	return fmt.Sprintf("query checkpoint %d read index is still building", e.CheckpointID)
}
func (*ErrCheckpointNotReady) Reason() string { return ErrReasonCheckpointNotReady }
func (e *ErrCheckpointNotReady) Metadata() map[string]string {
	return map[string]string{"checkpointId": strconv.FormatUint(e.CheckpointID, 10)}
}

// ErrIndexInconsistent — read path detects a structural inconsistency between
// the filter index and the per-ledger log index (e.g. logID present in the
// filter index but missing or malformed in the log index). Surfacing this as
// an explicit error prevents stale/corrupt indexes from producing truncated
// query results that the caller can't distinguish from a legitimate empty
// result.
type ErrIndexInconsistent struct {
	Index  string
	Detail string
}

func (e *ErrIndexInconsistent) Error() string {
	return fmt.Sprintf("index %s is inconsistent: %s", e.Index, e.Detail)
}
func (*ErrIndexInconsistent) Reason() string { return ErrReasonIndexInconsistent }
func (e *ErrIndexInconsistent) Metadata() map[string]string {
	return map[string]string{"index": e.Index, "detail": e.Detail}
}

// ErrInvalidReceipt — JWT receipt fails verification. The metadata field is
// named "reason" on the wire (legacy contract); the Go field is renamed
// Detail to free the method name Reason() for the Describable interface.
type ErrInvalidReceipt struct {
	Detail string
}

func (e *ErrInvalidReceipt) Error() string { return "invalid receipt: " + e.Detail }
func (*ErrInvalidReceipt) Reason() string  { return ErrReasonInvalidReceipt }
func (e *ErrInvalidReceipt) Metadata() map[string]string {
	return map[string]string{"reason": e.Detail}
}

// ErrNumscriptNotFound — referenced numscript does not exist in the library.
type ErrNumscriptNotFound struct {
	Name string
}

func (e *ErrNumscriptNotFound) Error() string               { return "numscript not found: " + e.Name }
func (*ErrNumscriptNotFound) Reason() string                { return ErrReasonNumscriptNotFound }
func (e *ErrNumscriptNotFound) Metadata() map[string]string { return map[string]string{"name": e.Name} }

// ErrNumscriptVersionAlreadyExists — saving with a semver version that already exists.
type ErrNumscriptVersionAlreadyExists struct {
	Name    string
	Version string
}

func (e *ErrNumscriptVersionAlreadyExists) Error() string {
	return fmt.Sprintf("numscript %q version %s already exists", e.Name, e.Version)
}
func (*ErrNumscriptVersionAlreadyExists) Reason() string {
	return ErrReasonNumscriptVersionAlreadyExists
}
func (e *ErrNumscriptVersionAlreadyExists) Metadata() map[string]string {
	return map[string]string{"name": e.Name, "version": e.Version}
}

// ErrNumscriptInvalidVersion — version string is not valid semver.
type ErrNumscriptInvalidVersion struct {
	Version string
}

func (e *ErrNumscriptInvalidVersion) Error() string {
	return fmt.Sprintf("invalid numscript version %q: must be semver (major.minor.patch) or \"latest\"", e.Version)
}
func (*ErrNumscriptInvalidVersion) Reason() string { return ErrReasonNumscriptInvalidVersion }
func (e *ErrNumscriptInvalidVersion) Metadata() map[string]string {
	return map[string]string{"version": e.Version}
}

// ErrAccountNotMatchingType — account address doesn't match any active account type pattern.
type ErrAccountNotMatchingType struct {
	Address string
}

func (e *ErrAccountNotMatchingType) Error() string {
	return "account does not match any account type pattern: " + e.Address
}
func (*ErrAccountNotMatchingType) Reason() string { return ErrReasonAccountNotMatchingType }
func (e *ErrAccountNotMatchingType) Metadata() map[string]string {
	return map[string]string{"address": e.Address}
}

// ErrAccountTypeNotFound — referenced account type does not exist.
type ErrAccountTypeNotFound struct {
	Name string
}

func (e *ErrAccountTypeNotFound) Error() string { return "account type not found: " + e.Name }
func (*ErrAccountTypeNotFound) Reason() string  { return ErrReasonAccountTypeNotFound }
func (e *ErrAccountTypeNotFound) Metadata() map[string]string {
	return map[string]string{"name": e.Name}
}

// ErrAccountTypeAlreadyExists — creating an account type with a name that already exists.
type ErrAccountTypeAlreadyExists struct {
	Name string
}

func (e *ErrAccountTypeAlreadyExists) Error() string { return "account type already exists: " + e.Name }
func (*ErrAccountTypeAlreadyExists) Reason() string  { return ErrReasonAccountTypeAlreadyExists }
func (e *ErrAccountTypeAlreadyExists) Metadata() map[string]string {
	return map[string]string{"name": e.Name}
}

// ErrAccountTypeConflict — new account type pattern conflicts with an
// existing pattern (same specificity and overlapping match space).
type ErrAccountTypeConflict struct {
	NewPattern      string
	ExistingName    string
	ExistingPattern string
}

func (e *ErrAccountTypeConflict) Error() string {
	return fmt.Sprintf("pattern %q conflicts with existing account type %q (pattern %q): ambiguous match possible",
		e.NewPattern, e.ExistingName, e.ExistingPattern)
}
func (*ErrAccountTypeConflict) Reason() string { return ErrReasonAccountTypeConflict }
func (e *ErrAccountTypeConflict) Metadata() map[string]string {
	return map[string]string{"pattern": e.NewPattern, "existingName": e.ExistingName, "existingPattern": e.ExistingPattern}
}

// ErrInvalidPattern — account type pattern is syntactically invalid.
type ErrInvalidPattern struct {
	Pattern string
	Details string
}

func (e *ErrInvalidPattern) Error() string {
	return fmt.Sprintf("invalid pattern %q: %s", e.Pattern, e.Details)
}
func (*ErrInvalidPattern) Reason() string { return ErrReasonInvalidPattern }
func (e *ErrInvalidPattern) Metadata() map[string]string {
	return map[string]string{"pattern": e.Pattern, "details": e.Details}
}

// ErrAccountTypeHasAccounts — removing an account type that still has matching accounts.
type ErrAccountTypeHasAccounts struct {
	Name string
}

func (e *ErrAccountTypeHasAccounts) Error() string {
	return fmt.Sprintf("account type %q still has matching accounts", e.Name)
}
func (*ErrAccountTypeHasAccounts) Reason() string { return ErrReasonAccountTypeHasAccounts }
func (e *ErrAccountTypeHasAccounts) Metadata() map[string]string {
	return map[string]string{"name": e.Name}
}

// ErrNumscriptParse — Numscript program has syntax errors.
type ErrNumscriptParse struct {
	Details string
}

func (e *ErrNumscriptParse) Error() string { return "numscript parse error: " + e.Details }
func (*ErrNumscriptParse) Reason() string  { return ErrReasonNumscriptParseError }
func (e *ErrNumscriptParse) Metadata() map[string]string {
	return map[string]string{"details": e.Details}
}

// ErrDependencyDiscoveryFailed is returned when admission cannot discover all
// dependencies needed to preload a Numscript transaction before proposal.
type ErrDependencyDiscoveryFailed struct {
	Cause error
}

func (e *ErrDependencyDiscoveryFailed) Error() string {
	if e.Cause == nil {
		return "numscript dependency discovery failed"
	}

	return fmt.Sprintf("numscript dependency discovery failed: %v", e.Cause)
}

func (e *ErrDependencyDiscoveryFailed) Unwrap() error {
	return e.Cause
}

func (e *ErrDependencyDiscoveryFailed) Reason() string {
	var describable Describable
	if errors.As(e.Cause, &describable) {
		return describable.Reason()
	}

	return ErrReasonValidation
}
func (e *ErrDependencyDiscoveryFailed) Metadata() map[string]string {
	return map[string]string{"details": e.Error()}
}

// ErrBalanceNotPreloaded — a balance the script reads was not preloaded into
// the cache by admission. A transient server-side gap (e.g. the boot-time
// bloom-populate window, #318), not a caller-satisfiable precondition: a retry
// re-runs preload and can succeed — hence KindUnavailable, and never frozen as
// an idempotency outcome.
type ErrBalanceNotPreloaded struct {
	Account string
	Asset   string
}

func (e *ErrBalanceNotPreloaded) Error() string {
	return fmt.Sprintf("balance not preloaded for account %q asset %q", e.Account, e.Asset)
}
func (*ErrBalanceNotPreloaded) Reason() string { return ErrReasonBalanceNotPreloaded }
func (e *ErrBalanceNotPreloaded) Metadata() map[string]string {
	return map[string]string{"account": e.Account, "asset": e.Asset}
}

// ErrTransientAccountNonZero — one or more transient accounts held a non-zero
// balance at end of batch. Accounts lists every offender; the producer
// (state.ValidateTransientVolumes) sorts it by (Account, Asset) and dedups
// cross-ledger repeats, so Error()/Metadata() render byte-identically across
// nodes. That identity is hashed into the AuditFailure, so a nondeterministic
// order would fork the audit hash chain (invariant #2 / #8, EN-1423).
type ErrTransientAccountNonZero struct {
	Accounts []AccountAssetKey
}

func (e *ErrTransientAccountNonZero) Error() string {
	return "transient accounts with non-zero balance at end of batch (input != output): " + e.joinedAccounts()
}
func (*ErrTransientAccountNonZero) Reason() string { return ErrReasonTransientAccountNonZero }
func (e *ErrTransientAccountNonZero) Metadata() map[string]string {
	return map[string]string{"accounts": e.joinedAccounts()}
}

// joinedAccounts renders the offenders as a deterministic, comma-separated
// "account/asset" list. The slice is pre-sorted by the producer, so the output
// is stable; a nil slice yields "".
func (e *ErrTransientAccountNonZero) joinedAccounts() string {
	parts := make([]string, len(e.Accounts))
	for i, a := range e.Accounts {
		parts[i] = a.Account + "/" + a.Asset
	}

	return strings.Join(parts, ", ")
}

// RemoteError is the client-side Describable produced by cmdutil's
// BusinessErrorFromGRPC: it transports the wire contract (Reason + Metadata
// + Message + the gRPC-derived Kind) without committing the client to any
// specific Go type. Replaces the 14-case hand-maintained reconstruction
// switch — new server-side error types automatically reach the CLI with
// full structured info (Reason, all Metadata keys, original Message).
//
// Server code must NOT use RemoteError; it is the boundary representation
// for errors arriving FROM the network. Use the specific Describable types
// in this file on the server side.
type RemoteError struct {
	KindValue   ErrorKind
	ReasonValue string
	Message     string
	Meta        map[string]string
}

// Compile-time assertion: RemoteError sits outside the Err* naming pattern
// that TestEveryDomainErrorImplementsDescribable scans, so we pin the
// contract here explicitly.
var _ Describable = (*RemoteError)(nil)

func (e *RemoteError) Error() string               { return e.Message }
func (e *RemoteError) Reason() string              { return e.ReasonValue }
func (e *RemoteError) Metadata() map[string]string { return e.Meta }

// kindOverride makes domain.Kind return the kind observed off the wire
// (derived from the gRPC status code) rather than re-deriving it from Reason.
// This matters for forward compatibility: a client older than the server may
// receive a reason its ErrorReason enum does not know, which would otherwise
// collapse to KindInternal and lose the status the wire already carried.
func (e *RemoteError) kindOverride() ErrorKind { return e.KindValue }

// ErrFilterCompilation — query-filter compilation failure: schema-type
// mismatches (e.g. string condition on an int64 field) and prepared-query
// parameter parse errors (e.g. "cannot parse 'x' as int64"). Both are
// client-actionable. See WrapCompileError above.
type ErrFilterCompilation struct {
	Detail string
}

func (e *ErrFilterCompilation) Error() string { return "compiling filter: " + e.Detail }
func (*ErrFilterCompilation) Reason() string  { return ErrReasonFilterCompilation }
func (e *ErrFilterCompilation) Metadata() map[string]string {
	return map[string]string{"detail": e.Detail}
}

// ErrInvalidOrderType — the FSM received an Order with an unknown proto type.
// Reachable only via a protocol-version mismatch (a peer or client crafted
// a message with a oneof case the local build does not recognise). Kind is
// Internal because the local node cannot satisfy the request; the client
// is expected to upgrade. Replaces a former `errors.New("invalid order
// type")` defensive branch (processor.go).
type ErrInvalidOrderType struct {
	TypeName string
}

func (e *ErrInvalidOrderType) Error() string {
	return "invalid order type: " + e.TypeName
}
func (*ErrInvalidOrderType) Reason() string { return ErrReasonInvalidOrderType }
func (e *ErrInvalidOrderType) Metadata() map[string]string {
	return map[string]string{"typeName": e.TypeName}
}

// ErrIdempotencyCheckFailed — Pebble lookup for an idempotency key returned
// an unexpected I/O error (the key not being present is not an error;
// only storage-level failures land here). KindInternal: the caller cannot
// fix it. Error() returns a sanitised message — the underlying Cause is
// still available via Unwrap for server-side logging and correlation, but
// never reaches the wire (it could carry Pebble paths or invariant
// strings per #326).
type ErrIdempotencyCheckFailed struct {
	Cause error
}

func (*ErrIdempotencyCheckFailed) Error() string               { return "checking idempotency key" }
func (e *ErrIdempotencyCheckFailed) Unwrap() error             { return e.Cause }
func (*ErrIdempotencyCheckFailed) Reason() string              { return ErrReasonIdempotencyCheckFailed }
func (*ErrIdempotencyCheckFailed) Metadata() map[string]string { return nil }

// ErrInvalidApplyType — the FSM received a LedgerApplyOrder with an unknown
// inner proto type. Reachable only via a protocol-version mismatch on the
// Apply sub-types. Distinct from ErrInvalidOrderType which covers the outer
// Order oneof.
type ErrInvalidApplyType struct {
	TypeName string
}

func (e *ErrInvalidApplyType) Error() string { return "invalid apply type: " + e.TypeName }
func (*ErrInvalidApplyType) Reason() string  { return ErrReasonInvalidApplyType }
func (e *ErrInvalidApplyType) Metadata() map[string]string {
	return map[string]string{"typeName": e.TypeName}
}

// ErrStorageOperation wraps a Pebble (or other store) IO failure that
// surfaces during order processing. Kind is Internal: the caller cannot
// fix it. Operation is a short identifier ("checking transaction reference",
// "getting numscript latest version") emitted via Error() and Metadata —
// the underlying Cause is preserved for server-side logging via Unwrap but
// is NOT included in the wire message: it can carry Pebble paths or other
// internal storage details that must not reach the API (#326).
type ErrStorageOperation struct {
	Operation string
	Cause     error
}

func (e *ErrStorageOperation) Error() string { return "storage operation failed: " + e.Operation }
func (e *ErrStorageOperation) Unwrap() error { return e.Cause }
func (*ErrStorageOperation) Reason() string  { return ErrReasonStorageOperation }
func (e *ErrStorageOperation) Metadata() map[string]string {
	return map[string]string{"operation": e.Operation}
}

// ErrTransactionStateInconsistent — an invariant of the transaction
// state-tracker is violated (e.g. a transaction is in the index but its
// state is absent or malformed). KindInternal: a server bug or data
// corruption, not a client mistake.
type ErrTransactionStateInconsistent struct {
	TransactionID uint64
	Operation     string
}

func (e *ErrTransactionStateInconsistent) Error() string {
	return fmt.Sprintf("transaction %d state inconsistent (%s)", e.TransactionID, e.Operation)
}
func (*ErrTransactionStateInconsistent) Reason() string { return ErrReasonTransactionStateInconsistent }
func (e *ErrTransactionStateInconsistent) Metadata() map[string]string {
	return map[string]string{
		"transactionId": strconv.FormatUint(e.TransactionID, 10),
		"operation":     e.Operation,
	}
}

// ErrCheckpointIDRequired — the caller asked for a query checkpoint
// operation but did not pass an ID. KindValidation.
type errCheckpointIDRequired struct{}

func (errCheckpointIDRequired) Error() string               { return "checkpoint_id must be non-zero" }
func (errCheckpointIDRequired) Reason() string              { return ErrReasonCheckpointIDRequired }
func (errCheckpointIDRequired) Metadata() map[string]string { return nil }

var ErrCheckpointIDRequired Describable = errCheckpointIDRequired{}

// ErrNumscriptRuntime — the Numscript program produced output that violates
// a server-side invariant at apply time (negative posting amount, posting
// amount exceeding 2^256, malformed metadata key produced by a numscript
// expression, etc.). KindInternal because the user wrote the script and
// the FSM cannot recover; the message carries the diagnostic detail.
type ErrNumscriptRuntime struct {
	Detail string
}

func (e *ErrNumscriptRuntime) Error() string { return "numscript runtime error: " + e.Detail }
func (*ErrNumscriptRuntime) Reason() string  { return ErrReasonNumscriptRuntime }
func (e *ErrNumscriptRuntime) Metadata() map[string]string {
	return map[string]string{"detail": e.Detail}
}

// ErrVolumeNotMaterialized — a posting references a (Account, Asset) pair
// whose Input/Output volumes have not been fully fetched into the FSM's
// working set. KindInternal: indicates a preloading miss the admission
// layer should have caught; reaching this branch is a server bug.
type ErrVolumeNotMaterialized struct {
	Account string
	Asset   string
	Side    string // "source" or "destination"
}

func (e *ErrVolumeNotMaterialized) Error() string {
	return fmt.Sprintf("%s volume %s/%s not fully materialized", e.Side, e.Account, e.Asset)
}
func (*ErrVolumeNotMaterialized) Reason() string { return ErrReasonVolumeNotMaterialized }
func (e *ErrVolumeNotMaterialized) Metadata() map[string]string {
	return map[string]string{"account": e.Account, "asset": e.Asset, "side": e.Side}
}

// ErrMetadataKeyValidation wraps another Describable to add the metadata-key
// name that caused a value-level validation failure. Same shape as
// ErrAccountValidation but for the metadata-map iteration in
// validateMetadataMap / numscript-produced metadata. Lets operator logs and
// the client identify which specific key violated which sentinel rule
// instead of just naming the rule.
type ErrMetadataKeyValidation struct {
	Key   string
	Cause Describable
}

func (e *ErrMetadataKeyValidation) Error() string {
	return fmt.Sprintf("metadata key %q value: %s", e.Key, e.Cause.Error())
}
func (e *ErrMetadataKeyValidation) Unwrap() error  { return e.Cause }
func (e *ErrMetadataKeyValidation) Reason() string { return e.Cause.Reason() }
func (e *ErrMetadataKeyValidation) Metadata() map[string]string {
	out := map[string]string{"key": e.Key}

	maps.Copy(out, e.Cause.Metadata())

	return out
}

// ErrAccountValidation wraps another Describable to add the account-name
// context produced when iterating per-account metadata maps in admission.
// The inner Cause carries the original Kind/Reason; Metadata is the inner
// metadata merged with {"account": Account} so the client can format
// per-account validation messages without parsing the wrapped string.
type ErrAccountValidation struct {
	Account string
	Cause   Describable
}

func (e *ErrAccountValidation) Error() string {
	return fmt.Sprintf("account %q: %s", e.Account, e.Cause.Error())
}
func (e *ErrAccountValidation) Unwrap() error  { return e.Cause }
func (e *ErrAccountValidation) Reason() string { return e.Cause.Reason() }
func (e *ErrAccountValidation) Metadata() map[string]string {
	out := map[string]string{"account": e.Account}

	maps.Copy(out, e.Cause.Metadata())

	return out
}

// ErrInvalidExecutionPlan signals that the ExecutionPlan shipped by
// admission is structurally inconsistent with itself — a coverage_bits
// or production_bits bit flags a position past the slice it indexes,
// or an AttributeCoverage/Production declares an attr_code the FSM does
// not handle.
//
// Detected at scope construction in the FSM, BEFORE any cache mutation
// lands: returning an error lets the proposal be rejected as a business
// error without dirtying the in-memory cache. Categorized as
// KindInternal — the client can't fix it; the admission side that built
// the plan has the bug.
type ErrInvalidExecutionPlan struct {
	Reason_ string
}

func (e *ErrInvalidExecutionPlan) Error() string { return "invalid execution plan: " + e.Reason_ }
func (*ErrInvalidExecutionPlan) Reason() string  { return ErrReasonInvalidExecutionPlan }
func (e *ErrInvalidExecutionPlan) Metadata() map[string]string {
	return map[string]string{"reason": e.Reason_}
}

// ErrExecutionPlanTooLarge is raised by plan.Builder.Build when the
// aggregated ExecutionPlan exceeds the configured cap. The cap is a
// safeguard against pathological proposals (very large Numscript
// scripts, or a malicious payload) that would otherwise force NewScope
// to allocate proportionally-large coverage slices on the apply path.
//
// Categorized as KindValidation — the client can shrink the request
// (split the script, reduce the touched accounts) and retry.
type ErrExecutionPlanTooLarge struct {
	Size  int
	Limit int
}

func (e *ErrExecutionPlanTooLarge) Error() string {
	return fmt.Sprintf("execution plan too large: %d attributes (limit %d)", e.Size, e.Limit)
}
func (*ErrExecutionPlanTooLarge) Reason() string { return ErrReasonExecutionPlanTooLarge }
func (e *ErrExecutionPlanTooLarge) Metadata() map[string]string {
	return map[string]string{
		"size":  strconv.Itoa(e.Size),
		"limit": strconv.Itoa(e.Limit),
	}
}

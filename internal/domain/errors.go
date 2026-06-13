package domain

import (
	"errors"
	"fmt"
)

// ErrNotFound is a sentinel error for missing records in storage lookups.
var ErrNotFound = errors.New("not found")

// Reason constants shared between server and client for gRPC error mapping.
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
	ErrReasonBalanceNotFound               = "BALANCE_NOT_FOUND"
	ErrReasonBalanceNotPreloaded           = "BALANCE_NOT_PRELOADED"
	ErrReasonNumscriptParseError           = "NUMSCRIPT_PARSE_ERROR"
	ErrReasonValidation                    = "VALIDATION"
	ErrReasonAuditDisabled                 = "AUDIT_DISABLED"
	ErrReasonSinkAlreadyExists             = "SINK_ALREADY_EXISTS"
	ErrReasonSinkNotFound                  = "SINK_NOT_FOUND"
	ErrReasonSinkBatchSizeTooLarge         = "SINK_BATCH_SIZE_TOO_LARGE"
	ErrReasonNoPeriodOpen                  = "NO_PERIOD_OPEN"
	ErrReasonPeriodNotFound                = "PERIOD_NOT_FOUND"
	ErrReasonPeriodNotClosing              = "PERIOD_NOT_CLOSING"
	ErrReasonPeriodNotClosed               = "PERIOD_NOT_CLOSED"
	ErrReasonPeriodNotArchiving            = "PERIOD_NOT_ARCHIVING"
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
	ErrReasonClusterUnhealthy              = "CLUSTER_UNHEALTHY"
	ErrReasonTransientAccountNonZero       = "TRANSIENT_ACCOUNT_NON_ZERO"
	ErrReasonFilterCompilation             = "FILTER_COMPILATION_ERROR"
)

// BusinessError wraps a processing error to distinguish it from infrastructure errors.
// It flows through futures -> admission -> controller -> gRPC server, where the interceptor
// maps it to proper gRPC status codes.
type BusinessError struct {
	Err error
}

func (e *BusinessError) Error() string {
	return e.Err.Error()
}

func (e *BusinessError) Unwrap() error {
	return e.Err
}

// ErrColdStorageDisabled is returned when archiving is attempted but cold storage is not configured.
var ErrColdStorageDisabled = errors.New("cold storage is disabled: archiving is not available")

// Sentinel validation errors (no context needed).
var (
	ErrTargetRequired             = errors.New("target is required")
	ErrMetadataKeyRequired        = errors.New("key is required")
	ErrAuditDisabled              = errors.New("audit log is disabled on this server")
	ErrMaintenanceMode            = errors.New("cluster is in maintenance mode: write operations are blocked")
	ErrStaleProposal              = errors.New("proposal rejected: predicted index mismatch (stale tracker after leadership transition)")
	ErrNumscriptNameRequired      = errors.New("numscript name is required")
	ErrNumscriptContentRequired   = errors.New("numscript content is required")
	ErrScriptAndReferenceConflict = errors.New("cannot specify both script and scriptReference")
	ErrScriptRequired             = errors.New("numscript: script is required")
	ErrLedgerNameRequired         = errors.New("ledger name is required")
)

// ErrLedgerAlreadyExists is returned when attempting to create a ledger that already exists.
type ErrLedgerAlreadyExists struct {
	Name string
}

func (e *ErrLedgerAlreadyExists) Error() string {
	return "ledger already exists: " + e.Name
}

// ErrLedgerNotFound is returned when a referenced ledger does not exist.
type ErrLedgerNotFound struct {
	Name string
}

func (e *ErrLedgerNotFound) Error() string {
	return "ledger does not exist: " + e.Name
}

// ErrLedgerDeleted is returned when a write operation targets a soft-deleted ledger.
type ErrLedgerDeleted struct {
	Name string
}

func (e *ErrLedgerDeleted) Error() string {
	return "ledger has been deleted: " + e.Name
}

// ErrIdempotencyKeyConflict is returned when an idempotency key is reused with different content.
type ErrIdempotencyKeyConflict struct {
	Key string
}

func (e *ErrIdempotencyKeyConflict) Error() string {
	return fmt.Sprintf("idempotency key conflict: key %q used with different request content", e.Key)
}

// ErrTransactionReferenceConflict is returned when a transaction reference already exists in the same ledger.
type ErrTransactionReferenceConflict struct {
	Ledger    string
	Reference string
}

func (e *ErrTransactionReferenceConflict) Error() string {
	return fmt.Sprintf("transaction reference %q already exists in ledger %s", e.Reference, e.Ledger)
}

// ErrTransactionNotFound is returned when a transaction ID is beyond the known range.
type ErrTransactionNotFound struct {
	TransactionID uint64
}

func (e *ErrTransactionNotFound) Error() string {
	return fmt.Sprintf("transaction %d does not exist", e.TransactionID)
}

// ErrTransactionReferenceNotFound is returned when a transaction reference does not
// resolve to an existing transaction in the visible state (cache or current batch).
type ErrTransactionReferenceNotFound struct {
	Reference string
}

func (e *ErrTransactionReferenceNotFound) Error() string {
	return fmt.Sprintf("transaction with reference %q does not exist", e.Reference)
}

// ErrTransactionTargetMissing is returned when a TargetTransaction is empty
// (neither id nor reference set).
var ErrTransactionTargetMissing = errors.New("transaction target requires either id or reference")

// ErrTransactionAlreadyReverted is returned when attempting to revert an already-reverted transaction.
type ErrTransactionAlreadyReverted struct {
	TransactionID uint64
}

func (e *ErrTransactionAlreadyReverted) Error() string {
	return fmt.Sprintf("transaction %d is already reverted", e.TransactionID)
}

// ErrInsufficientFunds is returned when a source account does not have enough balance.
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

// ErrVolumeOverflow is returned when a posting would push an account's Input
// or Output volume past 2^256. Posting amounts are unbounded 256-bit values
// supplied by the API and the cumulative volumes are themselves uint256, so
// the addition can wrap silently. A wrap on the destination Input or on a
// `world` / `Force=true` source Output would silently create or destroy
// funds; the FSM rejects the order instead (#321).
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

// ErrBalanceNotFound is returned when the balance for a source account cannot be determined.
type ErrBalanceNotFound struct {
	Account string
	Asset   string
}

func (e *ErrBalanceNotFound) Error() string {
	return fmt.Sprintf("balance not found for account %q asset %q", e.Account, e.Asset)
}

// ErrSinkAlreadyExists is returned when adding a sink that already exists.
type ErrSinkAlreadyExists struct {
	Name string
}

func (e *ErrSinkAlreadyExists) Error() string {
	return "event sink already exists: " + e.Name
}

// MaxSinkBatchSize is the server-side cap on SinkConfig.BatchSize. The
// emitter uses BatchSize as the capacity of a per-flush slice; without a
// bound, a misconfigured value drives one large allocation per flush on the
// leader. 100_000 is several orders of magnitude beyond any sane sink
// throughput and is intended only to catch fat-finger CLI input or
// adversarial admin commands — not to enforce policy.
const MaxSinkBatchSize int32 = 100_000

// ErrSinkBatchSizeTooLarge is returned when an event sink is configured with
// a batch size that exceeds MaxSinkBatchSize.
type ErrSinkBatchSizeTooLarge struct {
	Name      string
	BatchSize int32
	Max       int32
}

func (e *ErrSinkBatchSizeTooLarge) Error() string {
	return fmt.Sprintf("event sink %q has batchSize=%d, exceeds maximum %d",
		e.Name, e.BatchSize, e.Max)
}

// ErrMetadataNotFound is returned when deleting a metadata key that does not exist.
type ErrMetadataNotFound struct {
	Target string
	Key    string
}

func (e *ErrMetadataNotFound) Error() string {
	return fmt.Sprintf("metadata key %q not found on %s", e.Key, e.Target)
}

// ErrSinkNotFound is returned when removing a sink that does not exist.
type ErrSinkNotFound struct {
	Name string
}

func (e *ErrSinkNotFound) Error() string {
	return "event sink not found: " + e.Name
}

// Period-related sentinel errors.
var (
	ErrNoPeriodOpen = errors.New("no open period exists")
)

// ErrPeriodNotFound is returned when a period ID does not match any known period.
type ErrPeriodNotFound struct {
	PeriodID uint64
}

func (e *ErrPeriodNotFound) Error() string {
	return fmt.Sprintf("period %d not found", e.PeriodID)
}

// ErrPeriodNotClosing is returned when attempting to seal a period that is not in CLOSING state.
type ErrPeriodNotClosing struct {
	PeriodID uint64
}

func (e *ErrPeriodNotClosing) Error() string {
	return fmt.Sprintf("period %d is not in CLOSING state", e.PeriodID)
}

// ErrPeriodNotClosed is returned when attempting to archive a period that is not in CLOSED state.
type ErrPeriodNotClosed struct {
	PeriodID uint64
}

func (e *ErrPeriodNotClosed) Error() string {
	return fmt.Sprintf("period %d is not in CLOSED state", e.PeriodID)
}

// ErrPeriodNotArchiving is returned when attempting to confirm archive of a period that is not in ARCHIVING state.
type ErrPeriodNotArchiving struct {
	PeriodID uint64
}

func (e *ErrPeriodNotArchiving) Error() string {
	return fmt.Sprintf("period %d is not in ARCHIVING state", e.PeriodID)
}

// ErrInvalidCronExpression is returned when a cron expression is invalid.
type ErrInvalidCronExpression struct {
	Expression string
	Details    string
}

func (e *ErrInvalidCronExpression) Error() string {
	return fmt.Sprintf("invalid cron expression %q: %s", e.Expression, e.Details)
}

// ErrLedgerInMirrorMode is returned when a write operation is attempted on a mirror-mode ledger.
type ErrLedgerInMirrorMode struct {
	Name string
}

func (e *ErrLedgerInMirrorMode) Error() string {
	return fmt.Sprintf("ledger %s is in mirror mode: write operations are blocked", e.Name)
}

// ErrLedgerNotInMirrorMode is returned when a mirror-only operation is attempted on a normal-mode ledger.
type ErrLedgerNotInMirrorMode struct {
	Name string
}

func (e *ErrLedgerNotInMirrorMode) Error() string {
	return fmt.Sprintf("ledger %s is not in mirror mode", e.Name)
}

// ErrPreparedQueryAlreadyExists is returned when creating a prepared query that already exists.
type ErrPreparedQueryAlreadyExists struct {
	Ledger string
	Name   string
}

func (e *ErrPreparedQueryAlreadyExists) Error() string {
	return fmt.Sprintf("prepared query %s/%s already exists", e.Ledger, e.Name)
}

// ErrPreparedQueryNotFound is returned when a prepared query does not exist.
type ErrPreparedQueryNotFound struct {
	Ledger string
	Name   string
}

func (e *ErrPreparedQueryNotFound) Error() string {
	return fmt.Sprintf("prepared query %s/%s not found", e.Ledger, e.Name)
}

// ErrIndexNotFound is returned when a query references an index that does not exist.
type ErrIndexNotFound struct {
	Index string
}

func (e *ErrIndexNotFound) Error() string {
	return "index not found: " + e.Index
}

// ErrMetadataFieldNotInSchema is returned when CreateIndex targets a metadata
// key that has not been declared via SetMetadataFieldType first.
type ErrMetadataFieldNotInSchema struct {
	Target string
	Key    string
}

func (e *ErrMetadataFieldNotInSchema) Error() string {
	return "metadata field not declared in schema: " + e.Target + "/" + e.Key
}

// ErrIndexBuilding is returned when a query references an index that is still being built.
type ErrIndexBuilding struct {
	Index string
}

func (e *ErrIndexBuilding) Error() string {
	return "index is still building: " + e.Index
}

// ErrIndexInconsistent is returned when the read path detects a structural
// inconsistency between the filter index and the per-ledger log index — for
// example a logID present in the filter index but missing or malformed in
// the log index. Surfacing this as an explicit error (rather than silently
// skipping the entry) prevents stale/corrupt indexes from producing
// truncated query results that the caller can't distinguish from a
// legitimate empty result.
type ErrIndexInconsistent struct {
	Index  string
	Detail string
}

func (e *ErrIndexInconsistent) Error() string {
	return fmt.Sprintf("index %s is inconsistent: %s", e.Index, e.Detail)
}

// ErrInvalidReceipt is returned when a JWT receipt fails verification.
type ErrInvalidReceipt struct {
	Reason string
}

func (e *ErrInvalidReceipt) Error() string {
	return "invalid receipt: " + e.Reason
}

// ErrNumscriptNotFound is returned when a referenced numscript does not exist in the library.
type ErrNumscriptNotFound struct {
	Name string
}

func (e *ErrNumscriptNotFound) Error() string {
	return "numscript not found: " + e.Name
}

// ErrNumscriptVersionAlreadyExists is returned when saving with a semver version that already exists.
type ErrNumscriptVersionAlreadyExists struct {
	Name    string
	Version string
}

func (e *ErrNumscriptVersionAlreadyExists) Error() string {
	return fmt.Sprintf("numscript %q version %s already exists", e.Name, e.Version)
}

// ErrNumscriptInvalidVersion is returned when the version string is not valid semver.
type ErrNumscriptInvalidVersion struct {
	Version string
}

func (e *ErrNumscriptInvalidVersion) Error() string {
	return fmt.Sprintf("invalid numscript version %q: must be semver (major.minor.patch) or \"latest\"", e.Version)
}

// ErrAccountNotMatchingType is returned when an account address doesn't match any active account type pattern.
type ErrAccountNotMatchingType struct {
	Address string
}

func (e *ErrAccountNotMatchingType) Error() string {
	return "account does not match any account type pattern: " + e.Address
}

// ErrAccountTypeNotFound is returned when a referenced account type does not exist.
type ErrAccountTypeNotFound struct {
	Name string
}

func (e *ErrAccountTypeNotFound) Error() string {
	return "account type not found: " + e.Name
}

// ErrAccountTypeAlreadyExists is returned when creating an account type with a name that already exists.
type ErrAccountTypeAlreadyExists struct {
	Name string
}

func (e *ErrAccountTypeAlreadyExists) Error() string {
	return "account type already exists: " + e.Name
}

// ErrAccountTypeConflict is returned when a new account type pattern conflicts
// with an existing pattern (same specificity and overlapping match space).
type ErrAccountTypeConflict struct {
	NewPattern      string
	ExistingName    string
	ExistingPattern string
}

func (e *ErrAccountTypeConflict) Error() string {
	return fmt.Sprintf("pattern %q conflicts with existing account type %q (pattern %q): ambiguous match possible",
		e.NewPattern, e.ExistingName, e.ExistingPattern)
}

// ErrInvalidPattern is returned when an account type pattern is syntactically invalid.
type ErrInvalidPattern struct {
	Pattern string
	Details string
}

func (e *ErrInvalidPattern) Error() string {
	return fmt.Sprintf("invalid pattern %q: %s", e.Pattern, e.Details)
}

// ErrAccountTypeHasAccounts is returned when removing an account type that still has matching accounts.
type ErrAccountTypeHasAccounts struct {
	Name string
}

func (e *ErrAccountTypeHasAccounts) Error() string {
	return fmt.Sprintf("account type %q still has matching accounts", e.Name)
}

// ErrNumscriptParse is returned when a Numscript program has syntax errors.
type ErrNumscriptParse struct {
	Details string
}

func (e *ErrNumscriptParse) Error() string {
	return "numscript parse error: " + e.Details
}

// ErrFilterCompilation wraps a query-filter compilation failure: schema-type
// mismatches (e.g. string condition on an int64 field) and prepared-query
// parameter parse errors (e.g. "cannot parse 'x' as int64"). Both are
// client-actionable. Without this typed wrap the raw fmt.Errorf fell into
// convertToGRPCError's default branch and was sanitized to codes.Unknown,
// stripping the message the client needs to fix its request (#326).
type ErrFilterCompilation struct {
	Detail string
}

func (e *ErrFilterCompilation) Error() string {
	return "compiling filter: " + e.Detail
}

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

// ErrBalanceNotPreloaded is returned when the balance for an account was not
// preloaded by the admission layer before script execution.
type ErrBalanceNotPreloaded struct {
	Account string
	Asset   string
}

func (e *ErrBalanceNotPreloaded) Error() string {
	return fmt.Sprintf("balance not preloaded for account %q asset %q", e.Account, e.Asset)
}

// ErrTransientAccountNonZero is returned when a transient account has non-zero balance at end of batch.
type ErrTransientAccountNonZero struct {
	Account string
	Asset   string
}

func (e *ErrTransientAccountNonZero) Error() string {
	return fmt.Sprintf("transient account %s/%s has non-zero balance at end of batch (input != output)", e.Account, e.Asset)
}

package domain

import (
	"strings"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// errorReasonPrefix is the common prefix of every ledgerpb.ErrorReason enum
// name. The enum value for a reason is errorReasonPrefix + the Reason() string,
// which makes ReasonCode/ReasonString a pure naming bijection — no
// hand-maintained lookup table to drift.
const errorReasonPrefix = "ERROR_REASON_"

// ReasonCode maps a Describable Reason() string to its wire-bound ErrorReason
// enum. An unknown reason yields ERROR_REASON_UNSPECIFIED — only reachable for
// a non-Describable error that escaped the typed pipeline (see
// state.buildAuditFailure).
//
// The zero value it returns for an unknown reason is indistinguishable from
// the enum's explicit UNSPECIFIED member. That is harmless for a locally
// raised Describable, whose Reason() is always an enum name
// (TestEveryDomainErrorImplementsDescribable pins it), but a decoder reading a
// reason off the wire must tell the two apart: use LookupReasonCode.
func ReasonCode(reason string) ledgerpb.ErrorReason {
	code, _ := LookupReasonCode(reason)

	return code
}

// LookupReasonCode maps a Reason() string to its wire-bound ErrorReason enum
// and reports whether the enum knows that name at all.
//
// It separates the two conditions ReasonCode collapses onto
// ERROR_REASON_UNSPECIFIED: a reason this build's enum does not know (a newer
// sender), and the explicit UNSPECIFIED member. No ledger error emits the
// latter — every Describable's Reason() names a real reason — so a decoder
// that receives it received something no ledger server sends, which is a
// protocol fault rather than a reason from the future.
func LookupReasonCode(reason string) (ledgerpb.ErrorReason, bool) {
	code, ok := ledgerpb.ErrorReason_value[errorReasonPrefix+reason]

	return ledgerpb.ErrorReason(code), ok
}

// ReasonString is the inverse of ReasonCode: the stable, client-facing Reason()
// identifier (the gRPC ErrorInfo.reason) for an ErrorReason enum value.
func ReasonString(code ledgerpb.ErrorReason) string {
	return strings.TrimPrefix(code.String(), errorReasonPrefix)
}

// KindForReason returns the semantic ErrorKind for a reason. It re-derives the
// kind a typed error reports from its reason alone, so a frozen idempotency
// failure replays under the error's current classification and the checker can
// verify the stored failure projection against the hash-chained AuditFailure
// without the kind ever being persisted. Every typed error's Kind() must agree
// with this switch — enforced by TestKindForReasonMatchesTypedErrors.
func KindForReason(code ledgerpb.ErrorReason) ErrorKind {
	//exhaustive:enforce
	switch code {
	case ledgerpb.ErrorReason_ERROR_REASON_VALIDATION,
		ledgerpb.ErrorReason_ERROR_REASON_NUMSCRIPT_PARSE_ERROR,
		ledgerpb.ErrorReason_ERROR_REASON_SINK_BATCH_SIZE_TOO_LARGE,
		ledgerpb.ErrorReason_ERROR_REASON_INVALID_CRON_EXPRESSION,
		ledgerpb.ErrorReason_ERROR_REASON_NUMSCRIPT_INVALID_VERSION,
		ledgerpb.ErrorReason_ERROR_REASON_INVALID_PATTERN,
		ledgerpb.ErrorReason_ERROR_REASON_FILTER_COMPILATION_ERROR,
		ledgerpb.ErrorReason_ERROR_REASON_EXECUTION_PLAN_TOO_LARGE,
		ledgerpb.ErrorReason_ERROR_REASON_CHECKPOINT_ID_REQUIRED,
		ledgerpb.ErrorReason_ERROR_REASON_CLUSTER_POLICY_INVALID,
		// A metadata-limit violation is the caller sending too much metadata,
		// not the server exhausting a resource: Validation (InvalidArgument /
		// HTTP 400), never ResourceExhausted. Retrying the same payload cannot
		// succeed, and a retryable code would make client retry policies
		// re-drive a permanent rejection.
		ledgerpb.ErrorReason_ERROR_REASON_METADATA_LIMIT_EXCEEDED,
		// Reverting a transaction the same batch creates is a property of how
		// the caller composed the batch, not of ledger state: admission cannot
		// declare the volume coverage apply will need. The whole batch is
		// rejected, so the create never lands and re-admitting the identical
		// batch reproduces the same observation — a retryable code would spin
		// the client forever.
		ledgerpb.ErrorReason_ERROR_REASON_REVERT_TARGET_CREATED_IN_BATCH:
		return KindValidation
	case ledgerpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_SINK_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_PREPARED_QUERY_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_NUMSCRIPT_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_CHECKPOINT_NOT_FOUND:
		return KindNotFound
	case ledgerpb.ErrorReason_ERROR_REASON_LEDGER_ALREADY_EXISTS,
		ledgerpb.ErrorReason_ERROR_REASON_INDEX_ALREADY_EXISTS,
		ledgerpb.ErrorReason_ERROR_REASON_IDEMPOTENCY_KEY_CONFLICT,
		ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		ledgerpb.ErrorReason_ERROR_REASON_SINK_ALREADY_EXISTS,
		ledgerpb.ErrorReason_ERROR_REASON_PREPARED_QUERY_ALREADY_EXISTS,
		ledgerpb.ErrorReason_ERROR_REASON_NUMSCRIPT_VERSION_ALREADY_EXISTS,
		ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS:
		return KindAlreadyExists
	case ledgerpb.ErrorReason_ERROR_REASON_LEDGER_DELETED,
		ledgerpb.ErrorReason_ERROR_REASON_SINK_CONTROLLER_MISMATCH,
		ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED,
		ledgerpb.ErrorReason_ERROR_REASON_LEDGER_IN_MIRROR_MODE,
		ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_HAS_ACCOUNTS,
		ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_CONFLICT,
		ledgerpb.ErrorReason_ERROR_REASON_STALE_CLUSTER_POLICY:
		return KindConflict
	case ledgerpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
		ledgerpb.ErrorReason_ERROR_REASON_VOLUME_OVERFLOW,
		ledgerpb.ErrorReason_ERROR_REASON_AGGREGATE_OVERFLOW,
		ledgerpb.ErrorReason_ERROR_REASON_BALANCE_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_AUDIT_DISABLED,
		ledgerpb.ErrorReason_ERROR_REASON_LEDGER_NOT_IN_MIRROR_MODE,
		ledgerpb.ErrorReason_ERROR_REASON_INDEX_NOT_FOUND,
		ledgerpb.ErrorReason_ERROR_REASON_METADATA_FIELD_NOT_IN_SCHEMA,
		ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_NOT_MATCHING_TYPE,
		ledgerpb.ErrorReason_ERROR_REASON_TRANSIENT_ACCOUNT_NON_ZERO,
		ledgerpb.ErrorReason_ERROR_REASON_CHECKPOINT_LIMIT_REACHED:
		return KindPrecondition
	case ledgerpb.ErrorReason_ERROR_REASON_BALANCE_NOT_PRELOADED,
		ledgerpb.ErrorReason_ERROR_REASON_MAINTENANCE_MODE,
		ledgerpb.ErrorReason_ERROR_REASON_STALE_PROPOSAL,
		ledgerpb.ErrorReason_ERROR_REASON_STALE_INPUTS_RESOLUTION,
		ledgerpb.ErrorReason_ERROR_REASON_PRELOAD_UNAVAILABLE,
		ledgerpb.ErrorReason_ERROR_REASON_INDEX_BUILDING,
		ledgerpb.ErrorReason_ERROR_REASON_CHECKPOINT_NOT_READY,
		ledgerpb.ErrorReason_ERROR_REASON_CLUSTER_UNHEALTHY,
		ledgerpb.ErrorReason_ERROR_REASON_WRITES_BLOCKED_CLOCK_SKEW:
		return KindUnavailable
	case ledgerpb.ErrorReason_ERROR_REASON_WRITES_BLOCKED_DISK_FULL,
		ledgerpb.ErrorReason_ERROR_REASON_SEQUENCE_EXHAUSTED:
		return KindResourceExhausted
	case ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		ledgerpb.ErrorReason_ERROR_REASON_INDEX_INCONSISTENT,
		ledgerpb.ErrorReason_ERROR_REASON_INVALID_ORDER_TYPE,
		ledgerpb.ErrorReason_ERROR_REASON_INVALID_APPLY_TYPE,
		ledgerpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN,
		ledgerpb.ErrorReason_ERROR_REASON_COVERAGE_MISS,
		ledgerpb.ErrorReason_ERROR_REASON_IDEMPOTENCY_CHECK_FAILED,
		ledgerpb.ErrorReason_ERROR_REASON_STORAGE_OPERATION_FAILED,
		ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_STATE_INCONSISTENT,
		ledgerpb.ErrorReason_ERROR_REASON_NUMSCRIPT_RUNTIME,
		ledgerpb.ErrorReason_ERROR_REASON_MIRROR_V2_LOG_ID_GAP,
		ledgerpb.ErrorReason_ERROR_REASON_MIRROR_V2_LOG_ID_INVALID,
		ledgerpb.ErrorReason_ERROR_REASON_VOLUME_NOT_MATERIALIZED,
		ledgerpb.ErrorReason_ERROR_REASON_CLUSTER_POLICY_REVISION_CONFLICT:
		return KindInternal
	default:
		return KindInternal
	}
}

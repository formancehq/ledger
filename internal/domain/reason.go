package domain

import (
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// errorReasonPrefix is the common prefix of every commonpb.ErrorReason enum
// name. The enum value for a reason is errorReasonPrefix + the Reason() string,
// which makes ReasonCode/ReasonString a pure naming bijection — no
// hand-maintained lookup table to drift.
const errorReasonPrefix = "ERROR_REASON_"

// ReasonCode maps a Describable Reason() string to its wire-bound ErrorReason
// enum. An unknown reason yields ERROR_REASON_UNSPECIFIED — only reachable for
// a non-Describable error that escaped the typed pipeline (see
// state.buildAuditFailure).
func ReasonCode(reason string) commonpb.ErrorReason {
	return commonpb.ErrorReason(commonpb.ErrorReason_value[errorReasonPrefix+reason])
}

// ReasonString is the inverse of ReasonCode: the stable, client-facing Reason()
// identifier (the gRPC ErrorInfo.reason) for an ErrorReason enum value.
func ReasonString(code commonpb.ErrorReason) string {
	return strings.TrimPrefix(code.String(), errorReasonPrefix)
}

// Kind returns the semantic ErrorKind of a Describable. Kind is a function of
// the error's reason — KindForReason(ReasonCode(d.Reason())) — so it lives in
// exactly one place (the KindForReason switch) and is never duplicated per
// type. A BusinessError is unwrapped to its inner error. A type that observed
// its kind off the wire instead of deriving it (RemoteError, reconstructed from
// a gRPC status on the client) overrides via kindOverride.
func Kind(d Describable) ErrorKind {
	if o, ok := d.(interface{ kindOverride() ErrorKind }); ok {
		return o.kindOverride()
	}

	if be, ok := d.(*BusinessError); ok {
		return Kind(be.Err)
	}

	return KindForReason(ReasonCode(d.Reason()))
}

// KindForReason returns the semantic ErrorKind for a reason. It re-derives the
// kind a typed error reports from its reason alone, so a frozen idempotency
// failure replays under the error's current classification and the checker can
// verify the stored failure projection against the hash-chained AuditFailure
// without the kind ever being persisted. Every typed error's Kind() must agree
// with this switch — enforced by TestKindForReasonMatchesTypedErrors.
func KindForReason(code commonpb.ErrorReason) ErrorKind {
	//exhaustive:enforce
	switch code {
	case commonpb.ErrorReason_ERROR_REASON_VALIDATION,
		commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_PARSE_ERROR,
		commonpb.ErrorReason_ERROR_REASON_SINK_BATCH_SIZE_TOO_LARGE,
		commonpb.ErrorReason_ERROR_REASON_INVALID_RECEIPT,
		commonpb.ErrorReason_ERROR_REASON_INVALID_CRON_EXPRESSION,
		commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_INVALID_VERSION,
		commonpb.ErrorReason_ERROR_REASON_INVALID_PATTERN,
		commonpb.ErrorReason_ERROR_REASON_FILTER_COMPILATION_ERROR,
		commonpb.ErrorReason_ERROR_REASON_EXECUTION_PLAN_TOO_LARGE,
		commonpb.ErrorReason_ERROR_REASON_CHECKPOINT_ID_REQUIRED:
		return KindValidation
	case commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_SINK_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_CHAPTER_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_PREPARED_QUERY_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND:
		return KindNotFound
	case commonpb.ErrorReason_ERROR_REASON_LEDGER_ALREADY_EXISTS,
		commonpb.ErrorReason_ERROR_REASON_IDEMPOTENCY_KEY_CONFLICT,
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		commonpb.ErrorReason_ERROR_REASON_SINK_ALREADY_EXISTS,
		commonpb.ErrorReason_ERROR_REASON_PREPARED_QUERY_ALREADY_EXISTS,
		commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_VERSION_ALREADY_EXISTS,
		commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS:
		return KindAlreadyExists
	case commonpb.ErrorReason_ERROR_REASON_LEDGER_DELETED,
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED,
		commonpb.ErrorReason_ERROR_REASON_LEDGER_IN_MIRROR_MODE,
		commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_HAS_ACCOUNTS,
		commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_CONFLICT:
		return KindConflict
	case commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
		commonpb.ErrorReason_ERROR_REASON_VOLUME_OVERFLOW,
		commonpb.ErrorReason_ERROR_REASON_AUDIT_DISABLED,
		commonpb.ErrorReason_ERROR_REASON_NO_CHAPTER_OPEN,
		commonpb.ErrorReason_ERROR_REASON_CHAPTER_NOT_CLOSING,
		commonpb.ErrorReason_ERROR_REASON_CHAPTER_NOT_CLOSED,
		commonpb.ErrorReason_ERROR_REASON_CHAPTER_NOT_ARCHIVING,
		commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_IN_MIRROR_MODE,
		commonpb.ErrorReason_ERROR_REASON_INDEX_NOT_FOUND,
		commonpb.ErrorReason_ERROR_REASON_METADATA_FIELD_NOT_IN_SCHEMA,
		commonpb.ErrorReason_ERROR_REASON_ACCOUNT_NOT_MATCHING_TYPE,
		commonpb.ErrorReason_ERROR_REASON_COLD_STORAGE_DISABLED,
		commonpb.ErrorReason_ERROR_REASON_TRANSIENT_ACCOUNT_NON_ZERO:
		return KindPrecondition
	case commonpb.ErrorReason_ERROR_REASON_BALANCE_NOT_PRELOADED,
		commonpb.ErrorReason_ERROR_REASON_MAINTENANCE_MODE,
		commonpb.ErrorReason_ERROR_REASON_STALE_PROPOSAL,
		commonpb.ErrorReason_ERROR_REASON_STALE_INPUTS_RESOLUTION,
		commonpb.ErrorReason_ERROR_REASON_INDEX_BUILDING,
		commonpb.ErrorReason_ERROR_REASON_CHECKPOINT_NOT_READY,
		commonpb.ErrorReason_ERROR_REASON_CLUSTER_UNHEALTHY,
		commonpb.ErrorReason_ERROR_REASON_WRITES_BLOCKED_CLOCK_SKEW:
		return KindUnavailable
	case commonpb.ErrorReason_ERROR_REASON_WRITES_BLOCKED_DISK_FULL:
		return KindResourceExhausted
	case commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		commonpb.ErrorReason_ERROR_REASON_INDEX_INCONSISTENT,
		commonpb.ErrorReason_ERROR_REASON_INVALID_ORDER_TYPE,
		commonpb.ErrorReason_ERROR_REASON_INVALID_APPLY_TYPE,
		commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN,
		commonpb.ErrorReason_ERROR_REASON_COVERAGE_MISS,
		commonpb.ErrorReason_ERROR_REASON_IDEMPOTENCY_CHECK_FAILED,
		commonpb.ErrorReason_ERROR_REASON_STORAGE_OPERATION_FAILED,
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_STATE_INCONSISTENT,
		commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_RUNTIME,
		commonpb.ErrorReason_ERROR_REASON_VOLUME_NOT_MATERIALIZED:
		return KindInternal
	default:
		return KindInternal
	}
}

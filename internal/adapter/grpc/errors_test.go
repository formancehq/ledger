package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// testLogger is the silent logger used by convertToGRPCError tests.
func testLogger() logging.Logger { return logging.Testing() }

// TestHandlePanic_DoesNotLeakStackToClient pins the fix for #326.
// The recovery interceptor used to embed the raw panic value AND the
// full stack trace (with file paths, goroutine state) in the gRPC
// error message. handlePanic now returns a sanitized codes.Internal
// carrying only a correlation ID.
func TestHandlePanic_DoesNotLeakStackToClient(t *testing.T) {
	t.Parallel()

	const secretInternal = "panic message leaking /internal/path"
	stack := []byte("goroutine 1 [running]:\nmain.veryRevealingFunctionName(...)\n\t/build/ledger/internal/secret.go:42")

	grpcErr := handlePanic(context.Background(), testLogger(), secretInternal, stack)

	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
	require.NotContains(t, st.Message(), secretInternal,
		"raw panic value MUST NOT leak through to the client (#326)")
	require.NotContains(t, st.Message(), "goroutine",
		"stack trace MUST NOT leak through to the client (#326)")
	require.NotContains(t, st.Message(), "/build",
		"file paths MUST NOT leak through to the client (#326)")
	require.Contains(t, st.Message(), "correlation ID")
}

func TestBusinessErrorToGRPCStatus_LedgerAlreadyExists(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrLedgerAlreadyExists{Name: "my-ledger"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())
	require.Contains(t, st.Message(), "my-ledger")

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonLedgerAlreadyExists, info.GetReason())
	require.Equal(t, errorDomain, info.GetDomain())
	require.Equal(t, "my-ledger", info.GetMetadata()["name"])
}

func TestBusinessErrorToGRPCStatus_LedgerNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: "missing-ledger"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonLedgerNotFound, info.GetReason())
	require.Equal(t, "missing-ledger", info.GetMetadata()["name"])
}

func TestBusinessErrorToGRPCStatus_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrIdempotencyKeyConflict{Key: "ik-123"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonIdempotencyKeyConflict, info.GetReason())
	require.Equal(t, "ik-123", info.GetMetadata()["key"])
}

func TestBusinessErrorToGRPCStatus_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionReferenceConflict{
		Ledger:    "test",
		Reference: "ref-001",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionReferenceConflict, info.GetReason())
	require.Equal(t, "test", info.GetMetadata()["ledger"])
	require.Equal(t, "ref-001", info.GetMetadata()["reference"])
}

func TestBusinessErrorToGRPCStatus_TransactionReferenceNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionReferenceNotFound{Reference: "invoice:42"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionReferenceNotFound, info.GetReason())
	require.Equal(t, "invoice:42", info.GetMetadata()["reference"])
}

func TestBusinessErrorToGRPCStatus_TransactionNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionNotFound{TransactionID: 999}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionNotFound, info.GetReason())
	require.Equal(t, "999", info.GetMetadata()["transactionId"])
}

func TestBusinessErrorToGRPCStatus_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionAlreadyReverted{TransactionID: 42}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionAlreadyReverted, info.GetReason())
	require.Equal(t, "42", info.GetMetadata()["transactionId"])
}

func TestBusinessErrorToGRPCStatus_InsufficientFunds(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrInsufficientFunds{
		Account: "user:001",
		Asset:   "USD",
		Amount:  "1000",
		Balance: "500",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonInsufficientFunds, info.GetReason())
	require.Equal(t, "user:001", info.GetMetadata()["account"])
	require.Equal(t, "USD", info.GetMetadata()["asset"])
	require.Equal(t, "1000", info.GetMetadata()["amount"])
	require.Equal(t, "500", info.GetMetadata()["balance"])
}

func TestBusinessErrorToGRPCStatus_BalanceNotPreloaded(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrBalanceNotPreloaded{
		Account: "user:003",
		Asset:   "BTC",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	// Unavailable, not FailedPrecondition: a preload miss is a transient
	// server-side gap the caller should retry, not a precondition they failed.
	require.Equal(t, codes.Unavailable, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonBalanceNotPreloaded, info.GetReason())
	require.Equal(t, "user:003", info.GetMetadata()["account"])
	require.Equal(t, "BTC", info.GetMetadata()["asset"])
}

func TestBusinessErrorToGRPCStatus_NumscriptParseError(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrNumscriptParse{Details: "unexpected token at line 3"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonNumscriptParseError, info.GetReason())
	require.Equal(t, "unexpected token at line 3", info.GetMetadata()["details"])
}

func TestBusinessErrorToGRPCStatus_FilterCompilationError(t *testing.T) {
	t.Parallel()

	// Two flavors of filter-compile error must both reach the client as
	// InvalidArgument so callers can fix their request: schema-type mismatch
	// (covers tests/e2e/business/filter_schema_validation_test.go) and
	// prepared-query parameter parse (covers prepared_queries_test.go).
	tests := []struct {
		name   string
		detail string
	}{
		{
			name:   "schema type mismatch",
			detail: `field "age" is declared as METADATA_TYPE_INT64, cannot use string condition`,
		},
		{
			name:   "parameter parse",
			detail: `parameter "min": cannot parse "not-a-number" as int64: strconv.ParseInt: parsing "not-a-number": invalid syntax`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bizErr := &domain.BusinessError{Err: &domain.ErrFilterCompilation{Detail: tt.detail}}
			st := businessErrorToGRPCStatus(bizErr)

			require.Equal(t, codes.InvalidArgument, st.Code())
			require.Contains(t, st.Message(), tt.detail)

			info := extractErrorInfo(t, st)
			require.Equal(t, domain.ErrReasonFilterCompilation, info.GetReason())
			require.Equal(t, errorDomain, info.GetDomain())
			require.Equal(t, tt.detail, info.GetMetadata()["detail"])
		})
	}
}

func TestBusinessErrorToGRPCStatus_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  domain.Describable
	}{
		{"target required", domain.ErrTargetRequired},
		{"metadata key required", domain.ErrMetadataKeyRequired},
		{"script required", domain.ErrScriptRequired},
		{"transaction target missing", domain.ErrTransactionTargetMissing},
		{"ledger name required", domain.ErrLedgerNameRequired},
		{"envelopes required", errEnvelopesRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bizErr := &domain.BusinessError{Err: tt.err}
			st := businessErrorToGRPCStatus(bizErr)

			require.Equal(t, codes.InvalidArgument, st.Code())

			info := extractErrorInfo(t, st)
			require.Equal(t, domain.ErrReasonValidation, info.GetReason())
			require.Equal(t, errorDomain, info.GetDomain())
		})
	}
}

func TestBusinessErrorToGRPCStatus_NumscriptDependencyDiscoveryFailed(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{
		Err: &domain.ErrDependencyDiscoveryFailed{Cause: errors.New("non-deterministic script")},
	}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonValidation, info.GetReason())
	require.Contains(t, info.GetMetadata()["details"], "numscript dependency discovery failed")
	require.Contains(t, info.GetMetadata()["details"], "non-deterministic script")
}

func TestBusinessErrorToGRPCStatus_NumscriptDependencyDiscoveryFailedWithParseCause(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{
		Err: &domain.ErrDependencyDiscoveryFailed{Cause: &domain.ErrNumscriptParse{Details: "syntax error"}},
	}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonNumscriptParseError, info.GetReason())
	require.Contains(t, info.GetMetadata()["details"], "numscript dependency discovery failed")
	require.Contains(t, info.GetMetadata()["details"], "numscript parse error")
}

func TestBusinessErrorToGRPCStatus_SinkAlreadyExists(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrSinkAlreadyExists{Name: "my-sink"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonSinkAlreadyExists, info.GetReason())
	require.Equal(t, "my-sink", info.GetMetadata()["name"])
}

func TestBusinessErrorToGRPCStatus_SinkNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrSinkNotFound{Name: "missing-sink"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonSinkNotFound, info.GetReason())
	require.Equal(t, "missing-sink", info.GetMetadata()["name"])
}

func TestBusinessErrorToGRPCStatus_MetadataNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrMetadataNotFound{Target: "account:foo", Key: "role"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonMetadataNotFound, info.GetReason())
	require.Equal(t, "account:foo", info.GetMetadata()["target"])
	require.Equal(t, "role", info.GetMetadata()["key"])
}

func TestBusinessErrorToGRPCStatus_MetadataFieldNotInSchema(t *testing.T) {
	t.Parallel()

	// CreateIndex on an undeclared field is a caller precondition error, not an
	// internal fault — must be FailedPrecondition, not the Internal default.
	bizErr := &domain.BusinessError{Err: &domain.ErrMetadataFieldNotInSchema{
		Target: "TARGET_TYPE_ACCOUNT",
		Key:    "idx-key-33",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonMetadataFieldNotInSchema, info.GetReason())
	require.Equal(t, "TARGET_TYPE_ACCOUNT", info.GetMetadata()["target"])
	require.Equal(t, "idx-key-33", info.GetMetadata()["key"])
}

func TestBusinessErrorToGRPCStatus_NoChapterOpen(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: domain.ErrNoChapterOpen}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonNoChapterOpen, info.GetReason())
}

func TestBusinessErrorToGRPCStatus_ChapterNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrChapterNotFound{ChapterID: 7}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonChapterNotFound, info.GetReason())
	require.Equal(t, "7", info.GetMetadata()["chapterId"])
}

func TestBusinessErrorToGRPCStatus_ChapterNotClosing(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrChapterNotClosing{ChapterID: 3}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonChapterNotClosing, info.GetReason())
	require.Equal(t, "3", info.GetMetadata()["chapterId"])
}

func TestBusinessErrorToGRPCStatus_InvalidReceipt(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrInvalidReceipt{Detail: "bad signature"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonInvalidReceipt, info.GetReason())
	require.Equal(t, "bad signature", info.GetMetadata()["reason"])
}

func TestBusinessErrorToGRPCStatus_MaintenanceMode(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: domain.ErrMaintenanceMode}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.Unavailable, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonMaintenanceMode, info.GetReason())
}

func TestBusinessErrorToGRPCStatus_InvalidCronExpression(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrInvalidCronExpression{
		Expression: "* * * *",
		Details:    "expected 5 fields",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonInvalidCronExpression, info.GetReason())
	require.Equal(t, "* * * *", info.GetMetadata()["expression"])
	require.Equal(t, "expected 5 fields", info.GetMetadata()["details"])
}

func TestBusinessErrorToGRPCStatus_AccountNotMatchingType(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrAccountNotMatchingType{Address: "invalid:addr:here"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonAccountNotMatchingType, info.GetReason())
	require.Equal(t, errorDomain, info.GetDomain())
	require.Equal(t, "invalid:addr:here", info.GetMetadata()["address"])
}

func TestConvertToGRPCError_BusinessError(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: "test"}}
	grpcErr := convertToGRPCError(bizErr, testLogger())

	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

// TestConvertToGRPCError_UnknownErrorIsSanitized pins the fix for #326.
// Unmapped errors used to be returned verbatim, leaking internal error
// strings (Pebble messages, file paths, invariant text) to API clients.
// The default branch now returns a generic codes.Unknown with a
// correlation ID, and logs the raw error server-side.
func TestConvertToGRPCError_UnknownErrorIsSanitized(t *testing.T) {
	t.Parallel()

	const secretInternalDetail = "pebble: block /var/lib/ledger/db/000123.sst checksum mismatch"
	err := errors.New(secretInternalDetail)

	grpcErr := convertToGRPCError(err, testLogger())

	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unknown, st.Code())
	require.NotContains(t, st.Message(), secretInternalDetail,
		"raw internal error string MUST NOT leak through to the client (#326)")
	require.Contains(t, st.Message(), "correlation ID")
}

// TestConvertToGRPCError_BareValidationSentinels pins EN-1253: request
// validation sentinels returned raw (not wrapped in BusinessError) by the
// bucket-service handlers must surface as codes.InvalidArgument, not the
// codes.Unknown of the default sanitizer. These are the exact sentinels the
// server_bucket.go guards now return for empty ledger name, empty envelope
// list, and empty metadata key.
func TestConvertToGRPCError_BareValidationSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ledger name required", domain.ErrLedgerNameRequired},
		{"envelopes required", errEnvelopesRequired},
		{"metadata key required", domain.ErrMetadataKeyRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grpcErr := convertToGRPCError(tt.err, testLogger())

			st, ok := status.FromError(grpcErr)
			require.True(t, ok)
			require.Equal(t, codes.InvalidArgument, st.Code())
			require.Equal(t, tt.err.Error(), st.Message())

			info := extractErrorInfo(t, st)
			require.Equal(t, domain.ErrReasonValidation, info.GetReason())
			require.Equal(t, errorDomain, info.GetDomain())
		})
	}
}

// TestConvertToGRPCError_BackupInProgress pins the mapping of the
// FSM's backup busy-destination sentinel to a stable retry code. Before
// the fix, the cluster handlers wrapped state.ErrBackupInProgress in a
// plain fmt.Errorf — convertToGRPCError had no mapping, fell through to
// the default sanitizer, and clients got codes.Unknown for a perfectly
// normal "another backup is running" condition. Now the sentinel
// surfaces as FailedPrecondition both raw and wrapped.
func TestConvertToGRPCError_BackupInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"in-progress raw", state.ErrBackupInProgress},
		{"in-progress wrapped", fmt.Errorf("backup already in progress for this destination: %w", state.ErrBackupInProgress)},
		{"jobID collision raw", state.ErrBackupJobIDCollision},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grpcErr := convertToGRPCError(tt.err, testLogger())
			st, ok := status.FromError(grpcErr)
			require.True(t, ok)
			require.Equal(t, codes.FailedPrecondition, st.Code(),
				"backup-busy sentinels must surface as FailedPrecondition, not %v", st.Code())
		})
	}
}

// TestConvertToGRPCError_NodeNotReachable pins EN-1416: when the forwarder
// fails to resolve a peer (conn-pool entry missing during a partition or
// pod restart), the error must surface as codes.Unavailable so client
// retry logic and the antithesis IsTransient predicate cover it the same
// way as the other peer-transport transients. Before the fix the bare
// fmt.Errorf("node N not reachable") fell through to the codes.Unknown
// sanitizer and looked like a permanent server bug.
func TestConvertToGRPCError_NodeNotReachable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"sentinel raw", ErrNodeNotReachable},
		{"wrapped from resolve", fmt.Errorf("%w: node 3", ErrNodeNotReachable)},
		{"wrapped twice", fmt.Errorf("forward: %w", fmt.Errorf("%w: node 7", ErrNodeNotReachable))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grpcErr := convertToGRPCError(tt.err, testLogger())
			st, ok := status.FromError(grpcErr)
			require.True(t, ok)
			require.Equal(t, codes.Unavailable, st.Code(),
				"ErrNodeNotReachable must surface as Unavailable, not %v", st.Code())
		})
	}
}

// TestConvertToGRPCError_NoFullCheckpoint pins EN-888: an incremental backup
// attempted against a destination that has no full checkpoint must surface as
// codes.FailedPrecondition (run a full backup first), not the opaque
// codes.Unknown sanitizer. The IncrementalBackup handler wraps the runner error
// with fmt.Errorf, so the wrapped form must map too.
func TestConvertToGRPCError_NoFullCheckpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"raw", backup.ErrNoFullCheckpoint},
		{"wrapped by handler", fmt.Errorf("incremental backup failed: %w", backup.ErrNoFullCheckpoint)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grpcErr := convertToGRPCError(tt.err, testLogger())
			st, ok := status.FromError(grpcErr)
			require.True(t, ok)
			require.Equal(t, codes.FailedPrecondition, st.Code(),
				"ErrNoFullCheckpoint must surface as FailedPrecondition, not %v", st.Code())
			require.NotContains(t, st.Message(), "correlation ID",
				"must not fall through to the opaque Unknown sanitizer")
		})
	}
}

// TestConvertToGRPCError_CheckpointNotReady pins EN-1460: a read that targets a
// query checkpoint whose read index has not been materialized yet (async
// index-builder work, or a follower node lagging) must surface as a typed,
// retryable codes.Unavailable with reason CHECKPOINT_NOT_READY — never the
// opaque, non-retryable codes.Unknown of the default sanitizer. Covers both the
// raw sentinel and the wrapped form the read handlers may return.
func TestConvertToGRPCError_CheckpointNotReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"raw", &domain.ErrCheckpointNotReady{CheckpointID: 27}},
		{"wrapped", fmt.Errorf("serving read: %w", &domain.ErrCheckpointNotReady{CheckpointID: 27})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grpcErr := convertToGRPCError(tt.err, testLogger())
			st, ok := status.FromError(grpcErr)
			require.True(t, ok)
			require.Equal(t, codes.Unavailable, st.Code(),
				"checkpoint-not-ready must surface as Unavailable, not %v", st.Code())
			require.NotContains(t, st.Message(), "correlation ID",
				"must not fall through to the opaque Unknown sanitizer")

			info := extractErrorInfo(t, st)
			require.Equal(t, domain.ErrReasonCheckpointNotReady, info.GetReason())
			require.Equal(t, "27", info.GetMetadata()["checkpointId"])
		})
	}
}

func TestConvertToGRPCError_MissingSignature(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(signing.ErrMissingSignature, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}

func TestConvertToGRPCError_InvalidSignature(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(signing.ErrInvalidSignature, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
}

func TestConvertToGRPCError_UnknownKeyID(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(signing.ErrUnknownKeyID, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
}

func TestConvertToGRPCError_MaintenanceMode(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(admission.ErrMaintenanceMode, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

func TestConvertToGRPCError_NoLeader(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(commonpb.ErrNoLeader, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

func TestConvertToGRPCError_LeadershipLost(t *testing.T) {
	t.Parallel()

	// Truncated by a later term => did not commit => retryable. Surfaces wrapped
	// as "applying raft requests: %w" from the controller, so cover that too.
	for _, err := range []error{
		node.ErrLeadershipLost,
		fmt.Errorf("applying raft requests: %w", node.ErrLeadershipLost),
	} {
		grpcErr := convertToGRPCError(err, testLogger())
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		require.Equal(t, codes.Unavailable, st.Code())
	}
}

func TestConvertToGRPCError_NotFoundError(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(commonpb.NewNotFoundError("ledger %s not found", "test"), testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestConvertToGRPCError_AuditDisabled(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(domain.ErrAuditDisabled, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_ChapterNotClosed(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(&domain.ErrChapterNotClosed{ChapterID: 5}, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_ChapterNotArchiving(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(&domain.ErrChapterNotArchiving{ChapterID: 3}, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_ColdStorageDisabled(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(domain.ErrColdStorageDisabled, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_WritesBlockedDiskFull(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(domain.ErrWritesBlockedDiskFull, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.ResourceExhausted, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonWritesBlockedDiskFull, info.GetReason())
	require.Equal(t, errorDomain, info.GetDomain())
}

func TestConvertToGRPCError_WritesBlockedClockSkew(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(domain.ErrWritesBlockedClockSkew, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonWritesBlockedClockSkew, info.GetReason())
	require.Equal(t, errorDomain, info.GetDomain())
}

func TestConvertToGRPCError_RaftTransientErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"proposal dropped", raft.ErrProposalDropped},
		{"not leader", node.ErrNotLeader},
		{"node syncing", node.ErrNodeSyncing},
		{"context deadline exceeded", context.DeadlineExceeded},
		{"context canceled", context.Canceled},
		{"wrapped deadline exceeded", fmt.Errorf("applying raft requests: %w", context.DeadlineExceeded)},
		{"wrapped proposal dropped", fmt.Errorf("applying raft requests: %w", raft.ErrProposalDropped)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grpcErr := convertToGRPCError(tt.err, testLogger())
			st, ok := status.FromError(grpcErr)
			require.True(t, ok)
			require.Equal(t, codes.Unavailable, st.Code())
		})
	}
}

func TestConvertToGRPCError_AlreadyGRPCStatus(t *testing.T) {
	t.Parallel()

	original := status.Error(codes.Internal, "already grpc")
	grpcErr := convertToGRPCError(original, testLogger())
	require.Equal(t, original, grpcErr)
}

func TestKindResourceExhaustedMapsToGRPC(t *testing.T) {
	t.Parallel()
	require.Equal(t, codes.ResourceExhausted, kindToGRPCCode(domain.KindResourceExhausted))
}

// extractErrorInfo extracts the ErrorInfo detail from a gRPC status.
func extractErrorInfo(t *testing.T, st *status.Status) *errdetails.ErrorInfo {
	t.Helper()

	details := st.Details()
	require.NotEmpty(t, details, "expected status to have details")

	for _, detail := range details {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}

	t.Fatal("no ErrorInfo found in status details")

	return nil
}

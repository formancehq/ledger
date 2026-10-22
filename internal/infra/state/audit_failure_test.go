package state

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
)

// TestIdempotencyFailureFactsMatchAudit pins the equality the checker's
// idempotencyMismatch depends on: the reason, code, and facts frozen into the
// SubIdempKeys projection by recordIdempotencyFailure must equal the ones
// buildAuditFailure wrote into the hash-chained AuditFailure for the same
// error. On a drift a perfectly healthy store reports
// CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH on every frozen failure, and the
// audit side of the divergence is inside the hash chain, so it cannot be
// corrected afterwards.
//
// Both sites derive those fields from describeFailure, so on that half the
// test is a forcing function against a future re-split of the derivation, not
// an independent oracle. The independent half is the round trip through
// state.IdempotencyValueFromAudit — the same derivation
// check.expectedIdempotencyOutcome builds its expectation with — which crosses
// the auditpb.AuditFailure to commonpb.IdempotencyFailure field mapping
// (including code and facts) that describeFailure does not cover.
func TestIdempotencyFailureFactsMatchAudit(t *testing.T) {
	t.Parallel()

	const (
		proposalCreatedAt = uint64(1700000000)
		idempotencyKey    = "idempotency-key-1"
	)

	for _, tc := range []struct {
		name string
		err  domain.SerializableError
	}{
		{
			// Every field distinct and non-zero, so a projection bug cannot
			// hide behind a zero value that both sides happen to share.
			name: "populated facts",
			err: &domain.ErrInsufficientFunds{
				Account:    "user:alice",
				Asset:      "USD/2",
				Color:      "RESERVED",
				ColorKnown: true,
				Amount:     "100",
				Balance:    "10",
			},
		},
		{
			// FailureDetails() is nil here, so both stored fact maps are empty.
			name: "nil facts",
			err:  domain.NewValidationSentinel("TARGET_REQUIRED", "EN-1772 fixture: value must not be empty"),
		},
		{
			name: "numscript execution error",
			err:  &domain.ErrNumscriptExecution{Detail: "cannot send negative amount: -4", Details: ledgerpb.MustFailureDetails("NUMSCRIPT_NEGATIVE_AMOUNT", map[string]string{"amount": "-4"})},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.True(t, domain.IsFreezableFailure(tc.err.Kind()),
				"the fixture must be freezable or recordIdempotencyFailure is a no-op and this test proves nothing")

			machine, dataStore, _ := newTestMachine(t)

			// Side A — the hash-chained audit entry, round-tripped through the
			// wire because that is how the checker reads it: a proto3 map with
			// no entries comes back nil, so the empty fact map does not survive.
			entry := &auditpb.AuditEntry{
				Timestamp: &ledgerpb.Timestamp{Data: proposalCreatedAt},
				Outcome:   &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(tc.err)},
			}

			raw, err := entry.MarshalVT()
			require.NoError(t, err)

			audited := &auditpb.AuditEntry{}
			require.NoError(t, audited.UnmarshalVT(raw))

			// Side B — the SubIdempKeys projection, read back out of Pebble
			// rather than off the cache, so the commit is load-bearing and the
			// serialization the checker walks is covered.
			batch := dataStore.OpenWriteSession()
			require.NoError(t, machine.recordIdempotencyFailure(
				batch, idempotencyKey, []byte("proposal-hash"), tc.err, proposalCreatedAt, 0))
			require.NoError(t, batch.Commit())

			handle, err := dataStore.NewDirectReadHandle()
			require.NoError(t, err)
			t.Cleanup(func() { _ = handle.Close() })

			stored, err := LoadIdempotencyKey(handle, idempotencyKey)
			require.NoError(t, err)
			require.NotNil(t, stored.GetFailure(), "the failure outcome must have been frozen")

			// IdempotencyFailure.EqualVT compares facts by length then by
			// key, so it carries the same nil-vs-empty tolerance as the
			// checker — no hand-maintained copy of that rule.
			expected, ok := IdempotencyValueFromAudit(audited, nil)
			require.True(t, ok, "a freezable failure must yield an expectation")
			require.True(t, expected.GetFailure().EqualVT(stored.GetFailure()),
				"audit chain %+v and idempotency projection %+v must carry the same reason, code and facts",
				expected.GetFailure(), stored.GetFailure())
		})
	}
}

// auditFailureCase is one row of the buildAuditFailure projection table. Each
// row fixes the reason and fact map written to the audit chain. The shared
// assertion also validates the code and fact schema.
type auditFailureCase struct {
	name        string
	err         domain.SerializableError
	wantReason  string
	wantContext map[string]string
}

// auditFailureCases carries one row per auditable Describable reachable from
// the FSM failure path. UncommittedFailure producers are excluded because the
// FSM returns them before the audit writer.
func auditFailureCases() []auditFailureCase {
	return []auditFailureCase{
		// ErrInsufficientFunds gates the color key on the separate ColorKnown
		// boolean, NOT on Color != "": the empty color IS the uncolored bucket,
		// so the type needs a third state for "the Numscript path could not
		// resolve which bucket failed". All three branches of Error() and both
		// branches of FailureDetails() get a row (errors.go:660-697).
		{
			name: "InsufficientFundsColoredBucket",
			err: &domain.ErrInsufficientFunds{
				Account:    "user:alice",
				Asset:      "USD/2",
				Color:      "RESERVED",
				ColorKnown: true,
				Amount:     "100",
				Balance:    "10",
			},
			wantReason: domain.ErrReasonInsufficientFunds,
			wantContext: map[string]string{
				"account": "user:alice",
				"asset":   "USD/2",
				"amount":  "100",
				"balance": "10",
				"color":   "RESERVED",
			},
		},
		{
			// ColorKnown with an empty Color is the resolved UNCOLORED bucket,
			// so the key is published as an empty string — that emptiness is
			// the payload, not a missing value.
			name: "InsufficientFundsUncoloredBucket",
			err: &domain.ErrInsufficientFunds{
				Account:    "user:bob",
				Asset:      "EUR/2",
				ColorKnown: true,
				Amount:     "250",
				Balance:    "40",
			},
			wantReason: domain.ErrReasonInsufficientFunds,
			wantContext: map[string]string{
				"account": "user:bob",
				"asset":   "EUR/2",
				"amount":  "250",
				"balance": "40",
				"color":   "",
			},
		},
		{
			// The Numscript VM reports asset and amounts, but neither the
			// source account nor its color bucket. It has a distinct code.
			name: "InsufficientFundsColorUnresolved",
			err: &domain.ErrInsufficientFunds{
				Asset:   "GBP/2",
				Color:   "IGNORED-WHEN-UNKNOWN",
				Amount:  "7",
				Balance: "3",
			},
			wantReason: domain.ErrReasonInsufficientFunds,
			wantContext: map[string]string{
				"asset":   "GBP/2",
				"amount":  "7",
				"balance": "3",
			},
		},
		{
			name:        "LedgerNotFound",
			err:         &domain.ErrLedgerNotFound{Name: "missing-ledger"},
			wantReason:  domain.ErrReasonLedgerNotFound,
			wantContext: map[string]string{"name": "missing-ledger"},
		},
		{
			name:        "LedgerAlreadyExists",
			err:         &domain.ErrLedgerAlreadyExists{Name: "existing-ledger"},
			wantReason:  domain.ErrReasonLedgerAlreadyExists,
			wantContext: map[string]string{"name": "existing-ledger"},
		},
		{
			name:        "TransactionNotFound",
			err:         &domain.ErrTransactionNotFound{TransactionID: 42},
			wantReason:  domain.ErrReasonTransactionNotFound,
			wantContext: map[string]string{"transactionId": "42"},
		},
		{
			// domain.FailureFactsOf(validationSentinel).Facts returns nil (errors.go:297) and
			// buildAuditFailure always allocates the map, so the projected
			// The fact map is empty but non-nil.
			name:        "ValidationSentinel",
			err:         domain.ErrScriptRequired,
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{},
		},
		{
			name:        "LedgerInMirrorMode",
			err:         &domain.ErrLedgerInMirrorMode{Name: "mirror-ledger"},
			wantReason:  domain.ErrReasonLedgerInMirrorMode,
			wantContext: map[string]string{"name": "mirror-ledger"},
		},
		{
			name:        "LedgerNotInMirrorMode",
			err:         &domain.ErrLedgerNotInMirrorMode{Name: "normal-ledger"},
			wantReason:  domain.ErrReasonLedgerNotInMirrorMode,
			wantContext: map[string]string{"name": "normal-ledger"},
		},
		{
			name:        "CheckpointLimitReached",
			err:         &domain.ErrCheckpointLimitReached{Limit: 10},
			wantReason:  domain.ErrReasonCheckpointLimitReached,
			wantContext: map[string]string{"limit": "10"},
		},
		{
			name:        "CheckpointNotFound",
			err:         &domain.ErrCheckpointNotFound{CheckpointID: 7},
			wantReason:  domain.ErrReasonCheckpointNotFound,
			wantContext: map[string]string{"checkpointId": "7"},
		},
		{
			// Reachable from apply: the FSM bounds the metadata a Numscript
			// program merges into the caller's, so the rejection is recorded in
			// the audit chain with the dimension that was hit.
			name:       "MetadataLimitExceeded",
			err:        &domain.ErrMetadataLimitExceeded{Dimension: domain.MetadataLimitDimensionEntity, Limit: 65536, Actual: 70000},
			wantReason: domain.ErrReasonMetadataLimitExceeded,
			wantContext: map[string]string{
				"dimension": domain.MetadataLimitDimensionEntity,
				"limit":     "65536",
				"actual":    "70000",
			},
		},
		{
			name:        "InvalidCronExpression",
			err:         &domain.ErrInvalidCronExpression{Expression: "* * * *", Details: "expected 5 or 6 fields"},
			wantReason:  domain.ErrReasonInvalidCronExpression,
			wantContext: map[string]string{"expression": "* * * *"},
		},
		{
			name:        "SinkAlreadyExists",
			err:         &domain.ErrSinkAlreadyExists{Name: "kafka-main"},
			wantReason:  domain.ErrReasonSinkAlreadyExists,
			wantContext: map[string]string{"name": "kafka-main"},
		},
		{
			name:        "SinkNotFound",
			err:         &domain.ErrSinkNotFound{Name: "missing-sink"},
			wantReason:  domain.ErrReasonSinkNotFound,
			wantContext: map[string]string{"name": "missing-sink"},
		},
		{
			name:        "SinkControllerMismatch",
			err:         &domain.ErrSinkControllerMismatch{Name: "kafka-main", ControllerID: "uid-1"},
			wantReason:  domain.ErrReasonSinkControllerMismatch,
			wantContext: map[string]string{"name": "kafka-main", "controllerId": "uid-1"},
		},
		{
			name:        "LedgerDeleted",
			err:         &domain.ErrLedgerDeleted{Name: "deleted-ledger"},
			wantReason:  domain.ErrReasonLedgerDeleted,
			wantContext: map[string]string{"name": "deleted-ledger"},
		},
		{
			name:        "IdempotencyKeyConflict",
			err:         &domain.ErrIdempotencyKeyConflict{Key: "idem-key-9"},
			wantReason:  domain.ErrReasonIdempotencyKeyConflict,
			wantContext: map[string]string{"key": "idem-key-9"},
		},
		{
			// Zero is a valid explicit ID, and must retain presence on the wire.
			name: "TransactionReferenceConflictWithZeroExistingID",
			err: &domain.ErrTransactionReferenceConflict{
				Ledger:    "main",
				Reference: "invoice-7",
			},
			wantReason:  domain.ErrReasonTransactionReferenceConflict,
			wantContext: map[string]string{"ledger": "main", "reference": "invoice-7", "existingTransactionId": "0"},
		},
		{
			name: "TransactionReferenceConflictWithExistingID",
			err: &domain.ErrTransactionReferenceConflict{
				Ledger:                "main",
				Reference:             "invoice-8",
				ExistingTransactionID: 77,
			},
			wantReason: domain.ErrReasonTransactionReferenceConflict,
			wantContext: map[string]string{
				"ledger":                "main",
				"reference":             "invoice-8",
				"existingTransactionId": "77",
			},
		},
		{
			name:        "TransactionReferenceNotFound",
			err:         &domain.ErrTransactionReferenceNotFound{Reference: "invoice-404"},
			wantReason:  domain.ErrReasonTransactionReferenceNotFound,
			wantContext: map[string]string{"reference": "invoice-404"},
		},
		{
			name:        "TransactionAlreadyReverted",
			err:         &domain.ErrTransactionAlreadyReverted{TransactionID: 91},
			wantReason:  domain.ErrReasonTransactionAlreadyReverted,
			wantContext: map[string]string{"transactionId": "91"},
		},
		{
			name:        "RevertTargetCreatedInBatch",
			err:         &domain.ErrRevertTargetCreatedInBatch{TransactionID: 91},
			wantReason:  domain.ErrReasonRevertTargetCreatedInBatch,
			wantContext: map[string]string{"transactionId": "91"},
		},
		{
			// The Provided enum is rendered through domain.ReasonString, so the
			// projected value is the client-facing identifier, not the enum name.
			name:        "InvalidSkippableReason",
			err:         &domain.ErrInvalidSkippableReason{Provided: ledgerpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"providedReason": domain.ErrReasonLedgerNotFound},
		},
		{
			name:        "VolumeOverflow",
			err:         &domain.ErrVolumeOverflow{Account: "user:dave", Asset: "USD/2", Color: "RESERVED", Side: "input", Amount: "9", Current: "8"},
			wantReason:  domain.ErrReasonVolumeOverflow,
			wantContext: map[string]string{"account": "user:dave", "asset": "USD/2", "color": "RESERVED", "side": "input", "amount": "9", "current": "8"},
		},
		{
			name:        "BalanceNotFound",
			err:         &domain.ErrBalanceNotFound{Account: "user:erin", Asset: "USD/2"},
			wantReason:  domain.ErrReasonBalanceNotFound,
			wantContext: map[string]string{"account": "user:erin", "asset": "USD/2"},
		},
		{
			// joinedAccounts renders "account/asset" for the uncolored bucket and
			// "account/asset/color" otherwise (errors.go:1276-1287); one row with
			// both shapes covers both branches. The producer pre-sorts, so this
			// rendering is what the audit hash binds.
			name: "TransientAccountNonZero",
			err: &domain.ErrTransientAccountNonZero{Accounts: []domain.AccountAssetKey{
				{Account: "transient:a", Asset: "USD/2"},
				{Account: "transient:b", Asset: "EUR/2", Color: "RESERVED"},
			}},
			wantReason:  domain.ErrReasonTransientAccountNonZero,
			wantContext: map[string]string{"accounts": "transient:a/USD/2, transient:b/EUR/2/RESERVED"},
		},
		{
			name:        "SinkBatchSizeTooLarge",
			err:         &domain.ErrSinkBatchSizeTooLarge{Name: "kafka-main", BatchSize: 200000, Max: domain.MaxSinkBatchSize},
			wantReason:  domain.ErrReasonSinkBatchSizeTooLarge,
			wantContext: map[string]string{"name": "kafka-main", "batchSize": "200000", "max": "100000"},
		},
		{
			name:        "MetadataNotFound",
			err:         &domain.ErrMetadataNotFound{Target: "transaction:42", Key: "invoice"},
			wantReason:  domain.ErrReasonMetadataNotFound,
			wantContext: map[string]string{"target": "transaction:42", "key": "invoice"},
		},
		{
			name:        "MetadataFieldNotInSchema",
			err:         &domain.ErrMetadataFieldNotInSchema{Target: "TRANSACTION", Key: "invoice"},
			wantReason:  domain.ErrReasonMetadataFieldNotInSchema,
			wantContext: map[string]string{"target": "TRANSACTION", "key": "invoice"},
		},
		{
			name:        "PreparedQueryAlreadyExists",
			err:         &domain.ErrPreparedQueryAlreadyExists{Ledger: "main", Name: "top-accounts"},
			wantReason:  domain.ErrReasonPreparedQueryAlreadyExists,
			wantContext: map[string]string{"ledger": "main", "name": "top-accounts"},
		},
		{
			name:        "PreparedQueryNotFound",
			err:         &domain.ErrPreparedQueryNotFound{Ledger: "main", Name: "missing-query"},
			wantReason:  domain.ErrReasonPreparedQueryNotFound,
			wantContext: map[string]string{"ledger": "main", "name": "missing-query"},
		},
		{
			name:        "IndexAlreadyExists",
			err:         &domain.ErrIndexAlreadyExists{Index: "metadata:account:invoice"},
			wantReason:  domain.ErrReasonIndexAlreadyExists,
			wantContext: map[string]string{"index": "metadata:account:invoice"},
		},
		{
			name:        "IndexNotFound",
			err:         &domain.ErrIndexNotFound{Index: "main/TRANSACTION/invoice"},
			wantReason:  domain.ErrReasonIndexNotFound,
			wantContext: map[string]string{"index": "main/TRANSACTION/invoice"},
		},
		{
			// Version is gated on != "" (errors.go:1073), and Error() branches on
			// the same field.
			name:        "NumscriptNotFoundWithoutVersion",
			err:         &domain.ErrNumscriptNotFound{Name: "payout"},
			wantReason:  domain.ErrReasonNumscriptNotFound,
			wantContext: map[string]string{"name": "payout"},
		},
		{
			name:        "NumscriptNotFoundWithVersion",
			err:         &domain.ErrNumscriptNotFound{Name: "payout", Version: "1.2.3"},
			wantReason:  domain.ErrReasonNumscriptNotFound,
			wantContext: map[string]string{"name": "payout", "version": "1.2.3"},
		},
		{
			name:        "NumscriptVersionAlreadyExists",
			err:         &domain.ErrNumscriptVersionAlreadyExists{Name: "payout", Version: "1.2.3"},
			wantReason:  domain.ErrReasonNumscriptVersionAlreadyExists,
			wantContext: map[string]string{"name": "payout", "version": "1.2.3"},
		},
		{
			name:        "NumscriptInvalidVersion",
			err:         &domain.ErrNumscriptInvalidVersion{Version: "v1"},
			wantReason:  domain.ErrReasonNumscriptInvalidVersion,
			wantContext: map[string]string{"version": "v1"},
		},
		{
			name:        "NumscriptParse",
			err:         &domain.ErrNumscriptParse{Details: "unexpected token at line 3"},
			wantReason:  domain.ErrReasonNumscriptParseError,
			wantContext: map[string]string{},
		},
		{
			name:        "NumscriptCompile",
			err:         &domain.ErrNumscriptCompile{Detail: "cannot take all balance of an unbounded source", Details: ledgerpb.MustFailureDetails("NUMSCRIPT_COMPILE_INVALID_UNCAPPED_SOURCE", nil)},
			wantReason:  domain.ErrReasonNumscriptCompileError,
			wantContext: map[string]string{},
		},
		{
			name:        "NumscriptExecution",
			err:         &domain.ErrNumscriptExecution{Detail: "cannot send negative amount: -4", Details: ledgerpb.MustFailureDetails("NUMSCRIPT_NEGATIVE_AMOUNT", map[string]string{"amount": "-4"})},
			wantReason:  domain.ErrReasonNumscriptExecutionError,
			wantContext: map[string]string{"amount": "-4"},
		},
		{
			name:        "AccountNotMatchingType",
			err:         &domain.ErrAccountNotMatchingType{Address: "user:ivan"},
			wantReason:  domain.ErrReasonAccountNotMatchingType,
			wantContext: map[string]string{"address": "user:ivan"},
		},
		{
			name:        "AccountTypeNotFound",
			err:         &domain.ErrAccountTypeNotFound{Name: "missing-type"},
			wantReason:  domain.ErrReasonAccountTypeNotFound,
			wantContext: map[string]string{"name": "missing-type"},
		},
		{
			name:        "AccountTypeAlreadyExists",
			err:         &domain.ErrAccountTypeAlreadyExists{Name: "existing-type"},
			wantReason:  domain.ErrReasonAccountTypeAlreadyExists,
			wantContext: map[string]string{"name": "existing-type"},
		},
		{
			name:        "AccountTypeHasAccounts",
			err:         &domain.ErrAccountTypeHasAccounts{Name: "users"},
			wantReason:  domain.ErrReasonAccountTypeHasAccounts,
			wantContext: map[string]string{"name": "users"},
		},
		{
			// NewPattern is projected under the key "pattern" — the field name
			// and the audited key differ (errors.go:1159).
			name: "AccountTypeConflict",
			err: &domain.ErrAccountTypeConflict{
				NewPattern:      "user:*",
				ExistingName:    "users",
				ExistingPattern: "user:**",
			},
			wantReason: domain.ErrReasonAccountTypeConflict,
			wantContext: map[string]string{
				"pattern":         "user:*",
				"existingName":    "users",
				"existingPattern": "user:**",
			},
		},
		{
			name:        "InvalidPattern",
			err:         &domain.ErrInvalidPattern{Pattern: "user:[", Details: "unterminated character class"},
			wantReason:  domain.ErrReasonInvalidPattern,
			wantContext: map[string]string{"pattern": "user:["},
		},
		// The stateless sentinels below select a zero-field detail variant.
		// They are reached through their exported SerializableError
		// var — the concrete type is unexported, and that var IS the identity
		// every call site compares against with errors.Is.
		{
			name:        "CheckpointIDRequired",
			err:         domain.ErrCheckpointIDRequired,
			wantReason:  domain.ErrReasonCheckpointIDRequired,
			wantContext: map[string]string{},
		},
		{
			// errValidation wraps a github.com/formancehq/invariants primitive.
			// Its diagnostic message does not enter the typed audit chain.
			name:        "ValidationWrappedInvariant",
			err:         domain.ErrLedgerNameRequired,
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{},
		},
		{
			name:        "MetadataFactFailure",
			err:         domain.ValidateMetadataKey("bad key"),
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"key": "bad key"},
		},
		{
			// ErrMetadataKeyValidation adds {"key": Key} and merges the Cause's
			// stable facts. Reason() delegates to the Cause.
			name: "MetadataKeyValidationContextualCause",
			err: &domain.ErrMetadataKeyValidation{
				Key:   "invoice",
				Cause: domain.ValidateMetadataString("invoice", "invalid\x00value"),
			},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"key": "invoice"},
		},
		{
			// Account context is valid for a metadata failure.
			name: "AccountValidationMetadataCause",
			err: &domain.ErrAccountValidation{
				Account: "user:jack",
				Cause:   domain.ValidateMetadataString("invoice", "invalid\x00value"),
			},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"account": "user:jack", "key": "invoice"},
		},
		{
			name: "AccountValidationMergesCauseFacts",
			err: &domain.ErrAccountValidation{
				Account: "user:jack",
				Cause:   &domain.ErrMetadataLimitExceeded{Dimension: domain.MetadataLimitDimensionEntries, Limit: 1, Actual: 2},
			},
			wantReason: domain.ErrReasonMetadataLimitExceeded,
			wantContext: map[string]string{
				"account":   "user:jack",
				"dimension": domain.MetadataLimitDimensionEntries,
				"limit":     "1",
				"actual":    "2",
			},
		},
		{
			// ReplayedFailure carries reason and typed details from a stored
			// idempotency outcome, so this row pins the pass-through — including
			// that ReasonCode round-trips the
			// stored reason string.
			name: "ReplayedFailure",
			err: &domain.ReplayedFailure{
				ErrReason: domain.ErrReasonTransactionAlreadyReverted,
				Msg:       "transaction 91 is already reverted",
				Details:   ledgerpb.MustFailureDetails(domain.ErrReasonTransactionAlreadyReverted, map[string]string{"transactionId": "91"}),
			},
			wantReason:  domain.ErrReasonTransactionAlreadyReverted,
			wantContext: map[string]string{"transactionId": "91"},
		},
	}
}

// describableTypeKey identifies a Describable implementation by package and type
// name. The package is part of the key so a domain type and a state type that
// happen to share a name can never be confused for one another.
type describableTypeKey struct {
	pkg  string
	name string
}

func (k describableTypeKey) String() string { return k.pkg + "." + k.name }

// describableScanDirs are the packages whose SerializableError implementations
// can reach buildAuditFailure. This is a deliberate boundary, not an oversight:
// internal/domain holds the business errors the FSM returns and
// internal/infra/state holds the FSM-local ones (ErrCoverageMiss). Every other
// implementation in the tree — admission's errIdempotencyKeyTooLong /
// errIdempotencyKeyInvalidUTF8 / errCheckpointOrderNotLast,
// query.ErrAggregateOverflow, grpc.validationError — is produced before a
// proposal exists or on the read path, so it never reaches the audit chain. If
// one of them ever becomes FSM-reachable, add its directory here.
var describableScanDirs = map[string]string{
	"../../domain": "github.com/formancehq/ledger/v3/internal/domain",
	".":            "github.com/formancehq/ledger/v3/internal/infra/state",
}

// TestBuildAuditFailureCoversEveryDescribable is the forcing function: adding a
// SerializableError without adding a row to auditFailureCases fails this test.
//
// Since EN-2081 the scan predicate is the SerializableError contract rather
// than "any typed error". That is what buildAuditFailure now accepts.
//
// The table covers only types with a production path to the audit chain. A
// failure decoded from a peer has none by construction — it lives outside
// internal/domain, in internal/adapter/apierr, which the scan does not read
// and which no package under internal/domain, internal/infra/state or
// internal/admission may import (enforced by scripts/check-repo-invariants).
// It therefore needs no unreachable row here.
//
// Discovery is by METHOD SET, not by type name. It collects every receiver type
// declaring Reason() and FailureDetails() — the structured
// SerializableError contract. The name-prefix scan used by
// TestEveryDomainErrorImplementsDescribable (internal/domain/errors_test.go:318)
// would miss domain.ReplayedFailure, which does reach buildAuditFailure, and it
// would need a hand-maintained
// exclusion list for the Err* types in this package that are NOT Describable
// (ErrNodeOutOfSync, ErrInvalidEntryIndex, ErrDoubleEntryInvariantViolated,
// ErrVolumeCachePebbleDivergence). The method-set predicate needs neither.
func TestBuildAuditFailureCoversEveryDescribable(t *testing.T) {
	t.Parallel()

	discovered := make(map[describableTypeKey]bool)

	for dir, pkgPath := range describableScanDirs {
		for name := range describableTypesIn(t, dir) {
			discovered[describableTypeKey{pkg: pkgPath, name: name}] = true
		}
	}

	require.NotEmpty(t, discovered,
		"the AST scan found no SerializableError implementations at all — the scan is broken, not the table")

	covered := make(map[describableTypeKey]bool)

	for _, tc := range auditFailureCases() {
		rt := reflect.TypeOf(tc.err)
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}

		covered[describableTypeKey{pkg: rt.PkgPath(), name: rt.Name()}] = true
	}

	require.Equal(t, sortedKeys(discovered), sortedKeys(covered),
		"auditFailureCases must hold exactly one row per SerializableError reachable from the FSM failure path:\n"+
			"  a MISSING entry means a new error type landed with no assertion on what buildAuditFailure writes\n"+
			"  an EXTRA entry means a row references a type that is no longer a SerializableError")
}

// describableTypesIn returns the names of every type declared in dir whose
// method set includes Reason() and FailureDetails() — the
// structured SerializableError contract accepted by buildAuditFailure.
func describableTypesIn(t *testing.T, dir string) map[string]bool {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no Go files found in %s — the relative scan path is wrong", dir)

	hasReason := make(map[string]bool)
	hasFacts := make(map[string]bool)
	hasUncommitted := make(map[string]bool)

	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}

		fset := token.NewFileSet()

		f, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr, "parsing %s", path)

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}

			receiver := receiverTypeName(fn.Recv.List[0].Type)
			if receiver == "" {
				continue
			}

			switch fn.Name.Name {
			case "Reason":
				hasReason[receiver] = true
			case "FailureDetails":
				hasFacts[receiver] = true
			case "UncommittedFailure":
				hasUncommitted[receiver] = true
			}
		}
	}

	out := make(map[string]bool)

	for name := range hasReason {
		if hasFacts[name] && !hasUncommitted[name] {
			out[name] = true
		}
	}

	return out
}

// receiverTypeName resolves a method receiver expression to its bare type name,
// unwrapping the pointer form. Generic receivers (IndexExpr) return "" — there
// are none among the Describables today, and a new one would surface as a
// missing row rather than as a silent skip.
func receiverTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(e.X)
	case *ast.Ident:
		return e.Name
	default:
		return ""
	}
}

// TestBuildAuditFailure asserts the structured projection buildAuditFailure
// performs for every SerializableError the FSM can reach. Each row supplies
// literal facts, so changes to the audited tuple are visible in review.
// The sorted fact keys and values are folded
// into the audit hash pre-image by buildAuditFailurePayload (audit_envelope.go).
// A renamed key changes bytes that are immutable once written — the EN-1379 lesson,
// where a snake_case to camelCase rename shipped green.
func TestBuildAuditFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range auditFailureCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			failure := buildAuditFailure(tc.err)

			require.Equal(t, tc.wantReason, domain.ReasonString(failure.GetReason()))
			require.NotEmpty(t, failure.GetCode())
			require.Equal(t, tc.wantContext, failure.GetFacts())
			require.NoError(t, domain.ValidateFailureFacts(domain.FailureFacts{
				Reason: failure.GetReason(), Code: failure.GetCode(), Facts: failure.GetFacts(),
			}))

			// The audit projection and the idempotency projection must agree on
			// both shared fields for every error type, not just the one exercised
			// end to end by TestIdempotencyFailureFactsMatchAudit.
			require.Equal(t, domain.ReasonCode(tc.err.Reason()), failure.GetReason())

			// A frozen failure replays from the persisted reason alone, so the
			// kind the type declares must be the kind that reason re-derives.
			// Asserting it here covers the FSM-reachable set exactly — the same
			// types whose classification a replay has to reproduce.
			require.Equal(t, domain.KindForReason(failure.GetReason()), tc.err.Kind(),
				"the declared kind must match the one KindForReason derives from the audited reason")
		})
	}
}

func sortedKeys(set map[describableTypeKey]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k.String())
	}

	sort.Strings(out)

	return out
}

// A storage error remains uncommitted even when its underlying cause is a
// coverage miss. The caller can retry after repairing either condition.
func TestStoreFailureDoesNotFreezeCoverageMiss(t *testing.T) {
	t.Parallel()

	miss := &ErrCoverageMiss{Attribute: "volumes", IDHex: "0102", RaftIndex: 42}
	storage := domain.StoreFailure("loading volume", miss)
	require.Equal(t, domain.ErrReasonCoverageMiss, storage.Reason())
	_, uncommitted := storage.(domain.UncommittedFailure)
	require.True(t, uncommitted)
}

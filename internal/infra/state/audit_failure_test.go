package state

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestIdempotencyFailureMessageMatchesAudit pins the equality the checker's
// idempotencyMismatch depends on: the reason and message frozen into the
// SubIdempKeys projection by recordIdempotencyFailure MUST equal the ones
// buildAuditFailure wrote into the hash-chained AuditFailure for the same
// error. On a drift a perfectly healthy store reports
// CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH on every frozen failure, and the
// audit side of the divergence is inside the hash chain, so it cannot be
// corrected afterwards.
//
// Both sites derive those two fields from describeFailure, so on that half the
// test is a forcing function against a future re-split of the derivation, not
// an independent oracle. The independent half is the round trip through
// state.IdempotencyValueFromAudit — the same derivation
// check.expectedIdempotencyOutcome builds its expectation with — which crosses
// the auditpb.AuditFailure to commonpb.IdempotencyFailure field mapping
// (Context to Metadata included) that describeFailure does not cover.
func TestIdempotencyFailureMessageMatchesAudit(t *testing.T) {
	t.Parallel()

	const (
		proposalCreatedAt = uint64(1700000000)
		idempotencyKey    = "idempotency-key-1"
	)

	for _, tc := range []struct {
		name string
		err  domain.Describable
	}{
		{
			// Every field distinct and non-zero, so a projection bug cannot
			// hide behind a zero value that both sides happen to share.
			name: "populated metadata",
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
			// Metadata() is nil here, the only shape that exercises the
			// nil-vs-empty asymmetry: buildAuditFailure emits a non-nil empty
			// Context while recordIdempotencyFailure stores nil.
			name: "nil metadata",
			err:  domain.NewValidationSentinel("EN-1772 fixture: value must not be empty"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.True(t, domain.IsFreezableFailure(domain.Kind(tc.err)),
				"the fixture must be freezable or recordIdempotencyFailure is a no-op and this test proves nothing")

			machine, dataStore, _ := newTestMachine(t)

			// Side A — the hash-chained audit entry, round-tripped through the
			// wire because that is how the checker reads it: a proto3 map with
			// no entries comes back nil, so the empty Context does not survive.
			entry := &auditpb.AuditEntry{
				Timestamp: &commonpb.Timestamp{Data: proposalCreatedAt},
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

			// IdempotencyFailure.EqualVT compares metadata by length then by
			// key, so it carries the same nil-vs-empty tolerance as
			// checker.metadataEqual — no hand-maintained copy of that rule.
			expected, ok := IdempotencyValueFromAudit(audited, nil)
			require.True(t, ok, "a freezable failure must yield an expectation")
			require.True(t, expected.GetFailure().EqualVT(stored.GetFailure()),
				"audit chain %+v and idempotency projection %+v must carry the same reason, message and metadata",
				expected.GetFailure(), stored.GetFailure())
		})
	}
}

// auditFailureCase is one row of the buildAuditFailure projection table. The
// expected message is deliberately absent: it is derived in the shared body as
// tc.err.Error(), so no row can assert a subset of the projected fields. That
// per-row drift is exactly what left AuditFailure.Message unasserted for every
// error type but one (EN-1772).
type auditFailureCase struct {
	name        string
	err         domain.Describable
	wantReason  string
	wantContext map[string]string
}

// auditFailureCases carries one row per Describable reachable from the FSM
// failure path. TestBuildAuditFailureCoversEveryDescribable fails if a type is
// missing, so this list cannot silently fall behind.
func auditFailureCases() []auditFailureCase {
	return []auditFailureCase{
		// ErrInsufficientFunds gates the color key on the separate ColorKnown
		// boolean, NOT on Color != "": the empty color IS the uncolored bucket,
		// so the type needs a third state for "the Numscript path could not
		// resolve which bucket failed". All three branches of Error() and both
		// branches of Metadata() get a row (errors.go:660-697).
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
			// The Numscript path: Color is unresolved, so the key is OMITTED
			// rather than emitted empty. Publishing "" here would tell a client
			// the uncolored bucket definitely came up short.
			name: "InsufficientFundsColorUnresolved",
			err: &domain.ErrInsufficientFunds{
				Account: "user:carol",
				Asset:   "GBP/2",
				Color:   "IGNORED-WHEN-UNKNOWN",
				Amount:  "7",
				Balance: "3",
			},
			wantReason: domain.ErrReasonInsufficientFunds,
			wantContext: map[string]string{
				"account": "user:carol",
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
			// validationSentinel.Metadata() returns nil (errors.go:297) and
			// buildAuditFailure always allocates the map, so the projected
			// Context is empty-but-non-nil.
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
			name:        "MaintenanceMode",
			err:         domain.ErrMaintenanceMode,
			wantReason:  domain.ErrReasonMaintenanceMode,
			wantContext: map[string]string{},
		},
		{
			name:        "StaleClusterPolicy",
			err:         &domain.ErrStaleClusterPolicy{ProposedRevision: 3, AppliedRevision: 5},
			wantReason:  domain.ErrReasonStaleClusterPolicy,
			wantContext: map[string]string{"proposedRevision": "3", "appliedRevision": "5"},
		},
		{
			name:        "ClusterPolicyRevisionConflict",
			err:         &domain.ErrClusterPolicyRevisionConflict{Revision: 7},
			wantReason:  domain.ErrReasonClusterPolicyRevisionConflict,
			wantContext: map[string]string{"revision": "7"},
		},
		{
			name:        "ClusterPolicyInvalid",
			err:         &domain.ErrClusterPolicyInvalid{Detail: "query_checkpoint_limit must be at least 1"},
			wantReason:  domain.ErrReasonClusterPolicyInvalid,
			wantContext: map[string]string{"detail": "query_checkpoint_limit must be at least 1"},
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
			name:        "InvalidCronExpression",
			err:         &domain.ErrInvalidCronExpression{Expression: "* * * *", Details: "expected 5 or 6 fields"},
			wantReason:  domain.ErrReasonInvalidCronExpression,
			wantContext: map[string]string{"expression": "* * * *", "details": "expected 5 or 6 fields"},
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
			// ExistingTransactionID is gated on != 0 (errors.go:561), so a
			// collision detected without the owning id projects two keys.
			name: "TransactionReferenceConflictWithoutExistingID",
			err: &domain.ErrTransactionReferenceConflict{
				Ledger:    "main",
				Reference: "invoice-7",
			},
			wantReason:  domain.ErrReasonTransactionReferenceConflict,
			wantContext: map[string]string{"ledger": "main", "reference": "invoice-7"},
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
			name: "TransactionStateInconsistent",
			err: &domain.ErrTransactionStateInconsistent{
				TransactionID: 91,
				Operation:     "reverting transaction",
			},
			wantReason: domain.ErrReasonTransactionStateInconsistent,
			wantContext: map[string]string{
				"transactionId": "91",
				"operation":     "reverting transaction",
			},
		},
		{
			// The Provided enum is rendered through domain.ReasonString, so the
			// projected value is the client-facing identifier, not the enum name.
			name:        "InvalidSkippableReason",
			err:         &domain.ErrInvalidSkippableReason{Provided: commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"reason": domain.ErrReasonLedgerNotFound},
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
			// Unlike ErrInsufficientFunds this type publishes color
			// UNCONDITIONALLY (errors.go:1249) — only Error() branches on the
			// empty color. Both rows therefore carry the same key set.
			name:        "BalanceNotPreloadedColored",
			err:         &domain.ErrBalanceNotPreloaded{Account: "user:frank", Asset: "USD/2", Color: "RESERVED"},
			wantReason:  domain.ErrReasonBalanceNotPreloaded,
			wantContext: map[string]string{"account": "user:frank", "asset": "USD/2", "color": "RESERVED"},
		},
		{
			name:        "BalanceNotPreloadedUncolored",
			err:         &domain.ErrBalanceNotPreloaded{Account: "user:grace", Asset: "EUR/2"},
			wantReason:  domain.ErrReasonBalanceNotPreloaded,
			wantContext: map[string]string{"account": "user:grace", "asset": "EUR/2", "color": ""},
		},
		{
			name:        "VolumeNotMaterialized",
			err:         &domain.ErrVolumeNotMaterialized{Account: "user:heidi", Asset: "USD/2", Color: "RESERVED", Side: "source"},
			wantReason:  domain.ErrReasonVolumeNotMaterialized,
			wantContext: map[string]string{"account": "user:heidi", "asset": "USD/2", "color": "RESERVED", "side": "source"},
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
			name:        "MirrorV2LogIDGap",
			err:         &domain.ErrMirrorV2LogIDGap{Name: "mirror-ledger", Got: 12, Expected: 9},
			wantReason:  domain.ErrReasonMirrorV2LogIDGap,
			wantContext: map[string]string{"name": "mirror-ledger", "got": "12", "expected": "9"},
		},
		{
			name:        "MirrorV2LogIDInvalid",
			err:         &domain.ErrMirrorV2LogIDInvalid{Name: "mirror-ledger"},
			wantReason:  domain.ErrReasonMirrorV2LogIDInvalid,
			wantContext: map[string]string{"name": "mirror-ledger"},
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
			name:        "IndexNotFound",
			err:         &domain.ErrIndexNotFound{Index: "main/TRANSACTION/invoice"},
			wantReason:  domain.ErrReasonIndexNotFound,
			wantContext: map[string]string{"index": "main/TRANSACTION/invoice"},
		},
		{
			name:        "IndexBuilding",
			err:         &domain.ErrIndexBuilding{Index: "main/TRANSACTION/invoice"},
			wantReason:  domain.ErrReasonIndexBuilding,
			wantContext: map[string]string{"index": "main/TRANSACTION/invoice"},
		},
		{
			name:        "IndexInconsistent",
			err:         &domain.ErrIndexInconsistent{Index: "main/TRANSACTION/invoice", Detail: "logId 7 missing from the log index"},
			wantReason:  domain.ErrReasonIndexInconsistent,
			wantContext: map[string]string{"index": "main/TRANSACTION/invoice", "detail": "logId 7 missing from the log index"},
		},
		{
			name:        "CheckpointNotReady",
			err:         &domain.ErrCheckpointNotReady{CheckpointID: 18},
			wantReason:  domain.ErrReasonCheckpointNotReady,
			wantContext: map[string]string{"checkpointId": "18"},
		},
		{
			// The Go field is Detail; the wire key is "reason" (legacy contract,
			// errors.go:1051) — a rename here would break the audited key set.
			name:        "InvalidReceipt",
			err:         &domain.ErrInvalidReceipt{Detail: "signature verification failed"},
			wantReason:  domain.ErrReasonInvalidReceipt,
			wantContext: map[string]string{"reason": "signature verification failed"},
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
			wantContext: map[string]string{"details": "unexpected token at line 3"},
		},
		{
			name:        "NumscriptRuntime",
			err:         &domain.ErrNumscriptRuntime{Detail: "posting amount is negative"},
			wantReason:  domain.ErrReasonNumscriptRuntime,
			wantContext: map[string]string{"detail": "posting amount is negative"},
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
			wantContext: map[string]string{"pattern": "user:[", "details": "unterminated character class"},
		},
		{
			// Reason() delegates to the Cause when it is a Describable
			// (errors.go:1218-1225) and falls back to VALIDATION otherwise, so
			// each of the three Cause shapes gets a row. Metadata() projects
			// details = Error(), which itself branches on a nil Cause.
			name:        "DependencyDiscoveryFailedNilCause",
			err:         &domain.ErrDependencyDiscoveryFailed{},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"details": "numscript dependency discovery failed"},
		},
		{
			name:        "DependencyDiscoveryFailedOpaqueCause",
			err:         &domain.ErrDependencyDiscoveryFailed{Cause: errors.New("resolver timed out")},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"details": "numscript dependency discovery failed: resolver timed out"},
		},
		{
			name:        "DependencyDiscoveryFailedDescribableCause",
			err:         &domain.ErrDependencyDiscoveryFailed{Cause: &domain.ErrLedgerNotFound{Name: "absent-ledger"}},
			wantReason:  domain.ErrReasonLedgerNotFound,
			wantContext: map[string]string{"details": "numscript dependency discovery failed: ledger does not exist: absent-ledger"},
		},
		{
			name:        "FilterCompilation",
			err:         &domain.ErrFilterCompilation{Detail: "cannot parse 'x' as int64"},
			wantReason:  domain.ErrReasonFilterCompilation,
			wantContext: map[string]string{"detail": "cannot parse 'x' as int64"},
		},
		{
			name:        "InvalidOrderType",
			err:         &domain.ErrInvalidOrderType{TypeName: "*raftcmdpb.Order_Unknown"},
			wantReason:  domain.ErrReasonInvalidOrderType,
			wantContext: map[string]string{"typeName": "*raftcmdpb.Order_Unknown"},
		},
		{
			name:        "InvalidApplyType",
			err:         &domain.ErrInvalidApplyType{TypeName: "*raftcmdpb.LedgerApplyOrder_Unknown"},
			wantReason:  domain.ErrReasonInvalidApplyType,
			wantContext: map[string]string{"typeName": "*raftcmdpb.LedgerApplyOrder_Unknown"},
		},
		{
			// The Go field is Reason_ (the name Reason() is taken by the
			// interface); the audited key is "reason" (errors.go:1531).
			name:        "InvalidExecutionPlan",
			err:         &domain.ErrInvalidExecutionPlan{Reason_: "coverage bit 9 past the attribute slice"},
			wantReason:  domain.ErrReasonInvalidExecutionPlan,
			wantContext: map[string]string{"reason": "coverage bit 9 past the attribute slice"},
		},
		{
			name:        "ExecutionPlanTooLarge",
			err:         &domain.ErrExecutionPlanTooLarge{Size: 4096, Limit: 1024},
			wantReason:  domain.ErrReasonExecutionPlanTooLarge,
			wantContext: map[string]string{"size": "4096", "limit": "1024"},
		},
		{
			// Metadata() returns nil and Error() is a constant: the Cause is
			// deliberately kept off the wire (#326), so a populated Cause must
			// change neither the message nor the context.
			name:        "IdempotencyCheckFailed",
			err:         &domain.ErrIdempotencyCheckFailed{Cause: errors.New("pebble: /var/lib/ledger: io error")},
			wantReason:  domain.ErrReasonIdempotencyCheckFailed,
			wantContext: map[string]string{},
		},
		{
			// Only Operation is projected; the Cause stays server-side (#326).
			name:        "StorageOperation",
			err:         &domain.ErrStorageOperation{Operation: "loading volume", Cause: errors.New("pebble: /var/lib/ledger: io error")},
			wantReason:  domain.ErrReasonStorageOperation,
			wantContext: map[string]string{"operation": "loading volume"},
		},
		// The stateless sentinels below all return nil from Metadata(), so their
		// projected Context is the empty-but-non-nil map buildAuditFailure
		// allocates. They are reached through their exported Describable var —
		// the concrete type is unexported, and that var IS the identity every
		// call site compares against with errors.Is.
		{
			name:        "AuditDisabled",
			err:         domain.ErrAuditDisabled,
			wantReason:  domain.ErrReasonAuditDisabled,
			wantContext: map[string]string{},
		},
		{
			name:        "StaleProposal",
			err:         domain.ErrStaleProposal,
			wantReason:  domain.ErrReasonStaleProposal,
			wantContext: map[string]string{},
		},
		{
			name:        "StaleInputsResolution",
			err:         domain.ErrStaleInputsResolution,
			wantReason:  domain.ErrReasonStaleInputsResolution,
			wantContext: map[string]string{},
		},
		{
			name:        "PreloadUnavailable",
			err:         domain.ErrPreloadUnavailable,
			wantReason:  domain.ErrReasonPreloadUnavailable,
			wantContext: map[string]string{},
		},
		{
			name:        "WritesBlockedDiskFull",
			err:         domain.ErrWritesBlockedDiskFull,
			wantReason:  domain.ErrReasonWritesBlockedDiskFull,
			wantContext: map[string]string{},
		},
		{
			name:        "WritesBlockedClockSkew",
			err:         domain.ErrWritesBlockedClockSkew,
			wantReason:  domain.ErrReasonWritesBlockedClockSkew,
			wantContext: map[string]string{},
		},
		{
			name:        "CheckpointIDRequired",
			err:         domain.ErrCheckpointIDRequired,
			wantReason:  domain.ErrReasonCheckpointIDRequired,
			wantContext: map[string]string{},
		},
		{
			// errValidation wraps a github.com/formancehq/invariants primitive so
			// it satisfies Describable; Metadata() is nil (validation.go:66), so
			// the invariants message reaches the chain with no structured context.
			name:        "ValidationWrappedInvariant",
			err:         domain.ErrLedgerNameRequired,
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{},
		},
		{
			// BusinessError delegates Error/Reason/Metadata to the wrapped
			// Describable (errors.go:256-259), so the projection must be
			// indistinguishable from projecting the inner error directly.
			name:        "BusinessErrorDelegatesToInner",
			err:         &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: "wrapped-ledger"}},
			wantReason:  domain.ErrReasonLedgerNotFound,
			wantContext: map[string]string{"name": "wrapped-ledger"},
		},
		{
			// ErrMetadataKeyValidation adds {"key": Key} and merges the Cause's
			// metadata over it (errors.go:1483-1489); Reason() delegates to the
			// Cause. The practical Cause is a nil-metadata validation sentinel.
			name: "MetadataKeyValidationNilCauseMetadata",
			err: &domain.ErrMetadataKeyValidation{
				Key:   "invoice",
				Cause: domain.ErrMetadataValueContainsNullByte,
			},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"key": "invoice"},
		},
		{
			// A Cause that carries its own metadata: both key sets must land, and
			// the Reason must be the Cause's, not VALIDATION.
			name: "MetadataKeyValidationMergesCauseMetadata",
			err: &domain.ErrMetadataKeyValidation{
				Key:   "invoice",
				Cause: &domain.ErrNumscriptRuntime{Detail: "metadata key produced by expression is malformed"},
			},
			wantReason: domain.ErrReasonNumscriptRuntime,
			wantContext: map[string]string{
				"key":    "invoice",
				"detail": "metadata key produced by expression is malformed",
			},
		},
		{
			// Same shape as ErrMetadataKeyValidation but keyed on the account
			// (errors.go:1506-1512).
			name: "AccountValidationNilCauseMetadata",
			err: &domain.ErrAccountValidation{
				Account: "user:jack",
				Cause:   domain.ErrAccountAddressInvalidChar,
			},
			wantReason:  domain.ErrReasonValidation,
			wantContext: map[string]string{"account": "user:jack"},
		},
		{
			name: "AccountValidationMergesCauseMetadata",
			err: &domain.ErrAccountValidation{
				Account: "user:jack",
				Cause:   &domain.ErrMetadataNotFound{Target: "account:user:jack", Key: "tier"},
			},
			wantReason: domain.ErrReasonMetadataNotFound,
			wantContext: map[string]string{
				"account": "user:jack",
				"target":  "account:user:jack",
				"key":     "tier",
			},
		},
		{
			// ReplayedFailure carries reason, message and metadata verbatim from a
			// stored idempotency outcome rather than deriving them, so this row
			// pins the pass-through — including that ReasonCode round-trips the
			// stored reason string.
			name: "ReplayedFailure",
			err: &domain.ReplayedFailure{
				ErrReason: domain.ErrReasonTransactionAlreadyReverted,
				Msg:       "transaction 91 is already reverted",
				Meta:      map[string]string{"transactionId": "91"},
			},
			wantReason:  domain.ErrReasonTransactionAlreadyReverted,
			wantContext: map[string]string{"transactionId": "91"},
		},
		{
			// RemoteError is the client-side boundary representation. It never
			// originates on the server, but it satisfies Describable, so the
			// method-set scan finds it and the projection is pinned like any
			// other: reason, message and metadata pass through untouched.
			name: "RemoteError",
			err: &domain.RemoteError{
				KindValue:   domain.KindNotFound,
				ReasonValue: domain.ErrReasonLedgerNotFound,
				Message:     "ledger does not exist: remote-ledger",
				Meta:        map[string]string{"name": "remote-ledger"},
			},
			wantReason:  domain.ErrReasonLedgerNotFound,
			wantContext: map[string]string{"name": "remote-ledger"},
		},
		{
			// The EN-1379 key set, pinned on the surface that matters most: an
			// undeclared key is an admission bug, never a storage fault, and
			// operator tooling greps these exact camelCase names out of the
			// hash-chained Context.
			name: "CoverageMiss",
			err: &ErrCoverageMiss{
				Attribute:    "volumes",
				CanonicalHex: "deadbeef",
				IDHex:        "0102",
				RaftIndex:    42,
			},
			wantReason: domain.ErrReasonCoverageMiss,
			wantContext: map[string]string{
				"attribute":    "volumes",
				"canonicalHex": "deadbeef",
				"idHex":        "0102",
				"raftIndex":    "42",
			},
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

// describableScanDirs are the packages whose Describable implementations can
// reach buildAuditFailure. This is a deliberate boundary, not an oversight:
// internal/domain holds the business errors the FSM returns and internal/infra/state
// holds the FSM-local ones (ErrCoverageMiss). Every other implementation in the
// tree — admission's errIdempotencyKeyTooLong / errIdempotencyKeyInvalidUTF8 /
// errCheckpointOrderNotLast, query.ErrAggregateOverflow, grpc.validationError — is
// produced before a proposal exists or on the read path, so it never reaches the
// audit chain. If one of them ever becomes FSM-reachable, add its directory here.
var describableScanDirs = map[string]string{
	"../../domain": "github.com/formancehq/ledger/v3/internal/domain",
	".":            "github.com/formancehq/ledger/v3/internal/infra/state",
}

// TestBuildAuditFailureCoversEveryDescribable is the forcing function: adding a
// Describable without adding a row to auditFailureCases fails this test.
//
// Discovery is by METHOD SET, not by type name. It collects every receiver type
// declaring both Reason() string and Metadata() map[string]string — which is the
// Describable contract itself. The name-prefix scan used by
// TestEveryDomainErrorImplementsDescribable (internal/domain/errors_test.go:318)
// would miss domain.ReplayedFailure, domain.RemoteError and domain.BusinessError,
// all three of which do reach buildAuditFailure, and it would need a hand-maintained
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
		"the AST scan found no Describable implementations at all — the scan is broken, not the table")

	covered := make(map[describableTypeKey]bool)

	for _, tc := range auditFailureCases() {
		rt := reflect.TypeOf(tc.err)
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}

		covered[describableTypeKey{pkg: rt.PkgPath(), name: rt.Name()}] = true
	}

	require.Equal(t, sortedKeys(discovered), sortedKeys(covered),
		"auditFailureCases must hold exactly one row per Describable reachable from the FSM failure path:\n"+
			"  a MISSING entry means a new error type landed with no assertion on what buildAuditFailure writes\n"+
			"  an EXTRA entry means a row references a type that is no longer a Describable")
}

// describableTypesIn returns the names of every type declared in dir whose
// method set includes both Reason() string and Metadata() map[string]string.
func describableTypesIn(t *testing.T, dir string) map[string]bool {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no Go files found in %s — the relative scan path is wrong", dir)

	hasReason := make(map[string]bool)
	hasMetadata := make(map[string]bool)

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
			case "Metadata":
				hasMetadata[receiver] = true
			}
		}
	}

	out := make(map[string]bool)

	for name := range hasReason {
		if hasMetadata[name] {
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

// TestBuildAuditFailure asserts the full projection buildAuditFailure performs,
// for every Describable the FSM can reach. The three assertions live in this
// shared body rather than per row, so a row cannot assert a subset — which is
// how AuditFailure.Message ended up pinned for exactly one error type while
// Reason was pinned for all of them (EN-1772).
//
// The expected message is derived (tc.err.Error()) rather than a literal. That
// proves the projection — the message reaches the proto unmodified — without
// pinning the wording, because rewording a domain error is a legitimate change
// and 74 literal strings would invite blind bulk-updates that destroy the signal.
//
// wantContext IS a literal, because the sorted context keys and values are folded
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
			require.Equal(t, tc.err.Error(), failure.GetMessage(),
				"the error message must reach the hash-chained AuditFailure unmodified")
			require.Equal(t, tc.wantContext, failure.GetContext())

			// The audit projection and the idempotency projection must agree on
			// both shared fields for every error type, not just the one exercised
			// end to end by TestIdempotencyFailureMessageMatchesAudit.
			reason, message := describeFailure(tc.err)
			require.Equal(t, reason, failure.GetReason())
			require.Equal(t, message, failure.GetMessage())
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

// TestBuildAuditFailureDoesNotUnwrap is the negative control for the CoverageMiss
// row in auditFailureCases: buildAuditFailure reads Reason()/Metadata() off the
// OUTERMOST Describable and genuinely does NOT unwrap, so the no-wrap contract has
// to be upheld by every FSM read site going through domain.StoreFailure. If someone
// reintroduces a bare wrap, this is the shape the audit chain would record forever —
// permanently STORAGE_OPERATION_FAILED with the identifying key stripped (EN-1379).
//
// It is deliberately NOT a row in the table. It exercises two errors and asserts a
// negative, and a row would register ErrStorageOperation in the coverage set on the
// strength of a test that is not about that type's own projection.
func TestBuildAuditFailureDoesNotUnwrap(t *testing.T) {
	t.Parallel()

	miss := &ErrCoverageMiss{Attribute: "volumes", IDHex: "0102", RaftIndex: 42}

	failure := buildAuditFailure(&domain.ErrStorageOperation{Operation: "loading volume", Cause: miss})

	require.Equal(t, domain.ErrReasonStorageOperation, domain.ReasonString(failure.GetReason()))
	require.Equal(t, map[string]string{"operation": "loading volume"}, failure.GetContext(),
		"the wrap strips the identifying key — this is what EN-1379 prevents")

	// And the supported path avoids exactly that.
	require.Equal(t, domain.ErrReasonCoverageMiss,
		domain.ReasonString(buildAuditFailure(domain.StoreFailure("loading volume", miss)).GetReason()))
}

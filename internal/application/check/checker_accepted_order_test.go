package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestVerifyAuditHashChain_KeyedNumscriptTxBindsAcceptedOrder pins the
// accepted-order immutability rule at the audit/checker boundary. The FSM
// freezes a keyed proposal's idempotency hash over the ACCEPTED order, before
// ProcessOrders runs, then serializes each order into AuditItem.SerializedOrder
// AFTER processing. If Numscript set_tx_meta output is merged back into the
// caller-owned order.Metadata, the audited bytes diverge from the accepted
// order and the checker re-derives a different hash — a false
// CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH.
//
// Here the caller metadata collides with the script on "type" (caller wins) and
// carries a caller-only key, while the script emits a distinct set_tx_meta key.
// The transaction/log must carry the merged metadata, the audited order must
// carry only the caller's, and the checker must find no idempotency mismatch.
func TestVerifyAuditHashChain_KeyedNumscriptTxBindsAcceptedOrder(t *testing.T) {
	t.Parallel()

	const (
		clusterID = "accepted-order-cluster"
		idemKey   = "keyed-numscript-1"
		ledger    = "test"
		createdAt = 1700000000
	)

	// Phase 1 — run the real processing pipeline to produce the artifacts the
	// FSM would: the created log and the post-processing order serialization.
	engine := newTestEngine(t)
	engine.clusterID = clusterID
	engine.processAndCommit(createLedgerOrder(ledger))

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Metadata: map[string]*commonpb.MetadataValue{
								"type":        commonpb.NewStringValue("caller-wins"),
								"caller-only": commonpb.NewStringValue("kept"),
							},
							Script: &commonpb.Script{Plain: `
								set_tx_meta("type", "payment")
								set_tx_meta("category", "purchase")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`},
						},
					}},
				},
			},
		},
	}

	// The FSM freezes the idempotency hash of the accepted order, before
	// ProcessOrders runs. Capture it pre-processing.
	frozenHash := processing.HashOrders([]*raftcmdpb.Order{order})

	logs := engine.processAndCommit(order)
	require.Len(t, logs, 1)
	logSeq := logs[0].GetSequence()

	createdTx := logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	txMeta := commonpb.MetadataToGoMap(createdTx.GetTransaction().GetMetadata())
	require.Equal(t, "caller-wins", txMeta["type"], "caller metadata must win collisions")
	require.Equal(t, "purchase", txMeta["category"], "script metadata must be merged into the transaction")
	require.Equal(t, "kept", txMeta["caller-only"], "caller-only metadata must be preserved")

	// The audited order bytes are captured after processing. They must equal the
	// accepted order: only the caller's metadata, never the script's.
	serialized := order.MarshalDeterministicVT(nil)

	var audited raftcmdpb.Order
	require.NoError(t, audited.UnmarshalVT(serialized))
	auditedMeta := audited.GetLedgerScoped().GetApply().GetCreateTransaction().GetMetadata()
	require.Contains(t, auditedMeta, "type")
	require.Contains(t, auditedMeta, "caller-only")
	require.NotContains(t, auditedMeta, "category",
		"script set_tx_meta must not be bound into the audited order")

	// Phase 2 — build the audit entry + frozen idempotency projection the FSM
	// would persist for this keyed proposal, then run the checker.
	store := createTestStore(t)

	entry := &auditpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &commonpb.Timestamp{Data: createdAt},
		ProposalId:  2,
		OrderCount:  1,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Idempotency: &commonpb.Idempotency{Key: idemKey},
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{MinLogSequence: logSeq, MaxLogSequence: logSeq},
		},
	}
	items := []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized, LogSequence: logSeq}}
	persistAuditEntry(t, store, entry, items, clusterID)

	writeIdempotencyEntry(t, store, idemKey, &commonpb.IdempotencyKeyValue{
		CreatedAt:        createdAt,
		Hash:             frozenHash,
		FirstLogSequence: logSeq,
		LogCount:         1,
	})

	require.Empty(t, collectIdempotencyMismatches(t, store, clusterID),
		"a keyed Numscript transaction whose audited order matches the accepted order must not trip the idempotency check")
}

// collectIdempotencyMismatches runs the audit-chain verifier and returns only
// the idempotency-mismatch events, isolating the projection check under test.
func collectIdempotencyMismatches(t *testing.T, store *dal.Store, clusterID string) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attributes.New(), clusterID, nil, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []*servicepb.CheckStoreError

	_, err = checker.verifyAuditHashChain(context.Background(), handle, nil, nil, newChainBoundState(), nil,
		func(event *servicepb.CheckStoreEvent) {
			if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
				e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH {
				got = append(got, e.Error)
			}
		})
	require.NoError(t, err)

	return got
}

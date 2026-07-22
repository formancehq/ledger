package admission

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const (
	mActionTotal  = "admission.action.total"
	mActionErrors = "admission.action.errors.total"
)

// recordedActionCounts collects, from the manual reader, the value of each
// action counter broken down by its order_type attribute. The shape is
// metric name -> order_type -> summed value. Absent instruments / series map to
// a missing key (the SDK never created the series), which callers treat as zero.
func recordedActionCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	counts := make(map[string]map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			if m.Name != mActionTotal && m.Name != mActionErrors {
				continue
			}

			byType := make(map[string]int64)
			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("order_type"))
				require.True(t, ok, "action counter data point missing order_type attribute")
				byType[v.AsString()] += dp.Value
			}
			counts[m.Name] = byType
		}
	}

	return counts
}

func createLedgerOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}
}

func createTransactionOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{},
						},
					},
				},
			},
		},
	}
}

func registerSigningKeyOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
					RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{},
				},
			},
		},
	}
}

// TestRecordActionOutcome exercises the counting logic directly: total counts
// every order attempted, errors is the strict subset counted only when the
// batch failed, and both break down by order_type via domain.AuditOrderType.
// Driving it directly avoids faking the full green Admit path (plan.Builder.Run
// + resolved FSM future), which no unit test can cheaply synthesize.
func TestRecordActionOutcome(t *testing.T) {
	t.Parallel()

	t.Run("success increments total only, per order_type", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store, nil)

		var err error // nil == batch succeeded
		a.recordActionOutcome(context.Background(), []*raftcmdpb.Order{createTransactionOrder()}, &err)

		counts := recordedActionCounts(t, reader)
		require.Equal(t, int64(1), counts[mActionTotal]["create_transaction"])
		require.Zero(t, counts[mActionErrors]["create_transaction"], "errors must not increment on success")
	})

	t.Run("failure increments total and errors, per order_type", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store, nil)

		err := errors.New("batch failed")
		a.recordActionOutcome(context.Background(), []*raftcmdpb.Order{createTransactionOrder()}, &err)

		counts := recordedActionCounts(t, reader)
		require.Equal(t, int64(1), counts[mActionTotal]["create_transaction"])
		require.Equal(t, int64(1), counts[mActionErrors]["create_transaction"])
	})

	t.Run("mixed multi-order batch breaks down by order_type", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store, nil)

		// A failed atomic batch: total AND errors count every order under its own
		// order_type — two create_ledger, one create_transaction, one
		// register_signing_key.
		orders := []*raftcmdpb.Order{
			createLedgerOrder(),
			createLedgerOrder(),
			createTransactionOrder(),
			registerSigningKeyOrder(),
		}
		err := errors.New("batch failed")
		a.recordActionOutcome(context.Background(), orders, &err)

		counts := recordedActionCounts(t, reader)
		require.Equal(t, int64(2), counts[mActionTotal]["create_ledger"])
		require.Equal(t, int64(1), counts[mActionTotal]["create_transaction"])
		require.Equal(t, int64(1), counts[mActionTotal]["register_signing_key"])
		require.Equal(t, int64(2), counts[mActionErrors]["create_ledger"])
		require.Equal(t, int64(1), counts[mActionErrors]["create_transaction"])
		require.Equal(t, int64(1), counts[mActionErrors]["register_signing_key"])
	})
}

// TestAdmitActionCounters exercises the wiring: the deferred recorder is
// registered right after orders are built, so real failure paths past that point
// increment both counters under the order's type, while a failure before orders
// exist (the carve-out) records nothing.
func TestAdmitActionCounters(t *testing.T) {
	t.Parallel()

	t.Run("failure after orders built increments total and errors", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store, nil)

		// A CreateTransaction referencing a missing numscript fails in the scripts
		// phase — after orders are built and the recorder defer is registered.
		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								ScriptReference: &servicepb.ScriptReference{Name: "does-not-exist"},
							},
						},
					},
				},
			},
		}))
		require.Error(t, err)

		counts := recordedActionCounts(t, reader)
		require.Equal(t, int64(1), counts[mActionTotal]["create_transaction"])
		require.Equal(t, int64(1), counts[mActionErrors]["create_transaction"])
	})

	t.Run("propose failure increments total and errors", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)

		ctrl := gomock.NewController(t)
		proposer := NewMockProposer(ctrl)
		proposer.EXPECT().InitialIndex().Return(uint64(0)).AnyTimes()
		proposer.EXPECT().
			Propose(gomock.Any(), gomock.Any()).
			Return(nil, commonpb.ErrNoLeader).
			AnyTimes()

		a, reader := createTestAdmissionWithReader(t, store, proposer)

		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger-propose-fail"},
			},
		}))
		require.ErrorIs(t, err, commonpb.ErrNoLeader)

		counts := recordedActionCounts(t, reader)
		require.Equal(t, int64(1), counts[mActionTotal]["create_ledger"])
		require.Equal(t, int64(1), counts[mActionErrors]["create_ledger"])
	})

	t.Run("multi-order propose failure counts each order", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)

		ctrl := gomock.NewController(t)
		proposer := NewMockProposer(ctrl)
		proposer.EXPECT().InitialIndex().Return(uint64(0)).AnyTimes()
		proposer.EXPECT().
			Propose(gomock.Any(), gomock.Any()).
			Return(nil, commonpb.ErrNoLeader).
			AnyTimes()

		a, reader := createTestAdmissionWithReader(t, store, proposer)

		// Two CreateLedger orders in one atomic batch: both are built, the batch
		// fails at propose, so each is counted once under create_ledger.
		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("",
			&servicepb.Request{Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger-a"},
			}},
			&servicepb.Request{Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger-b"},
			}},
		))
		require.ErrorIs(t, err, commonpb.ErrNoLeader)

		counts := recordedActionCounts(t, reader)
		require.Equal(t, int64(2), counts[mActionTotal]["create_ledger"])
		require.Equal(t, int64(2), counts[mActionErrors]["create_ledger"])
	})

	t.Run("pre-order failure records nothing (carve-out)", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store, nil)

		// A revert with transaction id 0 is rejected inside requestsToOrders, before
		// orders exist and before the recorder defer is registered — so no action
		// counter is touched.
		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_RevertTransaction{
							RevertTransaction: &servicepb.RevertTransactionPayload{TransactionId: 0},
						},
					},
				},
			},
		}))
		require.ErrorIs(t, err, domain.ErrTransactionTargetMissing)

		counts := recordedActionCounts(t, reader)
		require.Empty(t, counts[mActionTotal], "no action.total series expected for a pre-order failure")
		require.Empty(t, counts[mActionErrors], "no action.errors series expected for a pre-order failure")
	})
}

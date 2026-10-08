package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/health"
)

func TestAdmitRejectsWhenUnhealthy(t *testing.T) {
	t.Parallel()

	t.Run("rejects create ledger when health check fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockWriteGate := health.NewMockWriteGate(ctrl)
		mockWriteGate.EXPECT().CheckWritesAllowed().Return(domain.ErrWritesBlockedDiskFull)

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)
		a.writeGate = mockWriteGate

		_, err := a.Admit(attributedTestContext(context.Background()), ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_CreateLedger{
				CreateLedger: &ledgerpb.CreateLedgerRequest{
					Name: "test-ledger-rejected",
				},
			},
		}))

		require.ErrorIs(t, err, domain.ErrWritesBlockedDiskFull)
	})

	t.Run("rejects batch requests when health check fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockWriteGate := health.NewMockWriteGate(ctrl)
		mockWriteGate.EXPECT().CheckWritesAllowed().Return(domain.ErrWritesBlockedDiskFull)

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)
		a.writeGate = mockWriteGate

		_, err := a.Admit(attributedTestContext(context.Background()),
			ledgerpb.UnsignedApplyRequest("",
				&ledgerpb.Request{
					Type: &ledgerpb.Request_CreateLedger{
						CreateLedger: &ledgerpb.CreateLedgerRequest{
							Name: "ledger1",
						},
					},
				},
				&ledgerpb.Request{
					Type: &ledgerpb.Request_Apply{
						Apply: &ledgerpb.LedgerApplyRequest{
							Ledger: "ledger1",
							Action: &ledgerpb.LedgerAction{
								Data: &ledgerpb.LedgerAction_CreateTransaction{
									CreateTransaction: &ledgerpb.CreateTransactionPayload{
										Postings: []*ledgerpb.Posting{
											{
												Source:      "world",
												Destination: "user:alice",
												Amount:      ledgerpb.NewUint256FromUint64(0),
												Asset:       "USD",
											},
										},
									},
								},
							},
						},
					},
				},
			),
		)

		require.ErrorIs(t, err, domain.ErrWritesBlockedDiskFull)
	})

	t.Run("rejects apply request when health check fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockWriteGate := health.NewMockWriteGate(ctrl)
		mockWriteGate.EXPECT().CheckWritesAllowed().Return(domain.ErrWritesBlockedDiskFull)

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)
		a.writeGate = mockWriteGate

		_, err := a.Admit(attributedTestContext(context.Background()), ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &ledgerpb.LedgerAction{
						Data: &ledgerpb.LedgerAction_CreateTransaction{
							CreateTransaction: &ledgerpb.CreateTransactionPayload{
								Postings: []*ledgerpb.Posting{
									{
										Source:      "world",
										Destination: "user:bob",
										Amount:      ledgerpb.NewUint256FromUint64(0),
										Asset:       "EUR",
									},
								},
							},
						},
					},
				},
			},
		}))

		require.ErrorIs(t, err, domain.ErrWritesBlockedDiskFull)
	})
}

func TestBarrierRejectsWhenWritesBlocked(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockWriteGate := health.NewMockWriteGate(ctrl)
	mockWriteGate.EXPECT().CheckWritesAllowed().Return(domain.ErrWritesBlockedDiskFull)

	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	a.writeGate = mockWriteGate

	// createTestAdmission wires a nil proposer; Barrier must short-circuit on the
	// blocked write gate before reaching it (a no-op barrier still appends to the
	// Raft WAL and must be gated like any other write). Reaching the proposer
	// would panic, so a clean error return proves the gate is consulted first.
	idx, err := a.Barrier(context.Background())

	require.ErrorIs(t, err, domain.ErrWritesBlockedDiskFull)
	require.Zero(t, idx)
}

package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAdmitRejectsWhenUnhealthy(t *testing.T) {
	t.Parallel()

	t.Run("rejects create ledger when health check fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockChecker := health.NewMockChecker(ctrl)
		mockChecker.EXPECT().IsHealthy().Return(false)

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)
		a.healthChecker = mockChecker

		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test-ledger-rejected",
				},
			},
		}))

		require.ErrorIs(t, err, health.ErrUnhealthy)
	})

	t.Run("rejects batch requests when health check fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockChecker := health.NewMockChecker(ctrl)
		mockChecker.EXPECT().IsHealthy().Return(false)

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)
		a.healthChecker = mockChecker

		_, err := a.Admit(context.Background(),
			servicepb.UnsignedApplyRequest("",
				&servicepb.Request{
					Type: &servicepb.Request_CreateLedger{
						CreateLedger: &servicepb.CreateLedgerRequest{
							Name: "ledger1",
						},
					},
				},
				&servicepb.Request{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: "ledger1",
							Action: &servicepb.LedgerAction{
								Data: &servicepb.LedgerAction_CreateTransaction{
									CreateTransaction: &servicepb.CreateTransactionPayload{
										Postings: []*commonpb.Posting{
											{
												Source:      "world",
												Destination: "user:alice",
												Amount:      commonpb.NewUint256FromUint64(0),
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

		require.ErrorIs(t, err, health.ErrUnhealthy)
	})

	t.Run("rejects apply request when health check fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockChecker := health.NewMockChecker(ctrl)
		mockChecker.EXPECT().IsHealthy().Return(false)

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)
		a.healthChecker = mockChecker

		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:bob",
										Amount:      commonpb.NewUint256FromUint64(0),
										Asset:       "EUR",
									},
								},
							},
						},
					},
				},
			},
		}))

		require.ErrorIs(t, err, health.ErrUnhealthy)
	})
}

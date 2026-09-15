package admission

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Event transports must represent the admitted ledger-name domain themselves;
// restricting ledger admission to work around NATS subject syntax is not valid.
// Exercise Admit through proposal serialization, rather than only the validator.
func TestAdmit_CreateLedgerNamesForEventRouting(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"orders", "a.b", "a..b", ".orders", "orders.", ".", "_systemx", "__system", "_System", "a-b_0", strings.Repeat("a", dal.LedgerNameFixedSize)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			proposer := NewMockProposer(gomock.NewController(t))
			proposed := errors.New("create ledger proposal reached consensus boundary")
			proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
					var command raftcmdpb.Proposal
					require.NoError(t, command.UnmarshalVT(proposal.Data()))
					require.Len(t, command.GetOrders(), 1)
					order := command.GetOrders()[0].GetLedgerScoped()
					require.NotNil(t, order.GetCreateLedger())
					require.Equal(t, name, order.GetLedger())

					return nil, proposed
				},
			).Times(1)
			admission, _ := createTestAdmissionWithReader(t, store, proposer)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			_, err := admission.Admit(ctx, businessWrite(name))
			require.ErrorIs(t, err, proposed)
		})
	}

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"", domain.ErrLedgerNameRequired},
		{"_", ErrLedgerNameReservedPrefix},
		{"_system", ErrLedgerNameReservedPrefix},
		{"a b", domain.ErrLedgerNameInvalidChar},
		{"a\x00b", domain.ErrLedgerNameInvalidChar},
	} {
		t.Run("rejected_"+tc.name, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			// No Propose expectation: malformed names must fail before consensus.
			proposer := NewMockProposer(gomock.NewController(t))
			admission, _ := createTestAdmissionWithReader(t, store, proposer)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			_, err := admission.Admit(ctx, businessWrite(tc.name))
			require.ErrorIs(t, err, tc.err)
		})
	}
}

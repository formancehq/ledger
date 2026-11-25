package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAdmitQueryCheckpointSchedule(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		cron       string
		neverFires bool
	}{
		{cron: "0 0 31 2 *", neverFires: true},
		{cron: "0 0 0 31 2 *", neverFires: true},
		{cron: "0 0 * * *"},
		{cron: "0 0 29 2 *"},
		{cron: "0 0 31 2 MON"},
		{cron: "* * * * * *"},
		{cron: "@every 1h"},
		{cron: "not-a-cron"},
		{cron: ""},
	} {
		t.Run(test.cron, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			proposer := NewMockProposer(gomock.NewController(t))
			proposed := errors.New("proposal reached")
			if !test.neverFires {
				proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil, proposed)
			}
			admission, _ := createTestAdmissionWithReader(t, store, proposer)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := admission.Admit(attributedTestContext(ctx), servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{Cron: test.cron}},
			}))
			if test.neverFires {
				var invalid *domain.ErrInvalidCronExpression
				require.ErrorAs(t, err, &invalid)
				require.Equal(t, test.cron, invalid.Expression)
				require.Equal(t, "schedule never fires", invalid.Details)
				var business *domain.BusinessError
				require.ErrorAs(t, err, &business)
			} else {
				require.ErrorIs(t, err, proposed)
			}
		})
	}
}

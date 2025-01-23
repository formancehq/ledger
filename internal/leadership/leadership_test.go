package leadership

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaderShip(t *testing.T) {

	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)

	const count = 10
	selectedInstance := atomic.Int32{}

	type instance struct {
		leadership *Leadership
		ctx        context.Context
		cancel     func()
	}

	instances := make([]instance, count)
	for i := range count {
		m := NewMockLocker(ctrl)
		m.EXPECT().
			Take(gomock.Any()).
			AnyTimes().
			DoAndReturn(func(ctx context.Context) (bool, func(), error) {
				return i == int(selectedInstance.Load()), func() {}, nil
			})

		l := NewLeadership(m, logging.Testing(), WithRetryPeriod(10*time.Millisecond))

		ctx, cancel := context.WithCancel(ctx)

		go l.Run(ctx)

		instances[i] = instance{
			leadership: l,
			ctx:        ctx,
			cancel:     cancel,
		}
	}

	for _, nextLeader := range []int{0, 2, 4, 8} {
		selectedInstance.Store(int32(nextLeader))

		leadershipSignal, release := instances[nextLeader].leadership.GetSignal().Listen()
		select {
		case acquired := <-leadershipSignal:
			require.True(t, acquired, "instance %d should be leader", nextLeader)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("signal should have been received")
		}

		instances[nextLeader].cancel()

		select {
		case acquired := <-leadershipSignal:
			require.False(t, acquired, "instance %d should have lost the leadership", nextLeader)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("signal should have been received")
		}
		release()
	}
}

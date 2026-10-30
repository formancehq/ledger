package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
)

type recordingLeadershipManager struct {
	calls []bool
}

func (m *recordingLeadershipManager) OnLeadershipChange(isLeader bool) {
	m.calls = append(m.calls, isLeader)
}

// TestManagerLeadership pins when the event and mirror managers learn about
// leadership: a gain reaches them through LeaderReadyEvent only, once the
// leader's store is current, and a loss reaches them at once.
func TestManagerLeadership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		emit func(events, mirrors leadershipChangeManager)
		want []bool
	}{
		{
			name: "leadership gain does not start the managers",
			emit: func(events, mirrors leadershipChangeManager) {
				handleLeadershipChangeEvent(node.LeadershipChangeEvent{IsLeader: true}, events, mirrors, logging.Testing())
			},
			want: nil,
		},
		{
			name: "leader readiness starts the managers",
			emit: func(events, mirrors leadershipChangeManager) {
				handleLeaderReadyEvent(events, mirrors, logging.Testing())
			},
			want: []bool{true},
		},
		{
			name: "leadership loss stops the managers",
			emit: func(events, mirrors leadershipChangeManager) {
				handleLeadershipChangeEvent(node.LeadershipChangeEvent{IsLeader: false}, events, mirrors, logging.Testing())
			},
			want: []bool{false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			events, mirrors := &recordingLeadershipManager{}, &recordingLeadershipManager{}
			tt.emit(events, mirrors)

			require.Equal(t, tt.want, events.calls)
			require.Equal(t, tt.want, mirrors.calls)
		})
	}
}

// A new raw leadership gain must not reactivate workers after a loss until
// that generation's FSM catch-up has published LeaderReadyEvent.
func TestManagerLeadershipLossAndRegainRequiresNewReadiness(t *testing.T) {
	t.Parallel()

	events, mirrors := &recordingLeadershipManager{}, &recordingLeadershipManager{}
	handleLeadershipChangeEvent(node.LeadershipChangeEvent{IsLeader: true}, events, mirrors, logging.Testing())
	handleLeaderReadyEvent(events, mirrors, logging.Testing())
	for _, isLeader := range []bool{false, true} {
		handleLeadershipChangeEvent(node.LeadershipChangeEvent{IsLeader: isLeader}, events, mirrors, logging.Testing())
	}
	require.Equal(t, []bool{true, false}, events.calls)
	require.Equal(t, []bool{true, false}, mirrors.calls)

	handleLeaderReadyEvent(events, mirrors, logging.Testing())
	require.Equal(t, []bool{true, false, true}, events.calls)
	require.Equal(t, []bool{true, false, true}, mirrors.calls)
}

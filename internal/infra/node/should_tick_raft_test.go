package node

import "testing"

// TestShouldTickRaft pins the contract from #316: a leader paused for
// maintenance (statusGated) MUST keep emitting heartbeats. The tick
// suppression exists for nodes that are genuinely behind raft state —
// statusOutOfSync (needs catch-up) and statusInstallingSnapshot
// (snapshot install in progress).
func TestShouldTickRaft(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int32
		want   bool
	}{
		{"normal", statusNormal, true},
		{"gated", statusGated, true},
		{"out of sync", statusOutOfSync, false},
		{"installing snapshot", statusInstallingSnapshot, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := shouldTickRaft(tt.status); got != tt.want {
				t.Errorf("shouldTickRaft(%v) = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

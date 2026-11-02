package node

import "testing"

// TestShouldTickRaft pins two contracts: a leader paused for maintenance
// (statusGated) MUST keep emitting heartbeats (#316), and an out-of-sync
// voter MUST keep running election timeouts — its log is complete, and
// withholding its ticks can leave the cluster without a leader when the
// newest logs sit on the resynchronizing nodes. Only a snapshot install
// pauses the clock.
func TestShouldTickRaft(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int32
		want   bool
	}{
		{"normal", statusNormal, true},
		{"gated", statusGated, true},
		{"out of sync", statusOutOfSync, true},
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

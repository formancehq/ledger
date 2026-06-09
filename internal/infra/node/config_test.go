package node

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNodeConfigValidate_NumericFlags covers the validation added for issue
// R-036: bad numeric flags must fail before fx startup, never panic the
// channel allocator or silently disable retries at runtime.
func TestNodeConfigValidate_NumericFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*NodeConfig)
		wantErr string
	}{
		{
			name:   "defaults: all zero is accepted (SetDefaults fills them)",
			mutate: func(c *NodeConfig) {},
		},
		{
			name:    "negative propose queue panics make(chan, -1) at startup",
			mutate:  func(c *NodeConfig) { c.ProposeQueueCapacity = -1 },
			wantErr: "--propose-queue-capacity",
		},
		{
			name:    "absurd propose queue OOMs the allocator",
			mutate:  func(c *NodeConfig) { c.ProposeQueueCapacity = math.MaxInt64 },
			wantErr: "--propose-queue-capacity",
		},
		{
			name:    "negative transport buffer size",
			mutate:  func(c *NodeConfig) { c.TransportBufferSize = -1 },
			wantErr: "--transport-buffer-size",
		},
		{
			name:    "negative replay batch size",
			mutate:  func(c *NodeConfig) { c.ReplayBatchSize = -1 },
			wantErr: "--replay-batch-size",
		},
		{
			name:    "negative election tick",
			mutate:  func(c *NodeConfig) { c.ElectionTick = -1 },
			wantErr: "--election-tick",
		},
		{
			name:    "election tick beyond sanity bound",
			mutate:  func(c *NodeConfig) { c.ElectionTick = maxRaftTicks + 1 },
			wantErr: "--election-tick",
		},
		{
			name:    "max-size-per-msg beyond 1GiB",
			mutate:  func(c *NodeConfig) { c.MaxSizePerMsg = maxBufferBytes + 1 },
			wantErr: "--max-size-per-msg",
		},
		{
			name:    "rotation threshold beyond cap",
			mutate:  func(c *NodeConfig) { c.RotationThreshold = maxQueueCapacity + 1 },
			wantErr: "--rotation-threshold",
		},
		{
			name:    "negative maintenance interval",
			mutate:  func(c *NodeConfig) { c.MaintenanceInterval = -time.Second },
			wantErr: "--maintenance-interval",
		},
		{
			name:    "negative tick interval",
			mutate:  func(c *NodeConfig) { c.TickInterval = -time.Millisecond },
			wantErr: "--tick-interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := NodeConfig{NodeID: 1, BindAddr: "127.0.0.1:7777"}
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr,
				"error should name the offending flag so ops can find it")
		})
	}
}

func TestTransportConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     TransportConfig
		wantErr string
	}{
		{
			name: "valid 3-slot priorities",
			cfg: TransportConfig{
				Reception: []int{10, 512, 512},
				Send:      []int{10, 512, 512},
			},
		},
		{
			name: "reception missing slots",
			cfg: TransportConfig{
				Reception: []int{10, 512},
				Send:      []int{10, 512, 512},
			},
			wantErr: "--raft-transport-reception-queues",
		},
		{
			name: "send has extra slot",
			cfg: TransportConfig{
				Reception: []int{10, 512, 512},
				Send:      []int{10, 512, 512, 512},
			},
			wantErr: "--raft-transport-send-queues",
		},
		{
			name: "negative reception capacity panics make(chan, -1)",
			cfg: TransportConfig{
				Reception: []int{10, -1, 512},
				Send:      []int{10, 512, 512},
			},
			wantErr: "--raft-transport-reception-queues",
		},
		{
			name: "absurd send capacity",
			cfg: TransportConfig{
				Reception: []int{10, 512, 512},
				Send:      []int{10, math.MaxInt32, 512},
			},
			wantErr: "--raft-transport-send-queues",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

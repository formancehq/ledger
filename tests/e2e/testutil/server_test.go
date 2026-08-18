//go:build e2e

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodePortConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		httpPort    int
		grpcPort    int
		raftPort    int
		errContains string
	}{
		{
			name:     "shared node owns its reserved ports",
			httpPort: TestSingleHTTPPort,
			grpcPort: TestSingleGRPCPort,
			raftPort: TestSingleGRPCPort - 1000,
		},
		{
			name:     "auxiliary node uses distinct ports",
			httpPort: 18000,
			grpcPort: 18100,
			raftPort: 17100,
		},
		{
			name:        "HTTP port collides",
			httpPort:    TestSingleHTTPPort,
			grpcPort:    18100,
			raftPort:    17100,
			errContains: "HTTP port 15200",
		},
		{
			name:        "gRPC port collides",
			httpPort:    18000,
			grpcPort:    TestSingleGRPCPort,
			raftPort:    17000,
			errContains: "gRPC port 15100",
		},
		{
			name:        "Raft port collides",
			httpPort:    16100,
			grpcPort:    16200,
			raftPort:    TestSingleHTTPPort,
			errContains: "Raft port (derived from gRPC port 16200 minus 1000) 15200",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := nodePortConflict(test.httpPort, test.grpcPort, test.raftPort)
			if test.errContains == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, test.errContains)
		})
	}
}

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
			// raftPort matches what SetupSingleNode derives (grpcPort - 1000),
			// so the tuple is one the production call site can actually build.
			name:        "gRPC port collides",
			httpPort:    18000,
			grpcPort:    TestSingleGRPCPort,
			raftPort:    TestSingleGRPCPort - 1000,
			errContains: "gRPC port 15100",
		},
		{
			name:        "Raft port collides",
			httpPort:    16100,
			grpcPort:    16200,
			raftPort:    TestSingleHTTPPort,
			errContains: "Raft port (derived from gRPC port 16200 minus 1000) 15200",
		},
		{
			// The shared node's derived Raft port is reserved too, and nothing
			// else in this table ever passes 14100 as a port value. An auxiliary
			// spec picking it for HTTP would collide with the shared node's Raft
			// listener.
			name:        "shared node's derived Raft port is reserved",
			httpPort:    TestSingleGRPCPort - 1000,
			grpcPort:    18100,
			raftPort:    17100,
			errContains: "HTTP port 14100 is reserved for the shared single-node server (the shared node's derived Raft port)",
		},
		{
			// The other gRPC value that slips past the HTTP and gRPC checks and
			// is only caught on its derived Raft port. 16100 is the live value of
			// the business suite's s3CredsHTTPPort, so transposing that spec's
			// HTTP and gRPC constants is the plausible next collision.
			name:        "Raft port collides via the second uncaught gRPC value",
			httpPort:    18000,
			grpcPort:    16100,
			raftPort:    TestSingleGRPCPort,
			errContains: "Raft port (derived from gRPC port 16100 minus 1000) 15100",
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

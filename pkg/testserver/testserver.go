package testserver

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
)

// Option functions

func WithHTTPPort(port int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--http-port", fmt.Sprintf("%d", port))
		return nil
	}
}

func WithGRPCPort(port int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--grpc-port", fmt.Sprintf("%d", port))
		return nil
	}
}

func WithRaftPort(port int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--bind-addr", fmt.Sprintf(":%d", port))
		return nil
	}
}

func WithWalDir(dir string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--wal-dir", dir)
		return nil
	}
}

func WithDataDir(dir string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--data-dir", dir)
		return nil
	}
}

func WithNodeID(nodeID int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--node-id", fmt.Sprintf("%d", nodeID))
		return nil
	}
}

func WithAdvertiseAddr(addr string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--advertise-addr", addr)
		return nil
	}
}

func WithPeers(peers ...node.Peer) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		for _, peer := range peers {
			// Format: <id>/<raftAddress>/<serviceAddress>
			cfg.AppendArgs("--peers", fmt.Sprintf("%d/%s/%s", peer.ID, peer.Address, peer.ServiceAddress))
		}
		return nil
	}
}

func WithSnapshotThreshold(threshold int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--snapshot-threshold", fmt.Sprintf("%d", threshold))
		return nil
	}
}

func WithRaftElectionTick(tick int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-election-tick", fmt.Sprintf("%d", tick))
		return nil
	}
}

func WithRaftHeartbeatTick(tick int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-heartbeat-tick", fmt.Sprintf("%d", tick))
		return nil
	}
}

func WithRaftMaxSizePerMsg(size uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-max-size-per-msg", fmt.Sprintf("%d", size))
		return nil
	}
}

func WithRaftMaxInflightMsgs(count int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-max-inflight-msgs", fmt.Sprintf("%d", count))
		return nil
	}
}

func WithRaftTickInterval(interval time.Duration) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-tick-interval", interval.String())
		return nil
	}
}

func WithRaftCompactionMargin(margin uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-compaction-margin", fmt.Sprintf("%d", margin))
		return nil
	}
}

func WithRaftProposeQueueCapacity(capacity int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-propose-queue-capacity", fmt.Sprintf("%d", capacity))
		return nil
	}
}

func WithRaftTransportReceptionQueues(capacities ...int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		for _, cap := range capacities {
			cfg.AppendArgs("--raft-transport-reception-queues", fmt.Sprintf("%d", cap))
		}
		return nil
	}
}

func WithRaftTransportSendQueues(capacities ...int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		for _, cap := range capacities {
			cfg.AppendArgs("--raft-transport-send-queues", fmt.Sprintf("%d", cap))
		}
		return nil
	}
}

func WithDebug(v bool) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		if v {
			cfg.AppendArgs("--debug")
		}
		return nil
	}
}

func WithAuditEnabled(v bool) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--audit-enabled", fmt.Sprintf("%t", v))
		return nil
	}
}

func WithJoin(serviceAddr string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--join", serviceAddr)
		return nil
	}
}

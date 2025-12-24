package testserver

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
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
		cfg.AppendArgs("--bind-addr", fmt.Sprintf(":%d", port))
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

func WithPeers(peers ...raft.Peer) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		for _, peer := range peers {
			cfg.AppendArgs("--peers", fmt.Sprintf("%d/%s", peer.ID, peer.Address))
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

func WithDebug(v bool) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		if v {
			cfg.AppendArgs("--debug")
		}
		return nil
	}
}

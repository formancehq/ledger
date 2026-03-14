package testserver

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/formancehq/go-libs/v3/testing/testservice"
)

// Option functions

func WithHTTPPort(port int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--http-port", strconv.Itoa(port))

		return nil
	}
}

func WithGRPCPort(port int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--grpc-port", strconv.Itoa(port))

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
		cfg.AppendArgs("--node-id", strconv.Itoa(nodeID))

		return nil
	}
}

func WithAdvertiseAddr(addr string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--advertise-addr", addr)

		return nil
	}
}

func WithBootstrap() testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--bootstrap")

		return nil
	}
}

func WithAutoPromoteThreshold(threshold uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--learner-promotion-threshold", strconv.FormatUint(threshold, 10))

		return nil
	}
}

func WithSnapshotThreshold(threshold int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--snapshot-threshold", strconv.Itoa(threshold))

		return nil
	}
}

func WithRaftElectionTick(tick int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-election-tick", strconv.Itoa(tick))

		return nil
	}
}

func WithRaftHeartbeatTick(tick int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-heartbeat-tick", strconv.Itoa(tick))

		return nil
	}
}

func WithRaftMaxSizePerMsg(size uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-max-size-per-msg", strconv.FormatUint(size, 10))

		return nil
	}
}

func WithRaftMaxInflightMsgs(count int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-max-inflight-msgs", strconv.Itoa(count))

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
		cfg.AppendArgs("--raft-compaction-margin", strconv.FormatUint(margin, 10))

		return nil
	}
}

func WithRaftProposeQueueCapacity(capacity int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--raft-propose-queue-capacity", strconv.Itoa(capacity))

		return nil
	}
}

func WithRaftTransportReceptionQueues(capacities ...int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		for _, cap := range capacities {
			cfg.AppendArgs("--raft-transport-reception-queues", strconv.Itoa(cap))
		}

		return nil
	}
}

func WithRaftTransportSendQueues(capacities ...int) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		for _, cap := range capacities {
			cfg.AppendArgs("--raft-transport-send-queues", strconv.Itoa(cap))
		}

		return nil
	}
}

func WithClusterID(id string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--cluster-id", id)

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

func WithReceiptSigningKey(key string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--receipt-signing-key", key)

		return nil
	}
}

func WithJoin(serviceAddr string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--join", serviceAddr)

		return nil
	}
}

func WithTLSCertFile(path string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--tls-cert-file", path)

		return nil
	}
}

func WithTLSKeyFile(path string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--tls-key-file", path)

		return nil
	}
}

func WithTLSCACertFile(path string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--tls-ca-cert-file", path)

		return nil
	}
}

func WithRestore() testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--restore")

		return nil
	}
}

func WithResponseSigningKey(path string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--response-signing-key", path)

		return nil
	}
}

func WithAuthEnabled() testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--auth-enabled")

		return nil
	}
}

func WithAuthIssuer(issuer string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--auth-issuer", issuer)

		return nil
	}
}

func WithAuthService(service string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--auth-service", service)

		return nil
	}
}

func WithAuthEd25519Keys(path string) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--auth-ed25519-keys", path)

		return nil
	}
}

func WithCacheRotationThreshold(threshold uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--cache-rotation-threshold", strconv.FormatUint(threshold, 10))

		return nil
	}
}

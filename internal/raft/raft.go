package raft

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
)

type RaftCluster struct {
	raft   *raft.Raft
	config *config.Config
	logger *zap.Logger
}

func NewRaftCluster(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*RaftCluster, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	// Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Output: &raftLogger{logger: logger},
		Level:  hclog.Info,
	})

	// Create transport
	// Resolve advertise address for Raft (this is what other nodes will connect to)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving advertise address: %w", err)
	}

	hclogger := hclog.New(&hclog.LoggerOptions{
		Output: &raftLogger{logger: logger},
		Level:  hclog.Info,
	})

	transport, err := raft.NewTCPTransportWithLogger(cfg.BindAddr, advertiseAddr, 3, 10*time.Second, hclogger)
	if err != nil {
		return nil, fmt.Errorf("creating transport: %w", err)
	}

	// Create log store
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}

	// Create stable store
	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return nil, fmt.Errorf("creating stable store: %w", err)
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot store: %w", err)
	}

	// Create FSM (Finite State Machine)
	fsm := NewFSM(logger)

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("creating raft: %w", err)
	}

	return &RaftCluster{
		raft:   r,
		config: cfg,
		logger: logger,
	}, nil
}

func (r *RaftCluster) Start() error {
	// If this is the first node, bootstrap the cluster
	if _, serverID := r.raft.LeaderWithID(); serverID == "" && r.config.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(r.config.NodeID),
				Address: raft.ServerAddress(r.config.AdvertiseAddr),
			},
		}

		// Add peers if provided
		// Extract node ID from peer address (assumes format "node-X:port")
		for _, peerAddr := range r.config.Peers {
			// Extract hostname from address (e.g., "node-1:7000" -> "node-1")
			host, _, err := net.SplitHostPort(peerAddr)
			if err != nil {
				r.logger.Warn("Invalid peer address format, skipping", zap.String("peer", peerAddr), zap.Error(err))
				continue
			}
			// Use hostname as node ID (assumes hostname matches node ID)
			peerID := host
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peerID),
				Address: raft.ServerAddress(peerAddr),
			})
		}

		configuration := raft.Configuration{
			Servers: servers,
		}
		future := r.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			// If cluster is already bootstrapped, this is fine
			if err != raft.ErrCantBootstrap {
				return fmt.Errorf("bootstrapping cluster: %w", err)
			}
		}
		r.logger.Info("Cluster bootstrapped", zap.Int("servers", len(servers)))
	}

	return nil
}

func (r *RaftCluster) Shutdown() error {
	r.logger.Info("Shutting down Raft cluster")
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("shutting down raft: %w", err)
	}
	return nil
}

func (r *RaftCluster) GetRaft() *raft.Raft {
	return r.raft
}

// raftLogger adapts zap.Logger to raft.Logger interface
type raftLogger struct {
	logger *zap.Logger
}

func (l *raftLogger) Write(p []byte) (n int, err error) {
	l.logger.Info(string(p[:len(p)-1])) // Remove trailing newline
	return len(p), nil
}

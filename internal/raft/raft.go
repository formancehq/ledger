package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
)

type Cluster struct {
	raft          *raft.Raft
	config        *config.Config
	logger        *zap.Logger
	grpcServer    *grpc.Server
	grpcClient    *grpc.Client
	defaultLedger *service.DefaultLedger
	logStore      service.LogStore // Can be SQLiteLogStore or FileLogStore
	closeStore    func() error     // Function to close the store
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewRaftCluster(parentCtx context.Context, cfg *config.Config, logger *zap.Logger) (*Cluster, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	// Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Output: &raftLogger{logger: logger},
		Level:  hclog.Debug,
	})

	// Create transport
	// Resolve advertise address for Raft (this is what other nodes will connect to)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving advertise address: %w", err)
	}

	hclogger := hclog.New(&hclog.LoggerOptions{
		Output: &raftLogger{logger: logger},
		Level:  hclog.Debug,
	})

	transport, err := raft.NewTCPTransportWithLogger(cfg.BindAddr, advertiseAddr, 3, 10*time.Second, hclogger)
	if err != nil {
		return nil, fmt.Errorf("creating transport: %w", err)
	}

	// Create Raft log store (for Raft's internal logs)
	raftLogStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	raftLogStore, err := raftboltdb.NewBoltStore(raftLogStorePath)
	if err != nil {
		return nil, fmt.Errorf("creating raft log store: %w", err)
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

	ctx, cancel := context.WithCancel(parentCtx)

	// Create application log store based on storage type
	var appLogStore service.LogStore
	var closeStore func() error

	switch cfg.StorageType {
	case "sqlite":
		sqliteStore, err := service.NewSQLiteLogStore(ctx, cfg.SQLiteDSN, logger)
		if err != nil {
			cancel() // Clean up context on error
			return nil, fmt.Errorf("creating sqlite store: %w", err)
		}
		appLogStore = sqliteStore
		closeStore = func() error {
			return sqliteStore.Close()
		}
	case "file":
		fileStore, err := service.NewFileLogStore(cfg.StorageFilePath, logger)
		if err != nil {
			cancel() // Clean up context on error
			return nil, fmt.Errorf("creating file store: %w", err)
		}
		appLogStore = fileStore
		closeStore = func() error {
			return fileStore.Close()
		}
	default:
		cancel() // Clean up context on error
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.StorageType)
	}

	// Create FSM (Finite State Machine) with application log store
	fsm := NewFSM(logger, appLogStore, appLogStore)

	// Create Raft instance (using Raft's log store, not application log store)
	r, err := raft.NewRaft(raftConfig, fsm, raftLogStore, stableStore, snapshotStore, transport)
	if err != nil {
		cancel() // Clean up context on error
		closeStore()
		return nil, fmt.Errorf("creating raft: %w", err)
	}

	// Create RaftLogWriter for writing logs via Raft
	raftLogWriter := service.NewRaftLogWriter(r, logger)

	// Create reconstructed volumes store (reconstructs volumes from logs)
	volumesStore := service.NewReconstructedVolumesStore(appLogStore)

	// Create ledger service (will use RaftLogWriter to persist logs via Raft)
	// appLogStore implements LogReader, volumesStore implements VolumesStore
	defaultLedger := service.NewDefaultLedger(raftLogWriter, volumesStore, appLogStore, logger)

	cluster := &Cluster{
		raft:          r,
		config:        cfg,
		logger:        logger,
		grpcServer:    grpc.NewServer(cfg.GRPCPort, logger, defaultLedger),
		grpcClient:    grpc.NewClient(logger),
		defaultLedger: defaultLedger,
		logStore:      appLogStore,
		ctx:           ctx,
		cancel:        cancel,
	}

	// Store close function for shutdown
	cluster.closeStore = closeStore

	return cluster, nil
}

func (r *Cluster) Start() error {
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
			// Extract hostname from address (e.g., "node-1:8888" -> "node-1")
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
			if !errors.Is(err, raft.ErrCantBootstrap) {
				return fmt.Errorf("bootstrapping cluster: %w", err)
			}
		}
		r.logger.Info("Cluster bootstrapped", zap.Int("servers", len(servers)))
	}

	// Start leader monitoring
	go r.monitorLeader()

	return nil
}

func (r *Cluster) monitorLeader() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastLeaderAddr string

	// Handle initial leader state
	_, leaderID := r.raft.LeaderWithID()
	leaderAddr := string(r.raft.Leader())
	lastLeaderAddr = leaderAddr
	r.handleLeaderChange(leaderID, leaderAddr)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			_, leaderID := r.raft.LeaderWithID()
			leaderAddr := string(r.raft.Leader())

			// Check if leader changed
			if leaderAddr != lastLeaderAddr {
				r.logger.Info("Leader changed", zap.String("old", lastLeaderAddr), zap.String("new", leaderAddr))
				lastLeaderAddr = leaderAddr
				r.handleLeaderChange(leaderID, leaderAddr)
			}
		}
	}
}

func (r *Cluster) handleLeaderChange(leaderID raft.ServerID, leaderAddr string) {
	// Check if we are the leader
	isLeader := leaderID == raft.ServerID(r.config.NodeID)

	if isLeader {
		r.logger.Info("Became leader, starting gRPC server")
		// Stop client if running
		r.grpcClient.Close()

		// Start gRPC server
		go func() {
			if err := r.grpcServer.Start(r.ctx); err != nil {
				r.logger.Error("gRPC server error", zap.Error(err))
			}
		}()
	} else if leaderAddr != "" {
		r.logger.Info("Leader changed, connecting to new leader", zap.String("leader", leaderAddr))
		// Stop server if running
		r.grpcServer.Stop()

		// Extract hostname from leader address and construct gRPC address
		host, _, err := net.SplitHostPort(leaderAddr)
		if err != nil {
			r.logger.Error("Failed to parse leader address", zap.String("address", leaderAddr), zap.Error(err))
			return
		}

		// Connect to leader's gRPC server (assume same host, different port)
		grpcAddr := fmt.Sprintf("%s:%d", host, r.config.GRPCPort)
		if err := r.grpcClient.Connect(r.ctx, grpcAddr); err != nil {
			r.logger.Error("Failed to connect to leader gRPC", zap.String("address", grpcAddr), zap.Error(err))
		}
	} else {
		r.logger.Debug("No leader available")
		// Stop both server and client
		r.grpcServer.Stop()
		r.grpcClient.Close()
	}
}

func (r *Cluster) Shutdown() error {
	r.logger.Info("Shutting down Raft cluster")

	// Cancel context to stop monitoring
	r.cancel()

	// Stop gRPC server and client
	r.grpcServer.Stop()
	r.grpcClient.Close()

	// Shutdown Raft
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("shutting down raft: %w", err)
	}

	// Close log store
	if r.closeStore != nil {
		if err := r.closeStore(); err != nil {
			return fmt.Errorf("closing log store: %w", err)
		}
	}

	return nil
}

func (r *Cluster) GetRaft() *raft.Raft {
	return r.raft
}

func (r *Cluster) GetGRPCClient() service.GRPCClient {
	return r.grpcClient
}

// GetDefaultLedger returns the default ledger service
func (r *Cluster) GetDefaultLedger() *service.DefaultLedger {
	return r.defaultLedger
}

// Snapshot forces a snapshot of the Raft cluster
func (r *Cluster) Snapshot() error {
	future := r.raft.Snapshot()
	if err := future.Error(); err != nil {
		return fmt.Errorf("creating snapshot: %w", err)
	}
	r.logger.Info("Snapshot created successfully")
	return nil
}

// raftLogger adapts zap.Logger to raft.Logger interface
type raftLogger struct {
	logger *zap.Logger
}

func (l *raftLogger) Write(p []byte) (n int, err error) {
	l.logger.Info(string(p[:len(p)-1])) // Remove trailing newline
	return len(p), nil
}

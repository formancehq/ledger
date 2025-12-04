package application

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// Application manages the lifecycle of the application components
type Application struct {
	config        *config.Config
	logger        *zap.Logger
	cluster       *raft.Cluster
	httpServer    *http.Server
	ledgerService service.Ledger
}

// New creates a new application instance
func New(cfg *config.Config, logger *zap.Logger) *Application {
	return &Application{
		config: cfg,
		logger: logger,
	}
}

// Start starts all application components
func (a *Application) Start(ctx context.Context) error {
	// Create Raft cluster
	cluster, err := raft.NewRaftCluster(ctx, a.config, a.logger)
	if err != nil {
		return fmt.Errorf("creating raft cluster: %w", err)
	}
	a.cluster = cluster

	// Create store and ledger services
	store := service.NewMemoryStore()
	defaultLedger := service.NewDefaultLedger(store)

	// Create routed ledger that will route to leader
	routedLedger := service.NewRoutedLedger(
		cluster,
		a.config.NodeID,
		defaultLedger,
		a.logger,
	)
	a.ledgerService = routedLedger

	// Create HTTP server
	httpServer := http.NewServer(a.config.HTTPPort, a.logger, routedLedger)
	a.httpServer = httpServer

	// Start Raft cluster
	if err := a.cluster.Start(); err != nil {
		return fmt.Errorf("starting raft cluster: %w", err)
	}

	a.logger.Info("Raft cluster started successfully")

	// Start HTTP server
	go func() {
		if err := a.httpServer.Start(ctx); err != nil {
			a.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// Shutdown gracefully shuts down all application components
func (a *Application) Shutdown() error {
	a.logger.Info("Shutting down application...")

	// Shutdown Raft cluster
	if a.cluster != nil {
		if err := a.cluster.Shutdown(); err != nil {
			return fmt.Errorf("shutting down raft cluster: %w", err)
		}
	}

	// Shutdown HTTP server
	if a.httpServer != nil {
		if err := a.httpServer.Stop(); err != nil {
			return fmt.Errorf("shutting down HTTP server: %w", err)
		}
	}

	return nil
}

// GetLedgerService returns the ledger service
func (a *Application) GetLedgerService() service.Ledger {
	return a.ledgerService
}

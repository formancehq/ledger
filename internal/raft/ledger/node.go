package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// logStoreFactory is a function that creates a LogStore from a JSON config
type logStoreFactory func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, extraDataDir string) (service.LogStore, error)

// logStoreFactories maps driver names to their factory functions
var logStoreFactories = map[string]logStoreFactory{
	"sqlite": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, extraDataDir string) (service.LogStore, error) {
		// SQLite DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite driver
		// Create database file path: extraDataDir/ledger-{id}.db
		dbFileName := fmt.Sprintf("ledger-%d.db", ledgerID)
		dbPath := filepath.Join(extraDataDir, dbFileName)

		// Ensure the directory exists
		if err := os.MkdirAll(extraDataDir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory for sqlite database: %w", err)
		}

		dsn := fmt.Sprintf("file:%s?mode=rwc", dbPath)
		return service.NewSQLiteLogStore(ctx, dsn, logger)
	},
}

// CreateLogStore creates a LogStore based on the ledger driver and config
func CreateLogStore(ctx context.Context, driver string, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, extraDataDir string) (service.LogStore, error) {
	factory, exists := logStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported ledger driver for log store: %s", driver)
	}

	store, err := factory(ctx, configJSON, logger, ledgerName, ledgerID, extraDataDir)
	if err != nil {
		return nil, fmt.Errorf("creating %s log store for ledger %s: %w", driver, ledgerName, err)
	}

	return store, nil
}

// Node represents a Raft group for a specific ledger
type Node struct {
	*raft.Node[ledger.LedgerState, *FSM]
	config        raft.NodeConfig
	logger        logging.Logger
	defaultLedger *service.DefaultLedger
	ledgerInfo    ledger.LedgerInfo
	logStore      service.LogStore // Underlying log store for direct access
}

func (node *Node) GetAllLogs(ctx context.Context, from uint64, to uint64) (service.Cursor[ledger.Log], error) {
	return node.logStore.GetAllLogs(ctx, from, to)
}

// NewNode creates a new Raft group for a ledger
func NewNode(
	ctx context.Context,
	ledgerInfo ledger.LedgerInfo,
	transport raft.NodeTransport,
	cfg raft.NodeConfig,
	logger logging.Logger,
	extraDataDir string,
	recoveryLogReader func(uint64) service.LogReader,
) (*Node, error) {

	// Create Raft storage for this ledger
	storage, err := raft.NewWALStorage(cfg.DataDir, logger.WithFields(map[string]any{"ledger": ledgerInfo.Name}))
	if err != nil {
		return nil, fmt.Errorf("creating storage for ledger %s: %w", ledgerInfo.Name, err)
	}

	// Create application log store for this ledger based on ledger driver
	appLogStore, err := CreateLogStore(ctx, ledgerInfo.Driver, ledgerInfo.Config, logger, ledgerInfo.Name, ledgerInfo.ID, extraDataDir)
	if err != nil {
		return nil, err
	}

	// Create ledger FSM for managing the ledger
	// recoveryLogReader is used for catching up logs from leader via gRPC
	ledgerFSM := newFSM(logger, appLogStore, recoveryLogReader, ledgerInfo)

	ret := &Node{
		config:     cfg,
		logger:     logger,
		ledgerInfo: ledgerInfo,
		logStore:   appLogStore,
	}

	// Create locked volumes store
	lockedVolumesStore := service.NewDefaultLockedBalancesStore(appLogStore)

	// Create ledger service for this ledger (will use stores for balance checking and log writing)
	ret.defaultLedger = service.NewDefaultLedger(ret, lockedVolumesStore, appLogStore, logger)

	logger.Infof("Creating Raft node for ledger %s", ledgerInfo.Name)
	ret.Node, err = raft.NewNode(cfg, storage, transport, ledgerFSM, logger)
	if err != nil {
		return nil, fmt.Errorf("creating Raft node for ledger %s: %w", ledgerInfo.Name, err)
	}
	logger.Infof("Raft node for ledger %s created", ledgerInfo.Name)

	return ret, nil
}

// InsertLogs writes logs via Raft (implements LogWriter)
func (node *Node) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// For each log, create a command to insert it via Raft
	for _, log := range logs {
		// Create a command to insert the log
		cmd, err := NewInsertLogCommand(log)
		if err != nil {
			return fmt.Errorf("creating insert log command: %w", err)
		}

		// Apply the command via Raft (waits for application)
		_, _, err = node.Apply(cmd, 5*time.Second)
		if err != nil {
			return fmt.Errorf("applying insert log command via etcdraft: %w", err)
		}

		node.logger.WithFields(map[string]any{"commandID": cmd.ID}).Debugf("Log inserted via ledger Raft")
	}

	return nil
}

// GetLastSequenceID returns the highest sequence number from the underlying log store (implements LogWriter)
func (node *Node) GetLastSequenceID(ctx context.Context) (uint64, error) {
	return node.logStore.GetLastSequenceID(ctx)
}

func (node *Node) CreateTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[service.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	return node.defaultLedger.CreateTransaction(ctx, ledgerName, parameters)
}

func (node *Node) RevertTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[service.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return node.defaultLedger.RevertTransaction(ctx, ledgerName, parameters)
}

func (node *Node) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.SaveTransactionMetadata]) (*ledger.Log, error) {
	return node.defaultLedger.SaveTransactionMetadata(ctx, ledgerName, parameters)
}

func (node *Node) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.SaveAccountMetadata]) (*ledger.Log, error) {
	return node.defaultLedger.SaveAccountMetadata(ctx, ledgerName, parameters)
}

func (node *Node) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.DeleteTransactionMetadata]) (*ledger.Log, error) {
	return node.defaultLedger.DeleteTransactionMetadata(ctx, ledgerName, parameters)
}

func (node *Node) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.DeleteAccountMetadata]) (*ledger.Log, error) {
	return node.defaultLedger.DeleteAccountMetadata(ctx, ledgerName, parameters)
}

func (node *Node) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return node.defaultLedger.Import(ctx, ledgerName, stream)
}

func (node *Node) Export(ctx context.Context, ledgerName string, w service.ExportWriter) error {
	return node.defaultLedger.Export(ctx, ledgerName, w)
}

func (node *Node) Info() ledger.LedgerInfo {
	return node.ledgerInfo
}

var _ service.Ledger = (*Node)(nil)

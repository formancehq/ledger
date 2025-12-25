package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.opentelemetry.io/otel/metric"
)

// logStoreFactory is a function that creates a LogStore from a JSON config
type logStoreFactory func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, dataDir string) (service.LogStore, error)

// logStoreFactories maps driver names to their factory functions
var logStoreFactories = map[string]logStoreFactory{
	"sqlite-mattn": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, dataDir string) (service.LogStore, error) {
		// SQLite DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Mattn driver
		// Create database file path: dataDir/ledger-{id}.db
		dbFileName := fmt.Sprintf("ledger-%d.db", ledgerID)
		dbPath := filepath.Join(dataDir, dbFileName)

		// Ensure the directory exists
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory for sqlite-mattn database: %w", err)
		}

		// Use sqlite3 driver (github.com/mattn/go-sqlite3)
		dsn := dbPath
		return service.NewSQLiteMattnLogStore(ctx, dsn, logger)
	},
	"sqlite-modern": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, dataDir string) (service.LogStore, error) {
		// SQLite Modern DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Modern driver
		// Create database file path: dataDir/ledger-{id}.db
		dbFileName := fmt.Sprintf("ledger-%d.db", ledgerID)
		dbPath := filepath.Join(dataDir, dbFileName)

		// Ensure the directory exists
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory for sqlite-modern database: %w", err)
		}

		// Use sqlite driver (modernc.org/sqlite)
		dsn := fmt.Sprintf("file:%s", dbPath)
		return service.NewSQLiteModernLogStore(ctx, dsn, logger)
	},
}

// CreateLogStore creates a LogStore based on the ledger driver and config
func CreateLogStore(ctx context.Context, driver string, configJSON json.RawMessage, logger logging.Logger, ledgerName string, ledgerID uint64, dataDir string) (service.LogStore, error) {
	factory, exists := logStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported ledger driver for log store: %s", driver)
	}

	store, err := factory(ctx, configJSON, logger, ledgerName, ledgerID, dataDir)
	if err != nil {
		return nil, fmt.Errorf("creating %s log store for ledger %s: %w", driver, ledgerName, err)
	}

	return store, nil
}

// Node represents a Raft group for a specific ledger
type Node struct {
	*raft.Node[ledgerpb.LedgerState, *FSM]
	config        raft.NodeConfig
	logger        logging.Logger
	defaultLedger *service.DefaultLedger
	ledgerInfo    *ledgerpb.LedgerInfo
	logStore      service.LogStore // Underlying log store for direct access
}

func (node *Node) GetAllLogs(ctx context.Context, from uint64, to uint64) (service.Cursor[*ledgerpb.Log], error) {
	return node.logStore.GetAllLogs(ctx, from, to)
}

// NewNode creates a new Raft group for a ledger
func NewNode(
	ctx context.Context,
	ledgerInfo *ledgerpb.LedgerInfo,
	transport raft.NodeTransport,
	cfg raft.NodeConfig,
	logger logging.Logger,
	recoveryLogReader func(uint64) service.LogReader,
	meter metric.Meter,
) (*Node, error) {

	// Create Raft storage for this ledger
	storage, err := raft.NewWALStorage(cfg.DataDir, logger.WithFields(map[string]any{"ledger": ledgerInfo.GetName()}))
	if err != nil {
		return nil, fmt.Errorf("creating storage for ledger %s: %w", ledgerInfo.GetName(), err)
	}

	// Convert Config from *structpb.Struct to json.RawMessage
	var configJSON json.RawMessage
	if ledgerInfo.Config != nil {
		configMap := ledgerInfo.Config.AsMap()
		var err error
		configJSON, err = json.Marshal(configMap)
		if err != nil {
			return nil, fmt.Errorf("marshaling ledger config: %w", err)
		}
	}

	// Create application log store for this ledger based on ledger driver
	// Use the same dataDir as the Raft storage (ledger data directory)
	appLogStore, err := CreateLogStore(ctx, ledgerInfo.Driver, configJSON, logger, ledgerInfo.GetName(), ledgerInfo.GetId(), cfg.DataDir)
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
	ret.Node, err = raft.NewNode(cfg, storage, transport, ledgerFSM, logger, meter)
	if err != nil {
		return nil, fmt.Errorf("creating Raft node for ledger %s: %w", ledgerInfo.Name, err)
	}
	logger.Infof("Raft node for ledger %s created", ledgerInfo.Name)

	return ret, nil
}

// InsertLogs writes logs via Raft (implements LogWriter)
func (node *Node) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
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

// GetLastLogID returns the highest log id from the underlying log store (implements LogWriter)
func (node *Node) GetLastLogID(ctx context.Context) (uint64, error) {
	return node.logStore.GetLastLogID(ctx)
}

func (node *Node) CreateTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.CreatedTransaction, error) {
	return node.defaultLedger.CreateTransaction(ctx, ledgerName, parameters)
}

func (node *Node) RevertTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.RevertedTransaction, error) {
	return node.defaultLedger.RevertTransaction(ctx, ledgerName, parameters)
}

func (node *Node) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveTransactionMetadata(ctx, ledgerName, parameters)
}

func (node *Node) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveAccountMetadata(ctx, ledgerName, parameters)
}

func (node *Node) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteTransactionMetadata(ctx, ledgerName, parameters)
}

func (node *Node) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteAccountMetadata(ctx, ledgerName, parameters)
}

func (node *Node) Import(ctx context.Context, ledgerName string, stream chan *ledgerpb.Log) error {
	return node.defaultLedger.Import(ctx, ledgerName, stream)
}

func (node *Node) Export(ctx context.Context, ledgerName string, w service.ExportWriter) error {
	return node.defaultLedger.Export(ctx, ledgerName, w)
}

func (node *Node) Info() *ledgerpb.LedgerInfo {
	return node.ledgerInfo
}

var _ service.Ledger = (*Node)(nil)

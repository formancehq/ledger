package bucket

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// logStoreFactory is a function that creates a LogStore from a JSON config
type logStoreFactory func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, bucketID uint64, extraDataDir string) (service.LogStore, error)

// logStoreFactories maps driver names to their factory functions
var logStoreFactories = map[string]logStoreFactory{
	"sqlite": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, bucketID uint64, extraDataDir string) (service.LogStore, error) {
		// SQLite DSN is automatically generated based on bucket ID
		// Config is ignored for SQLite driver
		// Create database file path: extraDataDir/bucket-{id}.db
		dbFileName := fmt.Sprintf("bucket-%d.db", bucketID)
		dbPath := filepath.Join(extraDataDir, dbFileName)

		// Ensure the directory exists
		if err := os.MkdirAll(extraDataDir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory for sqlite database: %w", err)
		}

		dsn := fmt.Sprintf("file:%s?mode=rwc", dbPath)
		return service.NewSQLiteLogStore(ctx, dsn, logger)
	},
}

// CreateLogStore creates a LogStore based on the bucket driver and config
func CreateLogStore(ctx context.Context, driver string, configJSON json.RawMessage, logger logging.Logger, bucketName string, bucketID uint64, extraDataDir string) (service.LogStore, error) {
	factory, exists := logStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported bucket driver for log store: %s", driver)
	}

	store, err := factory(ctx, configJSON, logger, bucketName, bucketID, extraDataDir)
	if err != nil {
		return nil, fmt.Errorf("creating %s log store for bucket %s: %w", driver, bucketName, err)
	}

	return store, nil
}

// Node represents a Raft group for a specific bucket
type Node struct {
	*raft.Node[ledger.BucketState, *FSM]
	config        raft.NodeConfig
	logger        logging.Logger
	defaultLedger *service.DefaultLedger
	bucketInfo    ledger.BucketInfo
	logStore      service.LogStore // Underlying log store for direct access
}

func (node *Node) GetAllLogs(ctx context.Context, from uint64) (service.Cursor[ledger.Log], error) {
	return node.logStore.GetAllLogs(ctx, from)
}

// NewNode creates a new Raft group for a bucket
func NewNode(
	bucketInfo ledger.BucketInfo,
	transport raft.NodeTransport,
	cfg raft.NodeConfig,
	logger logging.Logger,
	extraDataDir string,
	logReader service.LogReader,
) (*Node, error) {

	// Create Raft storage for this bucket ret
	storage, err := raft.NewWALStorage(cfg.DataDir, logger.WithFields(map[string]any{"bucket": bucketInfo.Name}))
	if err != nil {
		return nil, fmt.Errorf("creating storage for bucket %s: %w", bucketInfo.Name, err)
	}

	// Create application log store for this bucket based on bucket driver
	appLogStore, err := CreateLogStore(context.Background(), bucketInfo.Driver, bucketInfo.Config, logger, bucketInfo.Name, bucketInfo.ID, extraDataDir)
	if err != nil {
		return nil, err
	}

	// Create bucket FSM for managing ledgers
	// logReader is used for catching up logs from leader via gRPC
	bucketFSM := newFSM(logger, appLogStore, logReader)

	ret := &Node{
		config:     cfg,
		logger:     logger,
		bucketInfo: bucketInfo,
		logStore:   appLogStore,
	}

	// Create locked volumes store
	lockedVolumesStore := service.NewDefaultLockedBalancesStore(appLogStore)

	// Create ledger service for this bucket (will use stores for balance checking and log writing)
	ret.defaultLedger = service.NewDefaultLedger(ret, lockedVolumesStore, appLogStore, logger)

	ret.Node, err = raft.NewNode(cfg, storage, transport, bucketFSM, logger)
	if err != nil {
		return nil, fmt.Errorf("creating Raft node for bucket %s: %w", bucketInfo.Name, err)
	}

	return ret, nil
}

// CreateLedger creates a new ledger in this bucket via a FSM command
func (node *Node) CreateLedger(_ context.Context, name string, metadata metadata.Metadata) (*ledger.LedgerInfo, error) {
	// Create the command
	cmd, err := NewCreateLedgerCommand(name, metadata)
	if err != nil {
		return nil, fmt.Errorf("creating create ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, ledgerInfo, err := node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("applying command via etcdraft: %w", err)
	}

	node.logger.Infof("Ledger created via bucket Raft")
	return ledgerInfo.(*ledger.LedgerInfo), nil
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

		node.logger.WithFields(map[string]any{"ledger": log.Ledger, "commandID": cmd.ID}).Debugf("Log inserted via bucket Raft")
	}

	return nil
}

// GetLastSequenceID returns the highest sequence number from the underlying log store (implements LogWriter)
func (node *Node) GetLastSequenceID(ctx context.Context) (uint64, error) {
	return node.logStore.GetLastSequenceID(ctx)
}

// GetLedger returns the ledger info for a given name in this bucket
func (node *Node) GetLedger(ctx context.Context, name string) (*ledger.LedgerInfo, error) {
	return node.Inner().GetLedger(name)
}

// GetLedgers returns all ledgers in this bucket
func (node *Node) GetLedgers(ctx context.Context) ([]ledger.LedgerInfo, error) {
	return node.Inner().GetAllLedgers(), nil
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

func (node *Node) Info() ledger.BucketInfo {
	return node.bucketInfo
}

var _ service.Ledger = (*Node)(nil)

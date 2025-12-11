package bucket

import (
	"context"
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

// Node represents a Raft group for a specific bucket
type Node struct {
	*raft.Node[*FSM]
	config        raft.NodeConfig
	logger        logging.Logger
	defaultLedger *service.DefaultLedger
	bucketInfo    ledger.BucketInfo
}

// NewNode creates a new Raft group for a bucket
func NewNode(
	bucketInfo ledger.BucketInfo,
	transport raft.NodeTransport,
	cfg raft.NodeConfig,
	logger logging.Logger,
) (*Node, error) {

	// Create Raft storage for this bucket ret
	storage, err := raft.NewWALStorage(cfg.DataDir, logger.WithFields(map[string]any{"bucket": bucketInfo.Name}))
	if err != nil {
		return nil, fmt.Errorf("creating storage for bucket %s: %w", bucketInfo.Name, err)
	}

	// Create application log store for this bucket based on bucket driver
	var appLogStore service.LogStore
	switch bucketInfo.Driver {
	case "sqlite":
		dsn, ok := bucketInfo.Config["dsn"].(string)
		if !ok || dsn == "" {
			return nil, fmt.Errorf("sqlite driver requires 'dsn' configuration for bucket %s", bucketInfo.Name)
		}
		sqliteStore, err := service.NewSQLiteLogStore(context.Background(), dsn, logger)
		if err != nil {
			return nil, fmt.Errorf("creating sqlite log store for bucket %s: %w", bucketInfo.Name, err)
		}
		appLogStore = sqliteStore
	case "postgres":
		dsn, ok := bucketInfo.Config["dsn"].(string)
		if !ok || dsn == "" {
			return nil, fmt.Errorf("postgres driver requires 'dsn' configuration for bucket %s", bucketInfo.Name)
		}
		postgresStore, err := service.NewPostgresLogStore(context.Background(), dsn, logger)
		if err != nil {
			return nil, fmt.Errorf("creating postgres log store for bucket %s: %w", bucketInfo.Name, err)
		}
		appLogStore = postgresStore
	case "clickhouse":
		dsn, ok := bucketInfo.Config["dsn"].(string)
		if !ok || dsn == "" {
			return nil, fmt.Errorf("clickhouse driver requires 'dsn' configuration for bucket %s", bucketInfo.Name)
		}
		clickhouseStore, err := service.NewClickHouseLogStore(context.Background(), dsn, logger)
		if err != nil {
			return nil, fmt.Errorf("creating clickhouse log store for bucket %s: %w", bucketInfo.Name, err)
		}
		appLogStore = clickhouseStore
	case "file":
		path, ok := bucketInfo.Config["path"].(string)
		if !ok || path == "" {
			return nil, fmt.Errorf("file driver requires 'path' configuration for bucket %s", bucketInfo.Name)
		}
		// Create logs directory within the bucket path
		logsPath := filepath.Join(path, "logs.jsonl")

		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, fmt.Errorf("creating logs directory for bucket %s: %w", bucketInfo.Name, err)
		}

		fileStore, err := service.NewFileLogStore(logsPath, logger)
		if err != nil {
			return nil, fmt.Errorf("creating file log store for bucket %s: %w", bucketInfo.Name, err)
		}
		appLogStore = fileStore
	default:
		return nil, fmt.Errorf("unsupported bucket driver for log store: %s", bucketInfo.Driver)
	}

	// Create bucket FSM for managing ledgers
	bucketFSM := newFSM(logger, appLogStore)

	ret := &Node{
		config:     cfg,
		logger:     logger,
		bucketInfo: bucketInfo,
	}

	// Create reconstructed volumes store
	reconstructedVolumesStore := service.NewReconstructedBalancesStore(appLogStore)

	consolidatedVolumesStore := service.NewConsolidatedBalancesStore(reconstructedVolumesStore, bucketFSM)

	// Create locked volumes store
	lockedVolumesStore := service.NewDefaultLockedBalancesStore(consolidatedVolumesStore)

	// Create ledger service for this bucket (will use stores for balance checking and log writing)
	ret.defaultLedger = service.NewDefaultLedger(ret, lockedVolumesStore, struct {
		service.LogReader
	}{
		LogReader: service.NewConsolidatedLogReader(appLogStore, ret),
	}, logger)

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

// GetLedger returns the ledger info for a given name in this bucket
func (node *Node) GetLedger(ctx context.Context, name string) (*ledger.LedgerInfo, error) {
	return node.Inner().GetLedger(name)
}

// GetLedgers returns all ledgers in this bucket
func (node *Node) GetLedgers(ctx context.Context) ([]ledger.LedgerInfo, error) {
	return node.Inner().GetAllLedgers(), nil
}

// GetInMemoryDiffBalances returns the in-memory balance diff for a ledger (implements HotDiffBalancesProvider)
func (node *Node) GetInMemoryDiffBalances(ledgerName string) ledger.Balances {
	return node.Inner().GetInMemoryDiffBalances(ledgerName)
}

// GetInMemoryLogs returns the in-memory logs for a ledger
func (node *Node) GetInMemoryLogs(ledgerName string) []ledger.Log {
	return node.Inner().GetInMemoryLogs(ledgerName)
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

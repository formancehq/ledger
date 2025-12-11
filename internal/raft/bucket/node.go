package bucket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// logStoreFactory is a function that creates a LogStore from a JSON config
type logStoreFactory func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, extraDataDir string) (service.LogStore, error)

// resolvePath resolves a path against extraDataDir, treating absolute paths as relative to extraDataDir
func resolvePath(path string, extraDataDir string) string {
	// Remove leading slash from absolute paths to treat them as relative to extraDataDir
	if filepath.IsAbs(path) {
		// On Windows, remove the drive letter prefix (e.g., "C:/" -> "")
		// On Unix, remove the leading "/"
		path = strings.TrimPrefix(path, filepath.VolumeName(path))
		path = strings.TrimPrefix(path, "/")
	}
	return filepath.Join(extraDataDir, path)
}

// resolveSQLiteDSN resolves relative paths in SQLite DSN against extraDataDir
// It ensures the DSN is file-based and creates the directory if needed
func resolveSQLiteDSN(dsn string, extraDataDir string) (string, error) {
	// SQLite DSN format: "file:path?params" or "file:./path?params"
	// Parse the URL to extract path and query
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid sqlite DSN format: %w", err)
	}

	// Ensure it's a file: scheme
	if parsedURL.Scheme != "file" {
		return "", fmt.Errorf("sqlite DSN must use 'file:' scheme")
	}

	// Reject empty paths
	if parsedURL.Path == "" {
		return "", fmt.Errorf("sqlite DSN must specify a file path")
	}

	// Resolve relative paths
	resolvedPath := resolvePath(parsedURL.Path, extraDataDir)

	// Ensure the parent directory exists (resolvedPath is a file, not a directory)
	dir := filepath.Dir(resolvedPath)
	if dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("creating directory for sqlite database file: %w", err)
		}
	}

	// Reconstruct the DSN with resolved path
	parsedURL.Path = resolvedPath
	return parsedURL.String(), nil
}

// logStoreFactories maps driver names to their factory functions
var logStoreFactories = map[string]logStoreFactory{
	"sqlite": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, extraDataDir string) (service.LogStore, error) {
		var config service.SQLiteConfig
		if len(configJSON) > 0 {
			if err := json.Unmarshal(configJSON, &config); err != nil {
				return nil, fmt.Errorf("unmarshaling sqlite config: %w", err)
			}
		}
		if config.DSN == "" {
			return nil, fmt.Errorf("sqlite driver requires 'dsn' configuration for bucket %s", bucketName)
		}
		// Resolve relative paths in DSN and ensure it's file-based
		resolvedDSN, err := resolveSQLiteDSN(config.DSN, extraDataDir)
		if err != nil {
			return nil, fmt.Errorf("resolving sqlite DSN for bucket %s: %w", bucketName, err)
		}
		return service.NewSQLiteLogStore(ctx, resolvedDSN, logger)
	},
	"postgres": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, extraDataDir string) (service.LogStore, error) {
		var config service.PostgresConfig
		if len(configJSON) > 0 {
			if err := json.Unmarshal(configJSON, &config); err != nil {
				return nil, fmt.Errorf("unmarshaling postgres config: %w", err)
			}
		}
		if config.DSN == "" {
			return nil, fmt.Errorf("postgres driver requires 'dsn' configuration for bucket %s", bucketName)
		}
		// Postgres DSNs are typically connection strings, not file paths, so we don't resolve them
		return service.NewPostgresLogStore(ctx, config.DSN, logger)
	},
	"clickhouse": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, extraDataDir string) (service.LogStore, error) {
		var config service.ClickHouseConfig
		if len(configJSON) > 0 {
			if err := json.Unmarshal(configJSON, &config); err != nil {
				return nil, fmt.Errorf("unmarshaling clickhouse config: %w", err)
			}
		}
		if config.DSN == "" {
			return nil, fmt.Errorf("clickhouse driver requires 'dsn' configuration for bucket %s", bucketName)
		}
		// ClickHouse DSNs are typically connection strings, not file paths, so we don't resolve them
		return service.NewClickHouseLogStore(ctx, config.DSN, logger)
	},
	"file": func(ctx context.Context, configJSON json.RawMessage, logger logging.Logger, bucketName string, extraDataDir string) (service.LogStore, error) {
		var config service.FileConfig
		if len(configJSON) > 0 {
			if err := json.Unmarshal(configJSON, &config); err != nil {
				return nil, fmt.Errorf("unmarshaling file config: %w", err)
			}
		}
		if config.Path == "" {
			return nil, fmt.Errorf("file driver requires 'path' configuration for bucket %s", bucketName)
		}
		// Resolve relative paths
		resolvedPath := resolvePath(config.Path, extraDataDir)
		// Create logs directory within the bucket path
		logsPath := filepath.Join(resolvedPath, "logs.jsonl")

		if err := os.MkdirAll(resolvedPath, 0755); err != nil {
			return nil, fmt.Errorf("creating logs directory for bucket %s: %w", bucketName, err)
		}

		return service.NewFileLogStore(logsPath, logger)
	},
}

// createLogStore creates a LogStore based on the bucket driver and config
func createLogStore(ctx context.Context, driver string, configJSON json.RawMessage, logger logging.Logger, bucketName string, extraDataDir string) (service.LogStore, error) {
	factory, exists := logStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported bucket driver for log store: %s", driver)
	}

	store, err := factory(ctx, configJSON, logger, bucketName, extraDataDir)
	if err != nil {
		return nil, fmt.Errorf("creating %s log store for bucket %s: %w", driver, bucketName, err)
	}

	return store, nil
}

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
	extraDataDir string,
) (*Node, error) {

	// Create Raft storage for this bucket ret
	storage, err := raft.NewWALStorage(cfg.DataDir, logger.WithFields(map[string]any{"bucket": bucketInfo.Name}))
	if err != nil {
		return nil, fmt.Errorf("creating storage for bucket %s: %w", bucketInfo.Name, err)
	}

	// Create application log store for this bucket based on bucket driver
	appLogStore, err := createLogStore(context.Background(), bucketInfo.Driver, bucketInfo.Config, logger, bucketInfo.Name, extraDataDir)
	if err != nil {
		return nil, err
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

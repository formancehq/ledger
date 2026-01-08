package ledger

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

// logStoreFactory is a function that creates a LogStore from a JSON config
type logStoreFactory func(
	ctx context.Context,
	configJSON []byte,
	logger logging.Logger,
	ledgerName string,
	ledgerID uint64,
	dataDir string,
	meterProvider metric.MeterProvider,
) (LogStoreWithMetrics, error)

// runtimeStoreFactory is a function that creates a RuntimeStore from a JSON config
type runtimeStoreFactory func(
	ctx context.Context,
	configJSON []byte,
	logger logging.Logger,
	ledgerName string,
	ledgerID uint64,
	dataDir string,
	meterProvider metric.MeterProvider,
) (RuntimeStoreWithMetrics, error)

// logStoreFactories maps driver names to their factory functions
var logStoreFactories = map[string]logStoreFactory{
	"sqlite-mattn": func(
		ctx context.Context,
		configJSON []byte,
		logger logging.Logger,
		ledgerName string,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (LogStoreWithMetrics, error) {
		// SQLite DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Mattn driver
		// Create database file path: dataDir/ledger-{id}-logs.db
		logsDBFileName := fmt.Sprintf("ledger-%d-logs.db", ledgerID)
		logsDBPath := filepath.Join(dataDir, logsDBFileName)

		// Create log store (stores logs)
		logStore, err := service.NewSQLiteMattnLogStore(logsDBPath, logger)
		if err != nil {
			return nil, fmt.Errorf("creating log store: %w", err)
		}

		return logStore, nil
	},
	"sqlite-modern": func(
		ctx context.Context,
		configJSON []byte,
		logger logging.Logger,
		ledgerName string,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (LogStoreWithMetrics, error) {
		// SQLite Modern DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Modern driver
		// Create database file path: dataDir/ledger-{id}-logs.db
		logsDBFileName := fmt.Sprintf("ledger-%d-logs.db", ledgerID)
		logsDBPath := filepath.Join(dataDir, logsDBFileName)

		// Use sqlite driver (modernc.org/sqlite)
		logsDSN := fmt.Sprintf("file:%s", logsDBPath)

		// Create log store (stores logs)
		logStore, err := service.NewSQLiteModernLogStore(logsDSN, logger)
		if err != nil {
			return nil, fmt.Errorf("creating log store: %w", err)
		}

		return logStore, nil
	},
	"pebble": func(
		ctx context.Context,
		configJSON []byte,
		logger logging.Logger,
		ledgerName string,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (LogStoreWithMetrics, error) {
		// Pebble data directories are automatically generated based on ledger ID
		// Config is ignored for Pebble driver

		logStoreMeter := meterProvider.Meter("peeble.log_store", metric.WithInstrumentationAttributes(
			attribute.Int("ledger-id", int(ledgerID)),
		))

		// Create log store (stores logs)
		logStore, err := service.NewPebbleLogStore(dataDir, logger, logStoreMeter)
		if err != nil {
			return nil, fmt.Errorf("creating log store: %w", err)
		}

		return logStore, nil
	},
}

// runtimeStoreFactories maps driver names to their factory functions
var runtimeStoreFactories = map[string]runtimeStoreFactory{
	"sqlite-mattn": func(
		ctx context.Context,
		configJSON []byte,
		logger logging.Logger,
		ledgerName string,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (RuntimeStoreWithMetrics, error) {
		// SQLite DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Mattn driver
		// Create database file path: dataDir/ledger-{id}-runtime.db
		runtimeDBFileName := fmt.Sprintf("ledger-%d-runtime.db", ledgerID)
		runtimeDBPath := filepath.Join(dataDir, runtimeDBFileName)

		// Create runtime store (stores balances and metadata)
		runtimeStore, err := service.NewSQLiteMattnRuntimeStore(ctx, runtimeDBPath, logger)
		if err != nil {
			return nil, fmt.Errorf("creating runtime store: %w", err)
		}

		return runtimeStore, nil
	},
	"sqlite-modern": func(
		ctx context.Context,
		configJSON []byte,
		logger logging.Logger,
		ledgerName string,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (RuntimeStoreWithMetrics, error) {
		// SQLite Modern DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Modern driver
		// Create database file path: dataDir/ledger-{id}-runtime.db
		runtimeDBFileName := fmt.Sprintf("ledger-%d-runtime.db", ledgerID)
		runtimeDBPath := filepath.Join(dataDir, runtimeDBFileName)

		// Use sqlite driver (modernc.org/sqlite)
		runtimeDSN := fmt.Sprintf("file:%s", runtimeDBPath)

		// Create runtime store (stores balances and metadata)
		runtimeStore, err := service.NewSQLiteModernRuntimeStore(ctx, runtimeDSN, logger)
		if err != nil {
			return nil, fmt.Errorf("creating runtime store: %w", err)
		}

		return runtimeStore, nil
	},
	"pebble": func(
		ctx context.Context,
		configJSON []byte,
		logger logging.Logger,
		ledgerName string,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (RuntimeStoreWithMetrics, error) {
		// Pebble data directories are automatically generated based on ledger ID
		// Config is ignored for Pebble driver

		runtimeStoreMeter := meterProvider.Meter("peeble.runtime_store", metric.WithInstrumentationAttributes(
			attribute.Int("ledger-id", int(ledgerID)),
		))

		// Create runtime store (stores balances and metadata)
		runtimeStore, err := service.NewPebbleRuntimeStore(dataDir, logger, runtimeStoreMeter)
		if err != nil {
			return nil, fmt.Errorf("creating runtime store: %w", err)
		}

		return runtimeStore, nil
	},
}

// CreateLogStore creates a LogStore based on the ledger driver and config
func CreateLogStore(
	ctx context.Context,
	driver string,
	configJSON []byte,
	logger logging.Logger,
	ledgerName string,
	ledgerID uint64,
	dataDir string,
	meterProvider metric.MeterProvider,
) (LogStoreWithMetrics, error) {

	logStoreFactory, exists := logStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported ledger driver for log store: %s", driver)
	}

	logStore, err := logStoreFactory(ctx, configJSON, logger, ledgerName, ledgerID, dataDir, meterProvider)
	if err != nil {
		return nil, fmt.Errorf("creating %s log store for ledger %s: %w", driver, ledgerName, err)
	}

	return logStore, nil
}

// CreateRuntimeStore creates a RuntimeStore based on the ledger driver and config
func CreateRuntimeStore(
	ctx context.Context,
	driver string,
	configJSON []byte,
	logger logging.Logger,
	ledgerName string,
	ledgerID uint64,
	dataDir string,
	meterProvider metric.MeterProvider,
) (RuntimeStoreWithMetrics, error) {

	runtimeStoreFactory, exists := runtimeStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported ledger driver for runtime store: %s", driver)
	}

	runtimeStore, err := runtimeStoreFactory(ctx, configJSON, logger, ledgerName, ledgerID, dataDir, meterProvider)
	if err != nil {
		return nil, fmt.Errorf("creating %s runtime store for ledger %s: %w", driver, ledgerName, err)
	}

	return runtimeStore, nil
}

// Node represents a Raft group for a specific ledger
type Node struct {
	*raft.Node[ledgerpb.LedgerState, *FSM]
	config        raft.NodeConfig
	logger        logging.Logger
	defaultLedger *service.DefaultLedger
	ledgerInfo    *ledgerpb.LedgerInfo
	logReader     service.LogReader
	runtimeStore  service.RuntimeStore
	logStore      service.LogStore
}

func (node *Node) GetAllLogs(ctx context.Context, from uint64, to uint64) (service.Cursor[*ledgerpb.Log], error) {
	return node.logReader.GetAllLogs(ctx, from, to)
}

// NewNode creates a new Raft group for a ledger
func NewNode(
	ctx context.Context,
	ledgerInfo *ledgerpb.LedgerInfo,
	transport raft.NodeTransport,
	cfg raft.NodeConfig,
	logger logging.Logger,
	recoveryLogReader func(uint64) service.LogReader,
	meterProvider metric.MeterProvider,
) (*Node, error) {

	// Create Raft storage for this ledger
	storage, err := raft.NewWALStorage(cfg.DataDir, logger.WithFields(map[string]any{"ledger": ledgerInfo.GetName()}))
	if err != nil {
		return nil, fmt.Errorf("creating storage for ledger %s: %w", ledgerInfo.GetName(), err)
	}

	// Convert LogStoreConfig from *structpb.Struct to []byte
	var logStoreConfigJSON []byte
	if ledgerInfo.LogStoreConfig != nil {
		configMap := ledgerInfo.LogStoreConfig.AsMap()
		var err error
		logStoreConfigJSON, err = json.Marshal(configMap)
		if err != nil {
			return nil, fmt.Errorf("marshaling log store config: %w", err)
		}
	}

	// Convert RuntimeStoreConfig from *structpb.Struct to []byte
	var runtimeStoreConfigJSON []byte
	if ledgerInfo.RuntimeStoreConfig != nil {
		configMap := ledgerInfo.RuntimeStoreConfig.AsMap()
		var err error
		runtimeStoreConfigJSON, err = json.Marshal(configMap)
		if err != nil {
			return nil, fmt.Errorf("marshaling runtime store config: %w", err)
		}
	}

	// Ensure the directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating directory for ledger stores: %w", err)
	}

	// Create application log store for this ledger based on ledger driver
	// Use the same dataDir as the Raft storage (ledger data directory)
	logStore, err := CreateLogStore(
		ctx,
		ledgerInfo.GetLogStoreDriver(),
		logStoreConfigJSON,
		logger,
		ledgerInfo.GetName(),
		ledgerInfo.GetId(),
		cfg.DataDir,
		meterProvider,
	)
	if err != nil {
		return nil, err
	}

	// Create runtime store for this ledger based on ledger driver
	runtimeStore, err := CreateRuntimeStore(
		ctx,
		ledgerInfo.GetRuntimeStoreDriver(),
		runtimeStoreConfigJSON,
		logger,
		ledgerInfo.GetName(),
		ledgerInfo.GetId(),
		cfg.DataDir,
		meterProvider,
	)
	if err != nil {
		return nil, err
	}

	state := &ledgerpb.LedgerState{}
	snapshot, _ := storage.Snapshot()
	if snapshot.Metadata.Index > 0 {
		if err := proto.Unmarshal(snapshot.Data, state); err != nil {
			return nil, fmt.Errorf("unmarshaling snapshot data: %w", err)
		}
	} else {
		state = &ledgerpb.LedgerState{
			LedgerInfo: ledgerInfo,
		}
	}

	// Create ledger FSM for managing the ledger
	// recoveryLogReader is used for catching up logs from leader via gRPC
	ledgerFSM := newFSM(
		logger,
		logStore,
		runtimeStore,
		recoveryLogReader,
		state,
	)

	ret := &Node{
		config:       cfg,
		logger:       logger,
		ledgerInfo:   ledgerInfo,
		logReader:    logStore,
		runtimeStore: runtimeStore,
		logStore:     logStore,
	}

	// Create locked volumes store
	lockedVolumesStore := service.NewDefaultLockedBalancesStore(runtimeStore)

	// Create ledger service for this ledger (will use stores for balance checking and log writing)
	ret.defaultLedger = service.NewDefaultLedger(
		ret,
		lockedVolumesStore,
		logStore,
		runtimeStore,
		logger,
	)

	nodeMeter := meterProvider.Meter("raft.node.ledger", metric.WithInstrumentationAttributes(
		attribute.Int("id", int(ledgerInfo.GetId())),
		attribute.String("name", ledgerInfo.Name),
	))

	logger.Infof("Creating Raft node for ledger %s", ledgerInfo.Name)
	ret.Node, err = raft.NewNode(cfg, storage, transport, ledgerFSM, logger, nodeMeter)
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
		_, logID, err := node.Apply(cmd, 5*time.Second)
		if err != nil {
			return fmt.Errorf("applying insert log command via etcdraft: %w", err)
		}

		log.Id = logID.(uint64)

		node.logger.
			WithFields(map[string]any{"commandID": cmd.ID}).
			Debugf("Log inserted via ledger Raft")
	}

	return nil
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

// CloseStores closes the runtime and log stores
func (node *Node) CloseStores() error {
	var errs []error

	if node.runtimeStore != nil {
		if closer, ok := node.runtimeStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing runtime store: %w", err))
			}
		}
	}

	if node.logStore != nil {
		if closer, ok := node.logStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing log store: %w", err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing stores: %v", errs)
	}

	return nil
}

// DeleteStoreFiles deletes the database files and data directory for this ledger
func (node *Node) DeleteStoreFiles() error {
	// Close stores first to ensure all connections are closed
	if err := node.CloseStores(); err != nil {
		node.logger.WithFields(map[string]any{"error": err}).Errorf("Error closing stores before deletion")
	}

	// Delete the entire ledger data directory
	// This includes:
	// - ledger-{id}-logs.db
	// - ledger-{id}-runtime.db
	// - WAL files
	// - Other Raft storage files
	if err := os.RemoveAll(node.config.DataDir); err != nil {
		return fmt.Errorf("deleting ledger data directory %s: %w", node.config.DataDir, err)
	}

	node.logger.WithFields(map[string]any{"dataDir": node.config.DataDir}).Infof("Ledger store files deleted")
	return nil
}

var _ service.Ledger = (*Node)(nil)

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

// runtimeStoreFactory is a function that creates a RuntimeStore from a JSON config
type runtimeStoreFactory func(
	ctx context.Context,
	runtimeConfigJSON []byte,
	logger logging.Logger,
	ledgerID uint64,
	dataDir string,
	meterProvider metric.MeterProvider,
) (RuntimeStoreWithMetrics, error)

// runtimeStoreFactories maps driver names to their factory functions
var runtimeStoreFactories = map[string]runtimeStoreFactory{
	"sqlite-mattn": func(
		ctx context.Context,
		runtimeConfigJSON []byte,
		logger logging.Logger,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (RuntimeStoreWithMetrics, error) {
		// SQLite DSN is automatically generated based on ledger ID
		// Config is ignored for SQLite Mattn driver
		// Create database file path: dataDir/ledger-{id}-runtime.db
		runtimeDBFileName := fmt.Sprintf("ledger-%d-runtime.db", ledgerID)
		runtimeDBPath := filepath.Join(dataDir, runtimeDBFileName)

		// Create runtime store (stores balances, metadata, and logs)
		runtimeStore, err := service.NewSQLiteMattnRuntimeStore(ctx, runtimeDBPath, logger)
		if err != nil {
			return nil, fmt.Errorf("creating runtime store: %w", err)
		}

		return runtimeStore, nil
	},
	"sqlite-modern": func(
		ctx context.Context,
		runtimeConfigJSON []byte,
		logger logging.Logger,
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

		// Create runtime store (stores balances, metadata, and logs)
		runtimeStore, err := service.NewSQLiteModernRuntimeStore(runtimeDSN, logger)
		if err != nil {
			return nil, fmt.Errorf("creating runtime store: %w", err)
		}

		return runtimeStore, nil
	},
	"pebble": func(
		ctx context.Context,
		runtimeConfigJSON []byte,
		logger logging.Logger,
		ledgerID uint64,
		dataDir string,
		meterProvider metric.MeterProvider,
	) (RuntimeStoreWithMetrics, error) {
		// Pebble data directories are automatically generated based on ledger ID
		// Config is ignored for Pebble driver

		runtimeStoreMeter := meterProvider.Meter("peeble.runtime_store", metric.WithInstrumentationAttributes(
			attribute.Int("ledger-id", int(ledgerID)),
		))

		// Create runtime store (stores balances, metadata, and logs)
		runtimeStore, err := service.NewPebbleRuntimeStore(dataDir, logger, runtimeStoreMeter)
		if err != nil {
			return nil, fmt.Errorf("creating runtime store: %w", err)
		}

		return runtimeStore, nil
	},
}

// CreateRuntimeStore creates a RuntimeStore based on the ledger driver and config
func CreateRuntimeStore(
	ctx context.Context,
	driver string,
	runtimeConfigJSON []byte,
	logger logging.Logger,
	ledgerID uint64,
	dataDir string,
	meterProvider metric.MeterProvider,
) (RuntimeStoreWithMetrics, error) {

	runtimeStoreFactory, exists := runtimeStoreFactories[driver]
	if !exists {
		return nil, fmt.Errorf("unsupported ledger driver for runtime store: %s", driver)
	}

	runtimeStore, err := runtimeStoreFactory(ctx, runtimeConfigJSON, logger, ledgerID, dataDir, meterProvider)
	if err != nil {
		return nil, fmt.Errorf("creating %s runtime store for ledger: %w", driver, err)
	}

	return runtimeStore, nil
}

// Node represents a Raft group for a specific ledger
type Node struct {
	*raft.Node[*ledgerpb.LedgerState, *FSM]
	config        raft.NodeConfig
	logger        logging.Logger
	defaultLedger *service.DefaultLedger
	ledgerInfo    *ledgerpb.LedgerInfo
	runtimeStore  service.RuntimeStore
}

func (node *Node) GetAllLogs(ctx context.Context, from uint64, to uint64) (service.Cursor[*ledgerpb.Log], error) {
	return node.runtimeStore.GetAllLogs(ctx, from, to)
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

	// Convert StoreConfig from *structpb.Struct to []byte
	var storeConfigJSON []byte
	if ledgerInfo.StoreConfig != nil {
		configMap := ledgerInfo.StoreConfig.AsMap()
		var err error
		storeConfigJSON, err = json.Marshal(configMap)
		if err != nil {
			return nil, fmt.Errorf("marshaling store config: %w", err)
		}
	}

	// Ensure the directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating directory for ledger stores: %w", err)
	}

	// Create runtime store for this ledger based on ledger driver
	runtimeStore, err := CreateRuntimeStore(
		ctx,
		ledgerInfo.GetStoreDriver(),
		storeConfigJSON,
		logger,
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
		runtimeStore,
		recoveryLogReader,
		state,
	)

	ret := &Node{
		config:       cfg,
		logger:       logger,
		ledgerInfo:   ledgerInfo,
		runtimeStore: runtimeStore,
	}

	// Create ledger service for this ledger (will use stores for balance checking and log writing)
	ret.defaultLedger = service.NewDefaultLedger(
		runtimeStore,
		ret,
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
func (node *Node) CreateLog(ctx context.Context, idempotency *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {

	// Create a command to insert the log
	cmd, err := NewCreateLogCommand(input, idempotency)
	if err != nil {
		return nil, fmt.Errorf("creating insert log command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, log, err := node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("applying insert log command via etcdraft: %w", err)
	}

	node.logger.
		WithFields(map[string]any{"commandID": cmd.Id}).
		Debugf("Log inserted via ledger Raft")

	return log.(*ledgerpb.Log), nil
}

func (node *Node) CreateTransaction(ctx context.Context, parameters service.Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.CreateTransaction(ctx, parameters)
}

func (node *Node) RevertTransaction(ctx context.Context, parameters service.Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.RevertTransaction(ctx, parameters)
}

func (node *Node) SaveTransactionMetadata(ctx context.Context, parameters service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveTransactionMetadata(ctx, parameters)
}

func (node *Node) SaveAccountMetadata(ctx context.Context, parameters service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveAccountMetadata(ctx, parameters)
}

func (node *Node) DeleteTransactionMetadata(ctx context.Context, parameters service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteTransactionMetadata(ctx, parameters)
}

func (node *Node) DeleteAccountMetadata(ctx context.Context, parameters service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteAccountMetadata(ctx, parameters)
}

func (node *Node) Import(ctx context.Context, stream chan *ledgerpb.Log) error {
	return node.defaultLedger.Import(ctx, stream)
}

func (node *Node) Export(ctx context.Context, w service.ExportWriter) error {
	return node.defaultLedger.Export(ctx, w)
}

func (node *Node) Info() *ledgerpb.LedgerInfo {
	return node.ledgerInfo
}

// CloseStores closes the runtime store.
func (node *Node) CloseStores() error {
	var errs []error

	if node.runtimeStore != nil {
		if closer, ok := node.runtimeStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing runtime store: %w", err))
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
	// - ledger-{id}-runtime.db
	// - WAL files
	// - Other Raft storage files
	if err := os.RemoveAll(node.config.DataDir); err != nil {
		return fmt.Errorf("deleting ledger data directory %s: %w", node.config.DataDir, err)
	}

	node.logger.WithFields(map[string]any{"dataDir": node.config.DataDir}).Infof("Ledger store files deleted")
	return nil
}

package bucketfsm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// BucketFSM represents the finite state machine for a bucket Raft group
// It manages ledgers within a specific bucket
type BucketFSM struct {
	bucketName      string
	ledgers         map[string]service.LedgerInfo           // Map of ledger name -> ledger info
	logs            []ledger.Log                            // Logs stored in memory until snapshot
	balances        map[string]ledger.Balances              // Map of ledger name -> balances (account -> asset -> balance)
	accountMetadata map[string]map[string]map[string]string // Map of ledger name -> account -> metadata key -> metadata value
	logger          *zap.Logger
}

// NewBucketFSM creates a new bucket FSM
func NewBucketFSM(bucketName string, logger *zap.Logger) *BucketFSM {
	return &BucketFSM{
		bucketName:      bucketName,
		ledgers:         make(map[string]service.LedgerInfo),
		logs:            make([]ledger.Log, 0),
		balances:        make(map[string]ledger.Balances),
		accountMetadata: make(map[string]map[string]map[string]string),
		logger:          logger.With(zap.String("bucket", bucketName), zap.String("component", "bucket-fsm")),
	}
}

// HandleCreateLedger handles the create ledger command for this bucket
func (f *BucketFSM) HandleCreateLedger(cmd service.Command, index uint64) (*service.LedgerInfo, error) {
	var createCmd CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create ledger command", zap.Error(err))
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists in this bucket
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.Warn("Ledger already exists in bucket", zap.String("name", createCmd.Name), zap.String("bucket", f.bucketName))
		return nil, fmt.Errorf("ledger already exists in bucket %s: %s", f.bucketName, createCmd.Name)
	}

	// Assign sequential ID to the ledger (IDs start at 1, so next ID is len(ledgers) + 1)
	ledgerID := uint64(len(f.ledgers) + 1)

	// Convert protobuf Struct to metadata.Metadata
	var md metadata.Metadata
	if createCmd.Metadata != nil {
		md = structToMetadata(createCmd.Metadata)
	}

	// Create ledger info using the command date
	ledgerInfo := service.LedgerInfo{
		ID:        ledgerID,
		Name:      createCmd.Name,
		CreatedAt: cmd.Date,
		Metadata:  md,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	f.logger.Info("Ledger created in bucket", zap.Uint64("index", index), zap.Uint64("id", ledgerID), zap.String("name", createCmd.Name), zap.String("bucket", f.bucketName))
	return &ledgerInfo, nil
}

// HandleInsertLog handles the insert log command by storing the log in memory
// Logs will be persisted to the store during snapshot creation
func (f *BucketFSM) HandleInsertLog(cmd service.Command, index uint64) error {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.Error("Failed to unmarshal insert log command", zap.Error(err))
		return fmt.Errorf("unmarshaling insert log command: %w", err)
	}

	// Convert protobuf Log to ledger.Log
	log, err := logFromProto(insertCmd.Log)
	if err != nil {
		f.logger.Error("Failed to convert log from proto", zap.Error(err))
		return fmt.Errorf("converting log from proto: %w", err)
	}

	// Store log in memory (will be persisted to store during snapshot)
	f.logs = append(f.logs, log)

	// Update last log ID for this ledger
	if log.ID != nil {
		if ledgerInfo, exists := f.ledgers[log.Ledger]; exists {
			ledgerInfo.LastLogID = log.ID
			f.ledgers[log.Ledger] = ledgerInfo
		}
	}

	// Update balances and account metadata based on log type
	f.updateBalancesAndMetadata(log)

	f.logger.Info("Log stored in memory via FSM", zap.Uint64("index", index), zap.String("ledger", log.Ledger), zap.Uint64("commandID", cmd.ID), zap.Int("totalLogsInMemory", len(f.logs)))
	return nil
}

// updateBalancesAndMetadata updates balances and account metadata based on the log
func (f *BucketFSM) updateBalancesAndMetadata(log ledger.Log) {
	ledgerName := log.Ledger

	// Initialize balances map for this ledger if needed
	if f.balances[ledgerName] == nil {
		f.balances[ledgerName] = make(ledger.Balances)
	}

	// Process transaction logs
	switch log.Type {
	case ledger.NewTransactionLogType:
		if createdTx, ok := log.Data.(*ledger.CreatedTransaction); ok {
			tx := createdTx.Transaction
			// Update balances for each posting
			for _, posting := range tx.Postings {
				// Initialize account balance map if needed
				if f.balances[ledgerName][posting.Source] == nil {
					f.balances[ledgerName][posting.Source] = make(map[string]*big.Int)
				}
				if f.balances[ledgerName][posting.Destination] == nil {
					f.balances[ledgerName][posting.Destination] = make(map[string]*big.Int)
				}

				// Initialize asset balance if needed
				if f.balances[ledgerName][posting.Source][posting.Asset] == nil {
					f.balances[ledgerName][posting.Source][posting.Asset] = big.NewInt(0)
				}
				if f.balances[ledgerName][posting.Destination][posting.Asset] == nil {
					f.balances[ledgerName][posting.Destination][posting.Asset] = big.NewInt(0)
				}

				// Update balances: source account decreases, destination account increases
				f.balances[ledgerName][posting.Source][posting.Asset] = new(big.Int).Sub(
					f.balances[ledgerName][posting.Source][posting.Asset],
					posting.Amount,
				)
				f.balances[ledgerName][posting.Destination][posting.Asset] = new(big.Int).Add(
					f.balances[ledgerName][posting.Destination][posting.Asset],
					posting.Amount,
				)
			}

			// Update account metadata
			if len(createdTx.AccountMetadata) > 0 {
				if f.accountMetadata[ledgerName] == nil {
					f.accountMetadata[ledgerName] = make(map[string]map[string]string)
				}
				for account, metadata := range createdTx.AccountMetadata {
					if f.accountMetadata[ledgerName][account] == nil {
						f.accountMetadata[ledgerName][account] = make(map[string]string)
					}
					// Merge metadata (new values override existing ones)
					for k, v := range metadata {
						f.accountMetadata[ledgerName][account][k] = v
					}
				}
			}
		}
	case ledger.RevertedTransactionLogType:
		if revertedTx, ok := log.Data.(*ledger.RevertedTransaction); ok {
			// Reverse the transaction: reverse the postings
			reversedTx := revertedTx.RevertedTransaction.Reverse()
			// Update balances for each reversed posting
			for _, posting := range reversedTx.Postings {
				// Initialize account balance map if needed
				if f.balances[ledgerName][posting.Source] == nil {
					f.balances[ledgerName][posting.Source] = make(map[string]*big.Int)
				}
				if f.balances[ledgerName][posting.Destination] == nil {
					f.balances[ledgerName][posting.Destination] = make(map[string]*big.Int)
				}

				// Initialize asset balance if needed
				if f.balances[ledgerName][posting.Source][posting.Asset] == nil {
					f.balances[ledgerName][posting.Source][posting.Asset] = big.NewInt(0)
				}
				if f.balances[ledgerName][posting.Destination][posting.Asset] == nil {
					f.balances[ledgerName][posting.Destination][posting.Asset] = big.NewInt(0)
				}

				// Update balances: source account decreases, destination account increases
				f.balances[ledgerName][posting.Source][posting.Asset] = new(big.Int).Sub(
					f.balances[ledgerName][posting.Source][posting.Asset],
					posting.Amount,
				)
				f.balances[ledgerName][posting.Destination][posting.Asset] = new(big.Int).Add(
					f.balances[ledgerName][posting.Destination][posting.Asset],
					posting.Amount,
				)
			}
		}
	}
}

// GetLedger returns the ledger info for a given name in this bucket
func (f *BucketFSM) GetLedger(name string) (service.LedgerInfo, bool) {
	info, ok := f.ledgers[name]
	return info, ok
}

// GetAllLedgers returns all ledgers in this bucket
func (f *BucketFSM) GetAllLedgers() map[string]service.LedgerInfo {
	// Return a copy to avoid external modifications
	result := make(map[string]service.LedgerInfo, len(f.ledgers))
	for k, v := range f.ledgers {
		result[k] = v
	}
	return result
}

// CreateSnapshot creates a snapshot of the bucket FSM state
// It writes logs to the log store and returns snapshot data
func (f *BucketFSM) CreateSnapshot(ctx context.Context, logStore service.LogWriter) ([]byte, error) {
	// Write logs to the store before creating snapshot
	if len(f.logs) > 0 {
		if err := logStore.InsertLogs(ctx, f.logs...); err != nil {
			return nil, fmt.Errorf("writing logs to store during snapshot: %w", err)
		}
		f.logger.Info("Logs written to store during snapshot", zap.Int("count", len(f.logs)))
		// Clear logs from memory after writing to store
		f.logs = make([]ledger.Log, 0)
	}

	// Clear balances and account metadata from memory after saving to persistent storage
	f.balances = make(map[string]ledger.Balances)
	f.accountMetadata = make(map[string]map[string]map[string]string)

	snapshotData := map[string]interface{}{
		"ledgers": f.ledgers,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the bucket FSM from a snapshot
func (f *BucketFSM) RestoreSnapshot(data []byte) error {
	var snapshotData struct {
		Ledgers map[string]service.LedgerInfo `json:"ledgers"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.ledgers = snapshotData.Ledgers
	if f.ledgers == nil {
		f.ledgers = make(map[string]service.LedgerInfo)
	}

	// Initialize balances and account metadata maps
	f.balances = make(map[string]ledger.Balances)
	f.accountMetadata = make(map[string]map[string]map[string]string)

	f.logger.Info("Bucket FSM restored from snapshot", zap.Int("ledgerCount", len(f.ledgers)))
	return nil
}

// GetInMemoryBalances returns the in-memory balance diff for a ledger
// This represents the changes since the last snapshot
func (f *BucketFSM) GetInMemoryDiffBalances(ledgerName string) ledger.Balances {
	if f.balances[ledgerName] == nil {
		return make(ledger.Balances)
	}
	// Return a copy to avoid external modifications
	result := make(ledger.Balances)
	for account, assets := range f.balances[ledgerName] {
		result[account] = make(map[string]*big.Int)
		for asset, balance := range assets {
			result[account][asset] = new(big.Int).Set(balance)
		}
	}
	return result
}

// GetInMemoryLogs returns the in-memory logs for a ledger
// This represents the logs since the last snapshot
// Logs are returned in descending order by ID
func (f *BucketFSM) GetInMemoryLogs(ledgerName string) []ledger.Log {
	var result []ledger.Log
	for _, log := range f.logs {
		if log.Ledger == ledgerName {
			result = append(result, log)
		}
	}
	// Sort by ID descending
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID == nil && result[j].ID == nil {
			return false
		}
		if result[i].ID == nil {
			return false // nil IDs go to the end
		}
		if result[j].ID == nil {
			return true // nil IDs go to the end
		}
		return *result[i].ID > *result[j].ID // Descending order
	})
	return result
}

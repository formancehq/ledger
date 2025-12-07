package bucket

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// FSM represents the finite state machine for a bucket Raft group
// It manages ledgers within a specific bucket
type FSM struct {
	ledgers         map[string]ledger.LedgerInfo            // Map of ledger name -> ledger info
	logs            []ledger.Log                            // Logs stored in memory until snapshot
	balances        map[string]ledger.Balances              // Map of ledger name -> balances (account -> asset -> balance)
	accountMetadata map[string]map[string]map[string]string // Map of ledger name -> account -> metadata key -> metadata value
	logger          logging.Logger
	logWriter       service.LogWriter
}

// newFSM creates a new bucket FSM
func newFSM(logger logging.Logger, logStore service.LogWriter) *FSM {
	return &FSM{
		ledgers:         make(map[string]ledger.LedgerInfo),
		logs:            make([]ledger.Log, 0),
		balances:        make(map[string]ledger.Balances),
		accountMetadata: make(map[string]map[string]map[string]string),
		logger:          logger,
		logWriter:       logStore,
	}
}

// handleCreateLedger handles the create ledger command for this bucket
func (f *FSM) handleCreateLedger(cmd raft.Command) (*ledger.LedgerInfo, error) {
	var createCmd CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create ledger command")
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists in this bucket
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.WithFields(map[string]any{"name": createCmd.Name}).Infof("WARN: Ledger already exists in bucket")
		return nil, fmt.Errorf("ledger already exists in bucket: %s", createCmd.Name)
	}

	// Assign sequential ID to the ledger (IDs start at 1, so next ID is len(ledgers) + 1)
	ledgerID := uint64(len(f.ledgers) + 1)

	// Convert protobuf Struct to metadata.Metadata
	var md metadata.Metadata
	if createCmd.Metadata != nil {
		md = structToMetadata(createCmd.Metadata)
	}

	// Create ledger info using the command date
	ledgerInfo := ledger.LedgerInfo{
		ID:        ledgerID,
		Name:      createCmd.Name,
		CreatedAt: cmd.Date,
		Metadata:  md,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	f.logger.Infof("Ledger created in bucket")
	return &ledgerInfo, nil
}

// handleInsertLog handles the insert log command by storing the log in memory
// Logs will be persisted to the store during snapshot creation
func (f *FSM) handleInsertLog(cmd raft.Command) error {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return fmt.Errorf("unmarshaling insert log command: %w", err)
	}

	// Convert protobuf Log to ledger.Log
	log, err := logFromProto(insertCmd.Log)
	if err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to convert log from proto")
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

	f.logger.Infof("Log stored in memory via FSM")
	return nil
}

func (f *FSM) ApplyEntry(ctx context.Context, command raft.Command) (any, error) {
	switch command.Type {
	case CommandTypeCreateLedger:
		return f.handleCreateLedger(command)
	case CommandTypeInsertLog:
		return nil, f.handleInsertLog(command)
	}
	return nil, fmt.Errorf("unknown command type: %s", command.Type)
}

// updateBalancesAndMetadata updates balances and account metadata based on the log
func (f *FSM) updateBalancesAndMetadata(log ledger.Log) {
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
func (f *FSM) GetLedger(name string) (*ledger.LedgerInfo, error) {
	info, ok := f.ledgers[name]
	if !ok {
		return nil, fmt.Errorf("ledger does not exist: %s", name)
	}
	return &info, nil
}

// GetAllLedgers returns all ledgers in this bucket
func (f *FSM) GetAllLedgers() []ledger.LedgerInfo {
	// Return a copy to avoid external modifications
	result := make([]ledger.LedgerInfo, len(f.ledgers))
	for _, v := range f.ledgers {
		result = append(result, v)
	}
	return result
}

func (f *FSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	// Write logs to the store before creating snapshot
	if len(f.logs) > 0 {
		if err := f.logWriter.InsertLogs(ctx, f.logs...); err != nil {
			return nil, fmt.Errorf("writing logs to store during snapshot: %w", err)
		}
		f.logger.WithFields(map[string]any{"count": len(f.logs)}).Infof("Logs written to store during snapshot")
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
func (f *FSM) RestoreSnapshot(ctx context.Context, data []byte) error {
	var snapshotData struct {
		Ledgers map[string]ledger.LedgerInfo `json:"ledgers"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.ledgers = snapshotData.Ledgers
	if f.ledgers == nil {
		f.ledgers = make(map[string]ledger.LedgerInfo)
	}

	// Initialize balances and account metadata maps
	f.balances = make(map[string]ledger.Balances)
	f.accountMetadata = make(map[string]map[string]map[string]string)

	f.logger.WithFields(map[string]any{"ledgerCount": len(f.ledgers)}).Infof("BucketCluster FSM restored from snapshot")
	return nil
}

// GetInMemoryBalances returns the in-memory balance diff for a ledger
// This represents the changes since the last snapshot
func (f *FSM) GetInMemoryDiffBalances(ledgerName string) ledger.Balances {
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
func (f *FSM) GetInMemoryLogs(ledgerName string) []ledger.Log {
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

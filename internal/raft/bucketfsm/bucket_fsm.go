package bucketfsm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

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
	nextLedgerID    uint64                                  // Next sequential ledger ID
	lastLogIDs      map[string]uint64                       // Map of ledger name -> last log ID
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
		nextLedgerID:    1, // Start at 1, first ledger will have ID 1
		lastLogIDs:      make(map[string]uint64),
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

	// Assign sequential ID to the ledger
	ledgerID := f.nextLedgerID
	f.nextLedgerID++

	// Create ledger info using the command date
	ledgerInfo := service.LedgerInfo{
		ID:        ledgerID,
		Name:      createCmd.Name,
		CreatedAt: cmd.Date,
		Metadata:  createCmd.Metadata,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	// Initialize lastLogID for this ledger (starts at 0, first log will be 1)
	f.lastLogIDs[createCmd.Name] = 0

	f.logger.Info("Ledger created in bucket", zap.Uint64("index", index), zap.Uint64("id", ledgerID), zap.String("name", createCmd.Name), zap.String("bucket", f.bucketName))
	return &ledgerInfo, nil
}

// HandleInsertLog handles the insert log command by writing the log to the store
func (f *BucketFSM) HandleInsertLog(cmd service.Command, index uint64, logStore service.LogWriter) error {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.Error("Failed to unmarshal insert log command", zap.Error(err))
		return fmt.Errorf("unmarshaling insert log command: %w", err)
	}

	// Write log to store
	if err := logStore.InsertLogs(context.Background(), insertCmd.Log); err != nil {
		f.logger.Error("Failed to insert log to store", zap.Error(err))
		return fmt.Errorf("inserting log to store: %w", err)
	}

	// Update last log ID for this ledger
	if insertCmd.Log.ID != nil {
		if ledgerInfo, exists := f.ledgers[insertCmd.Log.Ledger]; exists {
			ledgerInfo.LastLogID = insertCmd.Log.ID
			f.ledgers[insertCmd.Log.Ledger] = ledgerInfo
		}
		// Update lastLogIDs map
		f.lastLogIDs[insertCmd.Log.Ledger] = *insertCmd.Log.ID
	}

	// Update balances and account metadata based on log type
	f.updateBalancesAndMetadata(insertCmd.Log)

	f.logger.Info("Log inserted via FSM", zap.Uint64("index", index), zap.String("ledger", insertCmd.Log.Ledger), zap.Uint64("commandID", cmd.ID))
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
func (f *BucketFSM) CreateSnapshot(ctx context.Context, logStore service.LogWriter, bucketStorage service.BucketStorage) ([]byte, error) {
	// Write logs to the store before creating snapshot
	if len(f.logs) > 0 {
		if err := logStore.InsertLogs(ctx, f.logs...); err != nil {
			return nil, fmt.Errorf("writing logs to store during snapshot: %w", err)
		}
		f.logger.Info("Logs written to store during snapshot", zap.Int("count", len(f.logs)))
		// Clear logs from memory after writing to store
		f.logs = make([]ledger.Log, 0)
	}

	// Save balances and account metadata to persistent storage
	for ledgerName, balances := range f.balances {
		if err := bucketStorage.SaveBalances(ctx, ledgerName, balances); err != nil {
			return nil, fmt.Errorf("saving balances for ledger %s: %w", ledgerName, err)
		}
	}

	for ledgerName, accountMetadata := range f.accountMetadata {
		if err := bucketStorage.SaveAccountMetadata(ctx, ledgerName, accountMetadata); err != nil {
			return nil, fmt.Errorf("saving account metadata for ledger %s: %w", ledgerName, err)
		}
	}

	// Clear balances and account metadata from memory after saving to persistent storage
	f.balances = make(map[string]ledger.Balances)
	f.accountMetadata = make(map[string]map[string]map[string]string)

	snapshotData := map[string]interface{}{
		"ledgers":      f.ledgers,
		"nextLedgerID": f.nextLedgerID,
		"lastLogIDs":   f.lastLogIDs,
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
		Ledgers      map[string]service.LedgerInfo `json:"ledgers"`
		NextLedgerID uint64                        `json:"nextLedgerID"`
		LastLogIDs   map[string]uint64             `json:"lastLogIDs"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.ledgers = snapshotData.Ledgers
	if f.ledgers == nil {
		f.ledgers = make(map[string]service.LedgerInfo)
	}

	// Restore nextLedgerID, or calculate from existing ledgers if not present
	if snapshotData.NextLedgerID == 0 {
		maxID := uint64(0)
		for _, ledger := range f.ledgers {
			if ledger.ID > maxID {
				maxID = ledger.ID
			}
		}
		f.nextLedgerID = maxID + 1
	} else {
		f.nextLedgerID = snapshotData.NextLedgerID
	}

	// Restore lastLogIDs
	f.lastLogIDs = snapshotData.LastLogIDs
	if f.lastLogIDs == nil {
		f.lastLogIDs = make(map[string]uint64)
		// Initialize lastLogIDs from ledger LastLogID if available
		for name, ledgerInfo := range f.ledgers {
			if ledgerInfo.LastLogID != nil {
				f.lastLogIDs[name] = *ledgerInfo.LastLogID
			}
		}
	}

	// Initialize balances and account metadata maps
	f.balances = make(map[string]ledger.Balances)
	f.accountMetadata = make(map[string]map[string]map[string]string)

	f.logger.Info("Bucket FSM restored from snapshot", zap.Int("ledgerCount", len(f.ledgers)), zap.Uint64("nextLedgerID", f.nextLedgerID), zap.Int("lastLogIDsCount", len(f.lastLogIDs)))
	return nil
}

// GetBalance returns the balance for a specific account and asset in a ledger
// It combines persistent balances (from snapshot) with in-memory balances
func (f *BucketFSM) GetBalance(ctx context.Context, ledgerName string, account, asset string, bucketStorage service.BucketStorage) (*big.Int, error) {
	// Get persistent balances from storage
	persistentBalances, err := bucketStorage.GetBalances(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting persistent balances: %w", err)
	}

	// Start with persistent balance
	balance := big.NewInt(0)
	if persistentBalances != nil {
		if accountBalances, ok := persistentBalances[account]; ok {
			if assetBalance, ok := accountBalances[asset]; ok {
				balance = new(big.Int).Set(assetBalance)
			}
		}
	}

	// Add in-memory balance diff
	if f.balances[ledgerName] != nil {
		if accountBalances, ok := f.balances[ledgerName][account]; ok {
			if assetBalance, ok := accountBalances[asset]; ok {
				balance = new(big.Int).Add(balance, assetBalance)
			}
		}
	}

	return balance, nil
}

// GetAccountMetadata returns the metadata for a specific account in a ledger
// It combines persistent metadata (from snapshot) with in-memory metadata
func (f *BucketFSM) GetAccountMetadata(ctx context.Context, ledgerName string, account string, bucketStorage service.BucketStorage) (metadata.Metadata, error) {
	// Get persistent account metadata from storage
	persistentAccountMetadata, err := bucketStorage.GetAccountMetadata(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting persistent account metadata: %w", err)
	}

	result := make(metadata.Metadata)

	// Start with persistent metadata
	if persistentAccountMetadata != nil {
		if accountMetadata, ok := persistentAccountMetadata[account]; ok {
			for k, v := range accountMetadata {
				result[k] = v
			}
		}
	}

	// Merge in-memory metadata (overrides persistent)
	if f.accountMetadata[ledgerName] != nil {
		if accountMetadata, ok := f.accountMetadata[ledgerName][account]; ok {
			for k, v := range accountMetadata {
				result[k] = v
			}
		}
	}

	return result, nil
}

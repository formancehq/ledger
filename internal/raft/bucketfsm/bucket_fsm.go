package bucketfsm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// IdempotencyKeyInfo stores information about an idempotency key
type IdempotencyKeyInfo struct {
	LogID uint64 `json:"logID"` // Log ID associated with this idempotency key
	Hash  string `json:"hash"`  // Idempotency hash for verification
}

// BucketFSM represents the finite state machine for a bucket Raft group
// It manages ledgers within a specific bucket
type BucketFSM struct {
	bucketName         string
	ledgers            map[string]service.LedgerInfo             // Map of ledger name -> ledger info
	nextLedgerID       uint64                                    // Next sequential ledger ID
	nextTransactionIDs map[string]uint64                         // Map of ledger name -> next transaction ID
	lastLogIDs         map[string]uint64                         // Map of ledger name -> last log ID
	idempotencyKeys    map[string]IdempotencyKeyInfo             // Map of idempotency key -> log ID and hash
	balances           map[string]map[string]map[string]*big.Int // Map of ledger name -> account -> asset -> balance
	logs               []ledger.Log                              // Logs stored in memory until snapshot
	createdLogs        map[uint64]*ledger.Log                    // Map of entry index -> created log (for retrieving logs after application)
	logger             *zap.Logger
}

// NewBucketFSM creates a new bucket FSM
func NewBucketFSM(bucketName string, logger *zap.Logger) *BucketFSM {
	return &BucketFSM{
		bucketName:         bucketName,
		ledgers:            make(map[string]service.LedgerInfo),
		nextLedgerID:       1, // Start at 1, first ledger will have ID 1
		nextTransactionIDs: make(map[string]uint64),
		lastLogIDs:         make(map[string]uint64),
		idempotencyKeys:    make(map[string]IdempotencyKeyInfo),
		balances:           make(map[string]map[string]map[string]*big.Int),
		logs:               make([]ledger.Log, 0),
		createdLogs:        make(map[uint64]*ledger.Log),
		logger:             logger.With(zap.String("bucket", bucketName), zap.String("component", "bucket-fsm")),
	}
}

// HandleCreateLedger handles the create ledger command for this bucket
func (f *BucketFSM) HandleCreateLedger(cmd service.Command, index uint64) error {
	var createCmd CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create ledger command", zap.Error(err))
		return fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists in this bucket
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.Warn("Ledger already exists in bucket", zap.String("name", createCmd.Name), zap.String("bucket", f.bucketName))
		return fmt.Errorf("ledger already exists in bucket %s: %s", f.bucketName, createCmd.Name)
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
	return nil
}

// HandleCreateTransaction handles the create transaction command by creating a log
func (f *BucketFSM) HandleCreateTransaction(cmd service.Command, index uint64) (*ledger.Log, error) {
	var createCmd CreateTransactionCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create transaction command", zap.Error(err))
		return nil, fmt.Errorf("unmarshaling create transaction command: %w", err)
	}

	// Check idempotency key if provided
	if createCmd.IdempotencyKey != "" {
		if existingInfo, exists := f.idempotencyKeys[createCmd.IdempotencyKey]; exists {
			// Idempotency key already exists, verify hash matches
			expectedHash := ledger.ComputeIdempotencyHash(createCmd.CreateTransaction)
			if existingInfo.Hash != expectedHash {
				f.logger.Warn("Idempotency key conflict: hash mismatch",
					zap.String("idempotencyKey", createCmd.IdempotencyKey),
					zap.String("existingHash", existingInfo.Hash),
					zap.String("expectedHash", expectedHash))
				return nil, service.ErrIdempotencyKeyConflict
			}
			// Hash matches, return error indicating the transaction already exists
			f.logger.Info("Idempotency key already exists with matching hash",
				zap.String("idempotencyKey", createCmd.IdempotencyKey),
				zap.Uint64("logID", existingInfo.LogID))
			return nil, service.ErrIdempotencyKeyConflict
		}
	}

	// Check balances before creating transaction (always check, even for dry run)
	if err := f.checkBalances(createCmd.LedgerName, createCmd.CreateTransaction.Postings); err != nil {
		return nil, err
	}

	// Get last log ID from FSM state
	lastLogID := f.lastLogIDs[createCmd.LedgerName]
	newLogID := lastLogID + 1

	// Get next transaction ID for this ledger (scoped per ledger)
	nextTxID := f.nextTransactionIDs[createCmd.LedgerName]
	newTxID := nextTxID + 1

	// Determine timestamp: use provided timestamp or current time if not provided
	timestamp := createCmd.CreateTransaction.Timestamp
	if timestamp == nil {
		timestamp = &cmd.Date
	}

	// Create transaction with ID, insertedAt and updatedAt dates from command
	tx := ledger.NewTransaction().
		WithPostings(createCmd.CreateTransaction.Postings...).
		WithTimestamp(*timestamp).
		WithMetadata(createCmd.CreateTransaction.Metadata).
		WithID(newTxID).
		WithInsertedAt(cmd.Date).
		WithUpdatedAt(cmd.Date)

	if createCmd.CreateTransaction.Reference != "" {
		tx = tx.WithReference(createCmd.CreateTransaction.Reference)
	}

	// Create CreatedTransaction payload
	createdTx := &ledger.CreatedTransaction{
		Transaction:     tx,
		AccountMetadata: ledger.AccountMetadata(createCmd.CreateTransaction.AccountMetadata),
	}

	// Create log with ID from FSM
	log := ledger.NewLog(createdTx).
		WithDate(*timestamp).
		WithLedger(createCmd.LedgerName).
		WithID(newLogID)

	if createCmd.IdempotencyKey != "" {
		log = log.WithIdempotencyKey(createCmd.IdempotencyKey)
		// Idempotency hash is computed and stored in idempotencyKeys map
		idempotencyHash := ledger.ComputeIdempotencyHash(createCmd.CreateTransaction)
		log.IdempotencyHash = idempotencyHash
	}

	// Store log in memory (will be written to store during snapshot) if not dry run
	if !createCmd.DryRun {
		f.logs = append(f.logs, log)
		// Update last log ID for this ledger
		f.lastLogIDs[createCmd.LedgerName] = newLogID
		// Update next transaction ID for this ledger
		f.nextTransactionIDs[createCmd.LedgerName] = newTxID
		// Update ledger info with last log ID
		if ledgerInfo, exists := f.ledgers[createCmd.LedgerName]; exists {
			ledgerInfo.LastLogID = &newLogID
			f.ledgers[createCmd.LedgerName] = ledgerInfo
		}
		// Store idempotency key if provided
		if createCmd.IdempotencyKey != "" {
			idempotencyHash := ledger.ComputeIdempotencyHash(createCmd.CreateTransaction)
			f.idempotencyKeys[createCmd.IdempotencyKey] = IdempotencyKeyInfo{
				LogID: newLogID,
				Hash:  idempotencyHash,
			}
		}
		// Update balances for each posting
		f.updateBalances(createCmd.LedgerName, createCmd.CreateTransaction.Postings)
		// Store created log for retrieval by entry index
		f.createdLogs[index] = &log
		f.logger.Info("Transaction log created and stored in memory", zap.Uint64("index", index), zap.Uint64("logID", newLogID), zap.String("ledger", createCmd.LedgerName))
	} else {
		f.logger.Info("Transaction log created (dry run)", zap.Uint64("index", index), zap.Uint64("logID", newLogID), zap.String("ledger", createCmd.LedgerName))
	}

	return &log, nil
}

// checkBalances verifies that source accounts have sufficient funds
func (f *BucketFSM) checkBalances(ledgerName string, postings ledger.Postings) error {
	// Group postings by source account and asset to check balances
	requiredFunds := make(map[string]map[string]*big.Int) // account -> asset -> amount

	for _, posting := range postings {
		if posting.Source == ledger.WORLD {
			continue // WORLD account has infinite funds
		}

		// Track required funds
		if requiredFunds[posting.Source] == nil {
			requiredFunds[posting.Source] = make(map[string]*big.Int)
		}
		if requiredFunds[posting.Source][posting.Asset] == nil {
			requiredFunds[posting.Source][posting.Asset] = big.NewInt(0)
		}
		requiredFunds[posting.Source][posting.Asset].Add(requiredFunds[posting.Source][posting.Asset], posting.Amount)
	}

	// Check if accounts have sufficient funds using balances from FSM
	for account, assets := range requiredFunds {
		for asset, requiredAmount := range assets {
			// Get balance from FSM (default to 0 if not found)
			var balance *big.Int
			if ledgerBalances, ok := f.balances[ledgerName]; ok {
				if accountBalances, ok := ledgerBalances[account]; ok {
					if assetBalance, ok := accountBalances[asset]; ok {
						balance = assetBalance
					} else {
						balance = big.NewInt(0)
					}
				} else {
					balance = big.NewInt(0)
				}
			} else {
				balance = big.NewInt(0)
			}

			if balance.Cmp(requiredAmount) < 0 {
				f.logger.Warn("Insufficient funds",
					zap.String("ledger", ledgerName),
					zap.String("account", account),
					zap.String("asset", asset),
					zap.String("balance", balance.String()),
					zap.String("required", requiredAmount.String()))
				return service.ErrInsufficientFunds
			}
		}
	}

	return nil
}

// updateBalances updates account balances based on postings
func (f *BucketFSM) updateBalances(ledgerName string, postings ledger.Postings) {
	// Initialize ledger balances map if needed
	if f.balances[ledgerName] == nil {
		f.balances[ledgerName] = make(map[string]map[string]*big.Int)
	}

	for _, posting := range postings {
		// Skip WORLD account (has infinite funds)
		if posting.Source == ledger.WORLD {
			// Only update destination account
			if f.balances[ledgerName][posting.Destination] == nil {
				f.balances[ledgerName][posting.Destination] = make(map[string]*big.Int)
			}
			if f.balances[ledgerName][posting.Destination][posting.Asset] == nil {
				f.balances[ledgerName][posting.Destination][posting.Asset] = big.NewInt(0)
			}
			// Add to destination (credit)
			f.balances[ledgerName][posting.Destination][posting.Asset] = new(big.Int).Add(
				f.balances[ledgerName][posting.Destination][posting.Asset],
				posting.Amount,
			)
			// Clean up zero balances
			f.cleanupZeroBalance(ledgerName, posting.Destination, posting.Asset)
			continue
		}

		// Initialize account maps if needed
		if f.balances[ledgerName][posting.Source] == nil {
			f.balances[ledgerName][posting.Source] = make(map[string]*big.Int)
		}
		if f.balances[ledgerName][posting.Destination] == nil {
			f.balances[ledgerName][posting.Destination] = make(map[string]*big.Int)
		}

		// Initialize asset balances if needed
		if f.balances[ledgerName][posting.Source][posting.Asset] == nil {
			f.balances[ledgerName][posting.Source][posting.Asset] = big.NewInt(0)
		}
		if f.balances[ledgerName][posting.Destination][posting.Asset] == nil {
			f.balances[ledgerName][posting.Destination][posting.Asset] = big.NewInt(0)
		}

		// Update source account (debit: subtract amount)
		f.balances[ledgerName][posting.Source][posting.Asset] = new(big.Int).Sub(
			f.balances[ledgerName][posting.Source][posting.Asset],
			posting.Amount,
		)

		// Update destination account (credit: add amount)
		f.balances[ledgerName][posting.Destination][posting.Asset] = new(big.Int).Add(
			f.balances[ledgerName][posting.Destination][posting.Asset],
			posting.Amount,
		)

		// Clean up zero balances
		f.cleanupZeroBalance(ledgerName, posting.Source, posting.Asset)
		f.cleanupZeroBalance(ledgerName, posting.Destination, posting.Asset)
	}
}

// cleanupZeroBalance removes balance entries that are zero to free memory
func (f *BucketFSM) cleanupZeroBalance(ledgerName, account, asset string) {
	if ledgerBalances, ok := f.balances[ledgerName]; ok {
		if accountBalances, ok := ledgerBalances[account]; ok {
			if balance, ok := accountBalances[asset]; ok {
				// Remove balance if it's zero
				if balance.Cmp(big.NewInt(0)) == 0 {
					delete(accountBalances, asset)
					// If account has no more assets, remove the account entry
					if len(accountBalances) == 0 {
						delete(ledgerBalances, account)
					}
					// If ledger has no more accounts, remove the ledger entry
					if len(ledgerBalances) == 0 {
						delete(f.balances, ledgerName)
					}
				}
			}
		}
	}
}

// GetCreatedLog returns the log created at the given entry index
func (f *BucketFSM) GetCreatedLog(index uint64) (*ledger.Log, bool) {
	log, ok := f.createdLogs[index]
	return log, ok
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

	// Convert balances to JSON-serializable format
	balancesJSON := make(map[string]map[string]map[string]string)
	for ledgerName, accounts := range f.balances {
		balancesJSON[ledgerName] = make(map[string]map[string]string)
		for account, assets := range accounts {
			balancesJSON[ledgerName][account] = make(map[string]string)
			for asset, balance := range assets {
				balancesJSON[ledgerName][account][asset] = balance.String()
			}
		}
	}

	snapshotData := map[string]interface{}{
		"ledgers":            f.ledgers,
		"nextLedgerID":       f.nextLedgerID,
		"nextTransactionIDs": f.nextTransactionIDs,
		"lastLogIDs":         f.lastLogIDs,
		"idempotencyKeys":    f.idempotencyKeys,
		"balances":           balancesJSON,
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
		Ledgers            map[string]service.LedgerInfo           `json:"ledgers"`
		NextLedgerID       uint64                                  `json:"nextLedgerID"`
		NextTransactionIDs map[string]uint64                       `json:"nextTransactionIDs"`
		LastLogIDs         map[string]uint64                       `json:"lastLogIDs"`
		IdempotencyKeys    map[string]IdempotencyKeyInfo           `json:"idempotencyKeys"`
		Balances           map[string]map[string]map[string]string `json:"balances"`
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

	// Restore nextTransactionIDs
	f.nextTransactionIDs = snapshotData.NextTransactionIDs
	if f.nextTransactionIDs == nil {
		f.nextTransactionIDs = make(map[string]uint64)
	}

	// Restore idempotencyKeys
	f.idempotencyKeys = snapshotData.IdempotencyKeys
	if f.idempotencyKeys == nil {
		f.idempotencyKeys = make(map[string]IdempotencyKeyInfo)
	}

	// Restore balances
	f.balances = make(map[string]map[string]map[string]*big.Int)
	if snapshotData.Balances != nil {
		for ledgerName, accounts := range snapshotData.Balances {
			f.balances[ledgerName] = make(map[string]map[string]*big.Int)
			for account, assets := range accounts {
				f.balances[ledgerName][account] = make(map[string]*big.Int)
				for asset, balanceStr := range assets {
					balance, ok := new(big.Int).SetString(balanceStr, 10)
					if !ok {
						return fmt.Errorf("invalid balance value for ledger %s, account %s, asset %s: %s", ledgerName, account, asset, balanceStr)
					}
					f.balances[ledgerName][account][asset] = balance
				}
			}
		}
	}

	f.logger.Info("Bucket FSM restored from snapshot", zap.Int("ledgerCount", len(f.ledgers)), zap.Uint64("nextLedgerID", f.nextLedgerID), zap.Int("lastLogIDsCount", len(f.lastLogIDs)), zap.Int("idempotencyKeysCount", len(f.idempotencyKeys)), zap.Int("balancesCount", len(f.balances)))
	return nil
}

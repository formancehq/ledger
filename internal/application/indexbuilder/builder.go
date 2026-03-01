package indexbuilder

import (
	"io"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	bolt "go.etcd.io/bbolt"
)

// Builder tails the system log and populates the bbolt read store indexes.
// It runs as a background goroutine on ALL nodes (not just the leader).
// Progress is stored locally in bbolt (no Raft needed).
type Builder struct {
	pebbleStore   *dal.Store
	readStore     *readstore.Store
	logger        logging.Logger
	notifications *signal.Notifications
	w             worker.Worker
}

// NewBuilder creates a new index builder.
func NewBuilder(
	pebbleStore *dal.Store,
	readStore *readstore.Store,
	logger logging.Logger,
) *Builder {
	return &Builder{
		pebbleStore: pebbleStore,
		readStore:   readStore,
		logger:      logger.WithFields(map[string]any{"cmp": "index-builder"}),
	}
}

// SetNotifications sets the dedicated Notifications signal for the builder.
func (b *Builder) SetNotifications(n *signal.Notifications) {
	b.notifications = n
}

// Start begins the background index-building loop.
func (b *Builder) Start() {
	b.w = worker.New()
	b.w.Run(b.loop)
}

// Stop gracefully stops the background loop.
func (b *Builder) Stop() {
	b.w.Stop()
}

func (b *Builder) loop(stop <-chan struct{}) {
	cursor, err := b.readStore.LastIndexedSequence()
	if err != nil {
		b.logger.Errorf("Failed to read progress: %v", err)
		return
	}
	b.logger.WithFields(map[string]any{"cursor": cursor}).Infof("Index builder started")

	// Initial catch-up
	if cursor, err = b.processLogs(cursor); err != nil {
		b.logger.Errorf("Error during initial catch-up: %v", err)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			b.logger.Infof("Index builder stopped")
			return
		case <-b.notifications.LogCommitted.C():
		case <-ticker.C:
		}

		// Check stop again after waking up, before accessing the store.
		// This avoids a race where the Pebble DB is closed between the
		// select wakeup and the store access.
		select {
		case <-stop:
			b.logger.Infof("Index builder stopped")
			return
		default:
		}

		if cursor, err = b.processLogs(cursor); err != nil {
			b.logger.Errorf("Error processing logs: %v", err)
		}
	}
}

// processLogs reads logs from Pebble starting after the given cursor,
// processes each log, and returns the updated cursor position.
func (b *Builder) processLogs(cursor uint64) (uint64, error) {
	logsCursor, err := query.ReadLogsSince(b.pebbleStore, cursor)
	if err != nil {
		return cursor, err
	}
	defer func() { _ = logsCursor.Close() }()

	for {
		log, err := logsCursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return cursor, err
		}

		if err := b.processLog(log); err != nil {
			b.logger.WithFields(map[string]any{
				"sequence": log.Sequence,
				"error":    err,
			}).Errorf("Error processing log entry")
			return cursor, err
		}

		cursor = log.Sequence
	}

	return cursor, nil
}

// RebuildAll replays all system logs from scratch (starting at sequence 0),
// rebuilding the entire read index. This is intended for offline use
// (CLI backfill). Returns the last processed log sequence.
func (b *Builder) RebuildAll() (uint64, error) {
	return b.processLogs(0)
}

// processLog dispatches a single log entry to the appropriate handler
// and updates the progress cursor in bbolt.
func (b *Builder) processLog(log *commonpb.Log) error {
	if log.Payload == nil {
		return nil
	}

	// Only process ApplyLedgerLog entries — other log types (create/delete ledger,
	// signing keys, periods, etc.) don't produce indexable data.
	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok {
		// Still advance the cursor past non-apply logs.
		return b.readStore.Update(func(tx *bolt.Tx) error {
			return b.readStore.WriteProgress(tx, log.Sequence)
		})
	}

	ledgerName := applyLog.Apply.LedgerName
	ledgerLog := applyLog.Apply.Log
	if ledgerLog == nil || ledgerLog.Data == nil {
		return b.readStore.Update(func(tx *bolt.Tx) error {
			return b.readStore.WriteProgress(tx, log.Sequence)
		})
	}

	return b.readStore.Update(func(tx *bolt.Tx) error {
		kb := readstore.NewKeyBuilder()

		switch p := ledgerLog.Data.Payload.(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if err := b.indexCreatedTransaction(tx, kb, ledgerName, p.CreatedTransaction); err != nil {
				return err
			}
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			if err := b.indexRevertedTransaction(tx, kb, ledgerName, p.RevertedTransaction); err != nil {
				return err
			}
		case *commonpb.LedgerLogPayload_SavedMetadata:
			if err := b.indexSavedMetadata(tx, kb, ledgerName, p.SavedMetadata); err != nil {
				return err
			}
		case *commonpb.LedgerLogPayload_DeletedMetadata:
			if err := b.indexDeletedMetadata(tx, kb, ledgerName, p.DeletedMetadata); err != nil {
				return err
			}
		case *commonpb.LedgerLogPayload_SetMetadataFieldType:
			if err := b.indexSetMetadataFieldType(tx, kb, ledgerName, p.SetMetadataFieldType); err != nil {
				return err
			}
			// ConvertMetadataBatch and MetadataConversionComplete are no-ops for the
			// index builder — schema changes are handled by SetMetadataFieldType.
		}

		return b.readStore.WriteProgress(tx, log.Sequence)
	})
}

// indexCreatedTransaction handles CreatedTransaction logs by indexing:
// - transaction existence
// - account existence (for all accounts in postings + account_metadata)
// - account metadata (from account_metadata)
// - transaction metadata (from transaction.metadata)
// - account→transaction mapping
func (b *Builder) indexCreatedTransaction(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	ledger string,
	ct *commonpb.CreatedTransaction,
) error {
	if ct.Transaction == nil {
		return nil
	}
	txn := ct.Transaction

	// Transaction existence
	if err := readstore.WriteTransactionExistence(tx, kb, ledger, txn.Id); err != nil {
		return err
	}

	// Collect unique accounts from postings
	accounts := make(map[string]struct{})
	for _, posting := range txn.Postings {
		accounts[posting.Source] = struct{}{}
		accounts[posting.Destination] = struct{}{}

		// Account→transaction mapping (any role)
		if err := readstore.WriteAccountTxMapping(tx, kb, ledger, posting.Source, txn.Id); err != nil {
			return err
		}
		if err := readstore.WriteAccountTxMapping(tx, kb, ledger, posting.Destination, txn.Id); err != nil {
			return err
		}
		// Role-specific mappings
		if err := readstore.WriteSourceAccountTxMapping(tx, kb, ledger, posting.Source, txn.Id); err != nil {
			return err
		}
		if err := readstore.WriteDestAccountTxMapping(tx, kb, ledger, posting.Destination, txn.Id); err != nil {
			return err
		}
	}

	// Account existence for all accounts in postings
	for account := range accounts {
		if err := readstore.WriteAccountExistence(tx, kb, ledger, account); err != nil {
			return err
		}
	}

	// Account existence + metadata from account_metadata map
	for account, metadataSet := range ct.AccountMetadata {
		if err := readstore.WriteAccountExistence(tx, kb, ledger, account); err != nil {
			return err
		}
		if metadataSet != nil {
			for _, md := range metadataSet.Metadata {
				reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.Key)
				encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
				if err := readstore.UpdateMetadataIndex(
					tx, kb, reverseKey,
					ledger, readstore.NamespaceAccount, md.Key,
					encodedValue, []byte(account),
				); err != nil {
					return err
				}
			}
		}
	}

	// Transaction metadata
	if txn.Metadata != nil {
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.Id)
		for _, md := range txn.Metadata.Metadata {
			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txn.Id, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			if err := readstore.UpdateMetadataIndex(
				tx, kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.Key,
				encodedValue, txIDBytes,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// indexRevertedTransaction handles RevertedTransaction logs by indexing:
// - revert transaction existence
// - account existence for revert postings
// - account→transaction mapping for revert postings
func (b *Builder) indexRevertedTransaction(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	ledger string,
	rt *commonpb.RevertedTransaction,
) error {
	if rt.RevertTransaction == nil {
		return nil
	}
	revertTxn := rt.RevertTransaction

	// Revert transaction existence
	if err := readstore.WriteTransactionExistence(tx, kb, ledger, revertTxn.Id); err != nil {
		return err
	}

	// Account existence + account→tx mapping for revert postings
	accounts := make(map[string]struct{})
	for _, posting := range revertTxn.Postings {
		accounts[posting.Source] = struct{}{}
		accounts[posting.Destination] = struct{}{}
		if err := readstore.WriteAccountTxMapping(tx, kb, ledger, posting.Source, revertTxn.Id); err != nil {
			return err
		}
		if err := readstore.WriteAccountTxMapping(tx, kb, ledger, posting.Destination, revertTxn.Id); err != nil {
			return err
		}
		// Role-specific mappings
		if err := readstore.WriteSourceAccountTxMapping(tx, kb, ledger, posting.Source, revertTxn.Id); err != nil {
			return err
		}
		if err := readstore.WriteDestAccountTxMapping(tx, kb, ledger, posting.Destination, revertTxn.Id); err != nil {
			return err
		}
	}
	for account := range accounts {
		if err := readstore.WriteAccountExistence(tx, kb, ledger, account); err != nil {
			return err
		}
	}

	// Transaction metadata for the revert transaction
	if revertTxn.Metadata != nil {
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.Id)
		for _, md := range revertTxn.Metadata.Metadata {
			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, revertTxn.Id, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			if err := readstore.UpdateMetadataIndex(
				tx, kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.Key,
				encodedValue, txIDBytes,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// indexSavedMetadata handles SavedMetadata logs.
func (b *Builder) indexSavedMetadata(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	ledger string,
	sm *commonpb.SavedMetadata,
) error {
	if sm.Target == nil || sm.Metadata == nil {
		return nil
	}

	switch t := sm.Target.Target.(type) {
	case *commonpb.Target_Account:
		account := t.Account.Addr
		for _, md := range sm.Metadata.Metadata {
			reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			if err := readstore.UpdateMetadataIndex(
				tx, kb, reverseKey,
				ledger, readstore.NamespaceAccount, md.Key,
				encodedValue, []byte(account),
			); err != nil {
				return err
			}
		}
	case *commonpb.Target_Transaction:
		txID := t.Transaction.Id
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		for _, md := range sm.Metadata.Metadata {
			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			if err := readstore.UpdateMetadataIndex(
				tx, kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.Key,
				encodedValue, txIDBytes,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// indexDeletedMetadata handles DeletedMetadata logs.
func (b *Builder) indexDeletedMetadata(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	ledger string,
	dm *commonpb.DeletedMetadata,
) error {
	if dm.Target == nil {
		return nil
	}

	switch t := dm.Target.Target.(type) {
	case *commonpb.Target_Account:
		account := t.Account.Addr
		reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, dm.Key)
		return readstore.DeleteMetadataEntry(
			tx, kb, reverseKey,
			ledger, readstore.NamespaceAccount, dm.Key,
			[]byte(account),
		)
	case *commonpb.Target_Transaction:
		txID := t.Transaction.Id
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, dm.Key)
		return readstore.DeleteMetadataEntry(
			tx, kb, reverseKey,
			ledger, readstore.NamespaceTransaction, dm.Key,
			txIDBytes,
		)
	}

	return nil
}

// indexSetMetadataFieldType handles schema change logs by re-encoding all
// inverted index entries for the affected key using the new type.
//
// Strategy: iterate the reverse map to find all entities that have this metadata key,
// then for each entity: delete the old forward index entry, convert the value,
// insert the new forward index entry, and update the reverse map.
func (b *Builder) indexSetMetadataFieldType(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	ledger string,
	smft *commonpb.SetMetadataFieldTypeLog,
) error {
	var ns string
	switch smft.TargetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		ns = readstore.NamespaceAccount
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		ns = readstore.NamespaceTransaction
	default:
		return nil
	}

	midxBucket := tx.Bucket(readstore.BucketMetadataIndex)
	rmapBucket := tx.Bucket(readstore.BucketReverseMap)

	// Iterate the reverse map for this namespace to find all entities with the key.
	rmapPrefix := kb.Reset().
		PutLedger(ledger).
		PutNamespace(ns).
		Snapshot()

	type rmapEntry struct {
		rmapKey  []byte // full reverse map key
		entityID []byte // account address or txID bytes
		oldValue []byte // old MetadataValue protobuf
	}
	var entries []rmapEntry

	rc := rmapBucket.Cursor()
	for k, v := rc.Seek(rmapPrefix); k != nil && readstore.HasPrefix(k, rmapPrefix); k, v = rc.Next() {
		metaKey := extractMetadataKeyFromReverseMap(k, rmapPrefix, ns)
		if metaKey != smft.Key {
			continue
		}

		entries = append(entries, rmapEntry{
			rmapKey:  cloneBytes(k),
			entityID: extractEntityIDFromReverseMap(k, rmapPrefix, ns),
			oldValue: cloneBytes(v),
		})
	}

	// For each entity: delete old forward index, convert, insert new forward index, update reverse map.
	for _, e := range entries {
		// Decode old MetadataValue
		oldMV := &commonpb.MetadataValue{}
		if err := oldMV.UnmarshalVT(e.oldValue); err != nil {
			b.logger.WithFields(map[string]any{
				"key":   smft.Key,
				"error": err,
			}).Errorf("Failed to unmarshal reverse map value during schema change")
			continue
		}

		// Delete old forward index entry
		oldEncoded := readstore.EncodeMetadataValue(nil, oldMV)
		oldKey := readstore.MetadataIndexKey(kb, ledger, ns, smft.Key, oldEncoded, e.entityID)
		if err := midxBucket.Delete(oldKey); err != nil {
			return err
		}

		// Convert to new type
		newMV := commonpb.ConvertMetadataValue(oldMV, smft.Type)
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// Write new forward index entry
		newKey := readstore.MetadataIndexKey(kb, ledger, ns, smft.Key, newEncoded, e.entityID)
		if err := midxBucket.Put(newKey, nil); err != nil {
			return err
		}

		// Update reverse map with new value
		newMVBytes, err := newMV.MarshalVT()
		if err != nil {
			return err
		}
		if err := rmapBucket.Put(e.rmapKey, newMVBytes); err != nil {
			return err
		}
	}

	return nil
}

// cloneBytes returns a copy of the given byte slice.
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// extractMetadataKeyFromReverseMap extracts the metadata key name from a
// reverse map key, given the prefix up to the namespace.
// For accounts:     [ledger\x00][a:][account\x00][metadataKey]
// For transactions: [ledger\x00][t:][txID(8B)][metadataKey]
func extractMetadataKeyFromReverseMap(key, nsPrefix []byte, ns string) string {
	suffix := key[len(nsPrefix):]
	if ns == readstore.NamespaceAccount {
		// Find the null terminator after the account address
		for i, b := range suffix {
			if b == 0x00 {
				return string(suffix[i+1:])
			}
		}
		return ""
	}
	// Transaction: skip 8-byte txID
	if len(suffix) > 8 {
		return string(suffix[8:])
	}
	return ""
}

// extractEntityIDFromReverseMap extracts the entity ID portion from a reverse map key.
func extractEntityIDFromReverseMap(key, nsPrefix []byte, ns string) []byte {
	suffix := key[len(nsPrefix):]
	if ns == readstore.NamespaceAccount {
		// Entity ID is the account address (up to \x00)
		for i, b := range suffix {
			if b == 0x00 {
				return suffix[:i]
			}
		}
		return suffix
	}
	// Transaction: entity ID is first 8 bytes
	if len(suffix) >= 8 {
		return suffix[:8]
	}
	return suffix
}

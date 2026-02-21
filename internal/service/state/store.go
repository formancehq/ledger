package state

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadPeriods returns a cursor over all periods from the given reader, ordered by period ID.
func ReadPeriods(reader dal.PebbleReader) (dal.Cursor[*commonpb.Period], error) {
	lowerBound := []byte{dal.KeyPrefixPeriods}
	upperBound := []byte{dal.KeyPrefixPeriods, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for periods: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.Period](iter), nil
}

// ReadAllPeriods returns all periods stored in Pebble, ordered by period ID.
// Returns nil if no periods have been persisted yet.
func ReadAllPeriods(reader dal.PebbleReader) ([]*commonpb.Period, error) {
	cursor, err := ReadPeriods(reader)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close() }()

	var periods []*commonpb.Period
	for {
		p, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		periods = append(periods, p)
	}

	return periods, nil
}

// ReadNextPeriodID returns the next period ID from the given reader.
// Returns 1 if not found (default starting value).
func ReadNextPeriodID(reader dal.PebbleReader) (uint64, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixNextPeriodID})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 1, nil
		}
		return 0, fmt.Errorf("getting next period ID: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	return binary.BigEndian.Uint64(value[:8]), nil
}

// ReadLastLog returns the full last log entry from the given reader. Returns nil if no logs exist.
func ReadLastLog(reader dal.PebbleReader) (*commonpb.Log, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(dal.KeyPrefixLog).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return nil, nil
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading log value: %w", err)
	}

	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log: %w", err)
	}

	return log, nil
}

// ReadLastSequence returns the last log sequence number from the given reader.
// Returns 0 if no logs exist. Reuses ReadLastLog to avoid duplicating the iterator logic.
func ReadLastSequence(reader dal.PebbleReader) (uint64, error) {
	log, err := ReadLastLog(reader)
	if err != nil {
		return 0, err
	}
	if log == nil {
		return 0, nil
	}
	return log.Sequence, nil
}

// ReadLastAppliedIndex returns the last applied Raft index from the given reader.
// Returns 0 if not found.
func ReadLastAppliedIndex(reader dal.PebbleReader) (uint64, error) {
	get, closer, err := reader.Get([]byte{dal.KeyPrefixLastAppliedIndex})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer func() {
		_ = closer.Close()
	}()

	if len(get) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadLastAppliedTimestamp returns the last applied HLC timestamp (microseconds since epoch) from the given reader.
// Returns 0 if not found.
func ReadLastAppliedTimestamp(reader dal.PebbleReader) (uint64, error) {
	get, closer, err := reader.Get([]byte{dal.KeyPrefixLastAppliedTimestamp})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer func() {
		_ = closer.Close()
	}()

	if len(get) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadLogBySequence retrieves a log by its sequence number from the given reader.
func ReadLogBySequence(reader dal.PebbleReader, sequence uint64) (*commonpb.Log, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog).
		PutUInt64(sequence)

	value, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting system log by sequence: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling system log from protobuf: %w", err)
	}

	return log, nil
}

// ReadTransactionUpdates retrieves all updates for a transaction ID from the given reader, ordered by ByLog.
func ReadTransactionUpdates(reader dal.PebbleReader, ledgerID uint32, transactionID uint64) ([]*commonpb.TransactionUpdate, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixTransactionUpdate).
		PutLedgerPrefix(ledgerID).
		PutUInt64(transactionID)
	lowerBound := kb.Snapshot()

	// Upper bound: add 0xFF to get all entries for this transaction
	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for transaction updates: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var updates []*commonpb.TransactionUpdate

	for iter.First(); iter.Valid(); iter.Next() {
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading transaction update value: %w", err)
		}

		update := &commonpb.TransactionUpdate{}
		if err := proto.Unmarshal(valueBytes, update); err != nil {
			return nil, fmt.Errorf("unmarshaling transaction update: %w", err)
		}

		updates = append(updates, update)
	}

	return updates, nil
}

// ReadLogsSince returns a cursor over global log entries after the given sequence from the given reader.
// Pass afterSequence=0 to return all log entries.
func ReadLogsSince(reader dal.PebbleReader, afterSequence uint64) (dal.Cursor[*commonpb.Log], error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)
	if afterSequence > 0 {
		kb.PutUInt64(afterSequence + 1)
	}
	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixLog).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb2.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for logs: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.Log](iter), nil
}

// ReadLedgers returns a cursor over all registered ledgers from the given reader.
func ReadLedgers(reader dal.PebbleReader) (dal.Cursor[*commonpb.LedgerInfo], error) {
	lowerBound := []byte{dal.KeyPrefixLedgerInfo}
	upperBound := []byte{dal.KeyPrefixLedgerInfo, 0xFF, 0xFF, 0xFF, 0xFF}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for ledger info: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.LedgerInfo](iter), nil
}

// ReadPeriodSchedule loads the period schedule cron expression from the given reader.
// Returns an empty string if no schedule is configured.
func ReadPeriodSchedule(reader dal.PebbleReader) (string, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixPeriodSchedule})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("loading period schedule: %w", err)
	}
	defer func() { _ = closer.Close() }()

	return string(value), nil
}

// GetLedgerByName retrieves a ledger by its name from the given reader.
// Returns dal.ErrNotFound if the ledger does not exist or is soft-deleted.
func GetLedgerByName(reader dal.PebbleReader, name string) (*commonpb.LedgerInfo, error) {
	cursor, err := ReadLedgers(reader)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close() }()

	for {
		ledger, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if ledger.Name == name {
			// Check if soft-deleted
			if ledger.DeletedAt != nil {
				return nil, dal.ErrNotFound
			}
			return ledger, nil
		}
	}

	return nil, dal.ErrNotFound
}

// FindTransactionCreationLog returns the system log that created a transaction.
// It resolves the ledger name to an ID, finds the TransactionInit update, and retrieves the log.
func FindTransactionCreationLog(reader dal.PebbleReader, ledgerName string, txID uint64) (*commonpb.Log, error) {
	ledgerInfo, err := GetLedgerByName(reader, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("resolving ledger ID for %s: %w", ledgerName, err)
	}

	updates, err := ReadTransactionUpdates(reader, ledgerInfo.Id, txID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", txID, err)
	}

	var sequence uint64
	for _, update := range updates {
		for _, ut := range update.Updates {
			if ut.GetTransactionInit() != nil {
				sequence = update.ByLog
				break
			}
		}
		if sequence != 0 {
			break
		}
	}
	if sequence == 0 {
		return nil, dal.ErrNotFound
	}

	log, err := ReadLogBySequence(reader, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, dal.ErrNotFound
	}

	return log, nil
}

// ReadMaxLedgerID returns the highest ledger ID from the given reader. Returns 0, false if no ledgers exist.
func ReadMaxLedgerID(reader dal.PebbleReader) (uint32, bool, error) {
	cursor, err := ReadLedgers(reader)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = cursor.Close() }()

	var (
		maxID uint32
		found bool
	)
	for {
		ledger, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, false, err
		}
		if !found || ledger.Id > maxID {
			maxID = ledger.Id
			found = true
		}
	}

	return maxID, found, nil
}

// ReadLastAuditSequence returns the last audit entry sequence from the given reader. Returns 0 if no entries exist.
func ReadLastAuditSequence(reader dal.PebbleReader) (uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixAudit)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(dal.KeyPrefixAudit).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return 0, nil
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return 0, fmt.Errorf("reading audit value: %w", err)
	}

	entry := &auditpb.AuditEntry{}
	if err := proto.Unmarshal(value, entry); err != nil {
		return 0, fmt.Errorf("unmarshaling audit entry: %w", err)
	}

	return entry.Sequence, nil
}

// ReadAuditEntries returns a cursor over audit entries after the given sequence from the given reader.
// Use afterSequence=nil to return all entries, or a pointer to a sequence to filter.
func ReadAuditEntries(reader dal.PebbleReader, afterSequence *uint64) (dal.Cursor[*auditpb.AuditEntry], error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixAudit)
	if afterSequence != nil {
		kb.PutUInt64(*afterSequence + 1)
	}
	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixAudit).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb2.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for audit entries: %w", err)
	}

	return dal.NewProtoCursor[*auditpb.AuditEntry](iter), nil
}

// ReadSigningKeys loads all signing keys from the given reader.
// Returns a map of keyID → publicKey (32-byte Ed25519 public key).
func ReadSigningKeys(reader dal.PebbleReader) (map[string][]byte, error) {
	lowerBound := []byte{dal.KeyPrefixSigningKey}
	upperBound := []byte{dal.KeyPrefixSigningKey + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for signing keys: %w", err)
	}
	defer func() { _ = iter.Close() }()

	keys := make(map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		// Key format: [KeyPrefixSigningKey(1)][keyID(variable)]
		key := iter.Key()
		keyID := string(key[1:]) // skip the prefix byte

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading signing key value: %w", err)
		}

		pubKey := make([]byte, len(value))
		copy(pubKey, value)
		keys[keyID] = pubKey
	}

	return keys, nil
}

// ReadSigningConfig loads the require-signatures flag from the given reader.
// Returns false if the config key does not exist.
func ReadSigningConfig(reader dal.PebbleReader) (bool, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixSigningConfig})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("loading signing config: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) == 0 {
		return false, nil
	}
	return value[0] == 0x01, nil
}

// ReadMaintenanceMode loads the maintenance mode flag from the given reader.
// Returns false if the config key does not exist.
func ReadMaintenanceMode(reader dal.PebbleReader) (bool, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixMaintenanceMode})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("loading maintenance mode: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) == 0 {
		return false, nil
	}
	return value[0] == 0x01, nil
}

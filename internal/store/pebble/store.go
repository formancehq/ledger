package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

const (
	liveDir               = "live"
	currentCheckpointFile = "CURRENT_CHECKPOINT"
	checkpointsDir        = "checkpoints"
)

var _ store.Store = (*Store)(nil)

// Store is a Pebble implementation of store.Store
// It stores balances and account metadata
// todo: add rotation of checkpoints
type Store struct {
	db                *pebble.DB
	logger            logging.Logger
	dataDir           string
	currentCheckPoint uint64
}

// Key prefixes for Pebble storage (3 bytes max)
var (
	keyPrefixLastAppliedIndex byte = 0x00
	keyPrefixLog              byte = 0x01
	keyPrefixBalanceDiff      byte = 0x02
	keyPrefixAccountMetadata  byte = 0x03
	keyPrefixIdempotency      byte = 0x04
	keyPrefixTransactionID    byte = 0x05
	keyPrefixRevertedTxID     byte = 0x06
)

// NewStore creates a new Store instance
func NewStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
) (*Store, error) {

	opts := &pebble.Options{
		EventListener: NewMetricsListener(meter),
		// 1) Absorber plus d'écritures avant flush => moins de SST, moins de compactions.
		MemTableSize:                256 << 20, // 256MB (à ajuster selon RAM)
		MemTableStopWritesThreshold: 4,         // défaut souvent 2; 4 réduit les write stalls

		// 2) Contrôler la pression L0 (source #1 de compactions/churn en write-heavy).
		L0CompactionThreshold: 8,         // déclenche plus tôt les compactions L0->L1 (évite l'emballement)
		L0StopWritesThreshold: 32,        // plus haut => moins de "stop-the-world writes" (au prix de plus de dette)
		LBaseMaxBytes:         512 << 20, // 512MB base level (augmente si dataset gros)

		// 3) Taille des tables: moins de petits fichiers => moins de compactions.
		// (à calibrer; 64MB est un bon départ)
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 64 << 20},
			{TargetFileSize: 64 << 20},
			{TargetFileSize: 64 << 20},
			{TargetFileSize: 64 << 20},
			{TargetFileSize: 64 << 20},
			{TargetFileSize: 64 << 20},
			{TargetFileSize: 64 << 20},
		},

		// 4) Lisser l'IO lors des flush/compactions.
		BytesPerSync:    1 << 20, // 1MB
		WALBytesPerSync: 1 << 20, // 1MB

		// 5) Concurrence compaction: OK mais pas trop haut (sinon tu satures l’IO).
		MaxConcurrentCompactions: func() int { return 2 }, // 2 ou 3 selon CPU/IO
	}

	var (
		db *pebble.DB
	)

	currentCheckpointRaw, err := os.ReadFile(filepath.Join(dataDir, currentCheckpointFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading current checkpoint: %w", err)
	}
	liveDir := filepath.Join(dataDir, liveDir)
	if errors.Is(err, os.ErrNotExist) {
		logger.Infof("No checkpoint found, creating new database in %s", liveDir)
		if err := os.RemoveAll(liveDir); err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}

		db, err = pebble.Open(liveDir, &pebble.Options{})
		if err != nil {
			return nil, fmt.Errorf("opening pebble database: %w", err)
		}

		if err := db.Checkpoint(filepath.Join(dataDir, checkpointsDir, "0")); err != nil {
			return nil, fmt.Errorf("creating initial checkpoint: %w", err)
		}

		f, err := os.Create(filepath.Join(dataDir, currentCheckpointFile))
		if err != nil {
			return nil, fmt.Errorf("creating current checkpoint file: %w", err)
		}

		if _, err := f.WriteString("0"); err != nil {
			return nil, fmt.Errorf("writing current checkpoint: %w", err)
		}

		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("syncing current checkpoint file: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("closing current checkpoint file: %w", err)
		}
	} else {
		logger.Infof("Checkpoint found, restoring from checkpoint %s to directory %s", string(currentCheckpointRaw), liveDir)
		if err := os.RemoveAll(liveDir); err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}

		if err := HardLink(filepath.Join(dataDir, checkpointsDir, string(currentCheckpointRaw)), liveDir); err != nil {
			return nil, fmt.Errorf("hard linking checkpoint: %w", err)
		}

		db, err = pebble.Open(liveDir, opts)
		if err != nil {
			return nil, fmt.Errorf("opening pebble database: %w", err)
		}
	}

	var currentCheckpoint uint64
	if len(currentCheckpointRaw) > 0 {
		currentCheckpoint, err = strconv.ParseUint(string(currentCheckpointRaw), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	return &Store{
		db:                db,
		logger:            logger.WithField("cmp", "pebble"),
		dataDir:           dataDir,
		currentCheckPoint: currentCheckpoint,
	}, nil
}

// Close closes the Pebble database
func (s *Store) Close(ctx context.Context) error {
	var errs []error
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing store: %v", errs)
	}
	return nil
}

// logCursor implements store.Cursor[*ledgerpb.Log] for Pebble.
type logCursor struct {
	iter *pebble.Iterator
	s    *Store
}

func (c *logCursor) Next(ctx context.Context) (*ledgerpb.Log, error) {
	if !c.iter.Valid() {
		if err := c.iter.Error(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	// Read protobuf Log
	value, err := c.iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading log value: %w", err)
	}

	// Unmarshal protobuf Log
	log := &ledgerpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	// Move to next
	c.iter.Next()

	return log, nil
}

func (c *logCursor) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger.
// Logs are returned in ascending order by id.
// from: optional log id to start from (0 = from beginning).
// to: optional log id to stop at (0 = until end, inclusive).
func (s *Store) GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	// Set up iterator bounds

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixLog)
	if from > 0 {
		writeInt64(buf, int64(from))
	}
	lowerBound := buf.Bytes()

	buf = bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixLog)
	if to > 0 {
		writeInt64(buf, int64(to+1))
	} else {
		if _, err := buf.Write([]byte{
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		}); err != nil {
			return nil, err
		}
	}
	upperBound := buf.Bytes()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}

	// Position iterator at first key
	iter.First()

	return &logCursor{
		iter: iter,
		s:    s,
	}, nil
}

// GetLogByID retrieves a log by its ID for a specific ledger.
func (s *Store) GetLogByID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error) {
	return s.GetLogWithID(ctx, ledger, id)
}

// GetLogWithID retrieves a log by its ID for a specific ledger.
func (s *Store) GetLogWithID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixLog)
	writeInt64(buf, int64(id))

	value, closer, err := s.db.Get(buf.Bytes())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting log by id: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Unmarshal protobuf Log
	log := &ledgerpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	return log, nil
}

// ============================================================================
// Store Implementation
// ============================================================================

// GetBalances retrieves balances from Pebble for a specific ledger (implements store.Store)
// Sums all balance diffs for each account/asset combination
func (s *Store) GetBalances(ctx context.Context, ledger string, balanceQuery map[string][]string) (ledgerpb.Balances, error) {
	result := make(ledgerpb.Balances)

	buf := bytes.NewBuffer(nil)

	// Build query for each account/asset combination
	for account, assets := range balanceQuery {
		if len(assets) == 0 {
			continue
		}
		buf.Reset()

		// Initialize account map
		result[account] = make(map[string]*big.Int)

		// Query each asset
		for _, asset := range assets {
			// Initialize balance to zero
			balance := big.NewInt(0)

			// Iterate over all balance diffs for this ledger/account/asset combination
			writeLedgerPrefix(buf, ledger)
			writeByte(buf, keyPrefixBalanceDiff)
			writeString(buf, account)
			writeString(buf, asset)
			lowerBound := buf.Bytes()

			writeByte(buf, 0xFF)
			upperBound := buf.Bytes()

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			})
			if err != nil {
				return nil, fmt.Errorf("creating iterator for balance diffs: %w", err)
			}

			// Sum all diffs
			for iter.First(); iter.Valid(); iter.Next() {
				valueBytes, err := iter.ValueAndErr()
				if err != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("reading balance diff value: %w", err)
				}

				diff := unmarshalBigInt(valueBytes)
				balance = balance.Add(balance, diff)
			}

			if err := iter.Close(); err != nil {
				return nil, fmt.Errorf("closing balance diff iterator: %w", err)
			}

			result[account][asset] = balance
		}
	}

	return result, nil
}

// GetAccountMetadata retrieves account metadata for multiple accounts from Pebble for a specific ledger (implements store.Store)
func (s *Store) GetAccountMetadata(ctx context.Context, ledger string, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)

	// Initialize with empty metadata for all requested accounts
	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	buf := bytes.NewBuffer(nil)

	// Iterate over all accounts and read their metadata
	for _, account := range accounts {
		buf.Reset()

		writeLedgerPrefix(buf, ledger)
		writeByte(buf, keyPrefixAccountMetadata)
		writeString(buf, account)
		lowerBound := buf.Bytes()

		writeByte(buf, 0xFF)
		upperBound := buf.Bytes()

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		})
		if err != nil {
			return nil, fmt.Errorf("creating iterator for ledger %s account %s: %w", ledger, account, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Extract metadata key from full key (format: "met{ledger}:{account}:{key}")
			metadataKeyBytes := key[len(lowerBound):]
			metadataKey := string(metadataKeyBytes)
			valueBytes, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("reading metadata value for ledger %s account %s key %s: %w", ledger, account, metadataKey, err)
			}

			// Metadata values are stored as strings directly
			valueStr := string(valueBytes)
			result[account][metadataKey] = valueStr
		}
		_ = iter.Close()
	}

	return result, nil
}

// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key for a specific ledger (implements store.Store)
func (s *Store) GetLogIDForIdempotencyKey(ctx context.Context, ledger string, idempotencyKey string) (uint64, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixIdempotency)
	writeString(buf, idempotencyKey)

	value, closer, err := s.db.Get(buf.Bytes())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("querying idempotency entry: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	return binary.BigEndian.Uint64(value[:8]), nil
}

// GetLogIDForTransactionID retrieves the log ID for a given transaction ID for a specific ledger (implements store.Store)
func (s *Store) GetLogIDForTransactionID(ctx context.Context, ledger string, transactionID uint64) (uint64, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixTransactionID)
	writeUInt64(buf, transactionID)

	value, closer, err := s.db.Get(buf.Bytes())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("querying transaction ID mapping: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	// Parse log ID from 8-byte big-endian value
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid log ID value length: expected 8 bytes, got %d", len(value))
	}
	logID := binary.BigEndian.Uint64(value)

	return logID, nil
}

// IsTransactionReverted checks if a transaction has been reverted for a specific ledger (implements store.Store)
func (s *Store) IsTransactionReverted(ctx context.Context, ledger string, transactionID uint64) (bool, error) {

	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, ledger)
	writeByte(buf, keyPrefixRevertedTxID)
	writeUInt64(buf, transactionID)

	_, closer, err := s.db.Get(buf.Bytes())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("querying reverted transaction ID: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	return true, nil
}

func (s *Store) CreateSnapshot(ctx context.Context) error {

	s.logger.Infof("Creating snapshot")

	checkpointDir := filepath.Join(s.dataDir, "checkpoints", fmt.Sprintf("%d", s.currentCheckPoint+1))
	if err := os.RemoveAll(checkpointDir); err != nil {
		return fmt.Errorf("removing checkpoint directory: %w", err)
	}

	if err := s.db.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("creating checkpoint: %w", err)
	}

	f, err := os.Create(filepath.Join(s.dataDir, currentCheckpointFile+".tmp"))
	if err != nil {
		return fmt.Errorf("creating checkpoint file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := f.WriteString(fmt.Sprintf("%d", s.currentCheckPoint+1)); err != nil {
		return fmt.Errorf("writing checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing checkpoint file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing checkpoint file: %w", err)
	}

	if err := os.Rename(filepath.Join(s.dataDir, currentCheckpointFile+".tmp"), filepath.Join(s.dataDir, currentCheckpointFile)); err != nil {
		return fmt.Errorf("renaming checkpoint file: %w", err)
	}

	// todo: it can fail, leaving an old checkpoint on disk
	// this is not critical, but we should fix it eventually
	if err := os.RemoveAll(filepath.Join(s.dataDir, "checkpoints", fmt.Sprintf("%d", s.currentCheckPoint))); err != nil {
		return fmt.Errorf("removing old checkpoint directory: %w", err)
	}

	s.logger.WithFields(map[string]any{
		"checkpoint": s.currentCheckPoint + 1,
	}).Infof("Snapshot created")
	s.currentCheckPoint++

	return nil
}

func (s *Store) GetLastAppliedIndex() (uint64, error) {
	get, closer, err := s.db.Get([]byte{keyPrefixLastAppliedIndex})
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

func (s *Store) DeleteLedger(_ context.Context, name string) error {
	startBuf := bytes.NewBuffer(nil)
	writeLedgerPrefix(startBuf, name)

	endBuf := bytes.NewBuffer(nil)
	writeLedgerPrefix(endBuf, name)
	writeByte(endBuf, 0xFF)

	return s.db.DeleteRange(startBuf.Bytes(), endBuf.Bytes(), pebble.NoSync)
}

func (s *Store) GetLastLogID(ctx context.Context, name string) (uint64, error) {
	buf := bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, name)
	writeByte(buf, keyPrefixLog)
	lowerBound := buf.Bytes()

	buf = bytes.NewBuffer(nil)
	writeLedgerPrefix(buf, name)
	writeByte(buf, keyPrefixLog)
	if _, err := buf.Write([]byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}); err != nil {
		return 0, err
	}
	upperBound := buf.Bytes()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Seek to the last key in the range
	if !iter.Last() {
		// No logs found for this ledger
		return 0, nil
	}

	// Parse the log ID from the value (the log protobuf contains the ID)
	value, err := iter.ValueAndErr()
	if err != nil {
		return 0, fmt.Errorf("reading log value: %w", err)
	}

	log := &ledgerpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return 0, fmt.Errorf("unmarshaling log from protobuf: %w", err)
	}

	return log.Id, nil
}

// Metrics returns Pebble database metrics (implements MetricsAware)
func (s *Store) Metrics() any {
	return s.db.Metrics()
}

func writeLedgerPrefix(buf *bytes.Buffer, ledger string) {
	if _, err := buf.WriteString(ledger); err != nil {
		panic(err)
	}
	if err := buf.WriteByte('/'); err != nil {
		panic(err)
	}
}

func writeUInt64(buf *bytes.Buffer, value uint64) {
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(buf *bytes.Buffer, value int64) {
	if err := binary.Write(buf, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeString(buf *bytes.Buffer, value string) {
	if _, err := buf.WriteString(value); err != nil {
		panic(err)
	}
}

func writeByte(buf *bytes.Buffer, value byte) {
	if err := buf.WriteByte(value); err != nil {
		panic(err)
	}
}

func setOnBatch(batch *pebble.Batch, buf *bytes.Buffer, value []byte) error {
	if err := batch.Set(buf.Bytes(), value, pebble.NoSync); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	buf.Reset()

	return nil
}

func deleteOnBatch(batch *pebble.Batch, buf *bytes.Buffer) error {
	if err := batch.Delete(buf.Bytes(), pebble.NoSync); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	buf.Reset()

	return nil
}

func marshalBigInt(x *big.Int) []byte {
	if x == nil {
		return []byte{0} // convention: nil => 0
	}
	sign := byte(0)
	if x.Sign() < 0 {
		sign = 1
	}
	mag := new(big.Int).Abs(x).Bytes()
	out := make([]byte, 1+len(mag))
	out[0] = sign
	copy(out[1:], mag)
	return out
}

func unmarshalBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	sign := b[0]
	x := new(big.Int).SetBytes(b[1:])
	if sign == 1 && x.Sign() != 0 {
		x.Neg(x)
	}
	return x
}

func HardLink(srcDir, dstDir string) error {
	srcDir = filepath.Clean(srcDir)
	dstDir = filepath.Clean(dstDir)

	srcInfo, err := os.Stat(srcDir)
	if err != nil {
		return fmt.Errorf("stat srcDir: %w", err)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("srcDir is not a directory: %s", srcDir)
	}
	if _, err := os.Stat(dstDir); err == nil {
		return fmt.Errorf("dstDir already exists: %s", dstDir)
	}

	parent := filepath.Dir(dstDir)
	base := filepath.Base(dstDir)
	tmpDir := filepath.Join(parent, base+fmt.Sprintf(".tmp-%d-%d", os.Getpid(), time.Now().UnixNano()))

	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Create tmp root with same perms as source root (best effort).
	if err := os.MkdirAll(tmpDir, srcInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("mkdir tmpDir: %w", err)
	}
	if err := fsyncDir(tmpDir); err != nil {
		return err
	}

	err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		dstPath := filepath.Join(tmpDir, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}

		switch {
		case info.IsDir():
			if err := os.MkdirAll(dstPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("mkdir %s: %w", dstPath, err)
			}
			_ = os.Chmod(dstPath, info.Mode().Perm())
			return fsyncDir(dstPath)

		case info.Mode().Type() == 0: // regular file
			if err := os.Link(path, dstPath); err != nil {
				return fmt.Errorf("hardlink %s -> %s: %w", path, dstPath, err)
			}
			_ = os.Chmod(dstPath, info.Mode().Perm())
			return nil

		case (info.Mode() & os.ModeSymlink) != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", path, err)
			}
			if err := os.Symlink(target, dstPath); err != nil {
				return fmt.Errorf("symlink %s -> %s: %w", dstPath, target, err)
			}
			return nil

		default:
			return fmt.Errorf("unsupported file type %s mode=%s", path, info.Mode().String())
		}
	})
	if err != nil {
		return fmt.Errorf("walk: %w", err)
	}

	if err := fsyncDir(tmpDir); err != nil {
		return err
	}
	if err := fsyncDir(parent); err != nil {
		return err
	}

	if err := os.Rename(tmpDir, dstDir); err != nil {
		return fmt.Errorf("rename publish: %w", err)
	}

	return fsyncDir(parent)
}

// fsyncDir fsyncs a directory so its entries are durably recorded. On Windows, it's a no-op.
func fsyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync %s: %w", dir, err)
	}
	defer func() {
		_ = f.Close()
	}()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

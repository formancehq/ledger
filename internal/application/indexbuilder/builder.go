package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// DefaultBatchSize is the default number of log entries per bbolt write
// transaction. Can be overridden via --read-index-batch-size.
const DefaultBatchSize = 1000

// Proposer proposes Raft commands to the cluster.
// Implemented by a thin adapter around *node.Node (bootstrap.NodeProposer).
type Proposer interface {
	ProposeOrders(orders ...*raftcmdpb.Order) error
}

// indexID identifies a specific index (transaction, account, or log builtin).
type indexID struct {
	transaction *commonpb.TransactionIndex // set for transaction indexes (builtin or metadata)
	account     *commonpb.AccountIndex     // set for account indexes (builtin or metadata)
	logBuiltin  *commonpb.LogBuiltinIndex  // set for log builtin indexes
}

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger string
	index  indexID
	cursor uint64 // current position (persisted in bbolt)
	bbKey  []byte // precomputed bbolt key for progress persistence
}

// ledgerIndexConfig caches which indexes are enabled and ready for a ledger.
type ledgerIndexConfig struct {
	txMetadataIndexed   map[string]bool                           // transaction metadata key → indexed
	txBuiltinIndexed    map[commonpb.TransactionBuiltinIndex]bool // transaction builtin → indexed
	acctMetadataIndexed map[string]bool                           // account metadata key → indexed
	acctBuiltinIndexed  map[commonpb.AccountBuiltinIndex]bool     // account builtin → indexed
	logBuiltinIndexed   map[commonpb.LogBuiltinIndex]bool         // log builtin → indexed
}

// Builder tails the system log and populates the bbolt read store indexes.
// It runs as a background goroutine on ALL nodes (not just the leader).
// Progress is stored locally in bbolt (no Raft needed).
//
// When a new index is created, the builder also backfills historical data.
// Only the leader proposes IndexReady through Raft when backfill completes.
type Builder struct {
	pebbleStore   *dal.Store
	readStore     *readstore.Store
	logger        logging.Logger
	meter         metric.Meter
	notifications *signal.Notifications
	proposer      Proposer
	isLeader      func() bool
	w             worker.Worker

	lastIndexedSeq      atomic.Uint64
	pebbleLastSeq       atomic.Uint64
	logsIndexed         atomic.Uint64
	metricsRegistration metric.Registration

	// Per-ledger index configuration cache.
	indexConfig map[string]*ledgerIndexConfig

	// Active backfill tasks for BUILDING indexes.
	backfillTasks []*backfillTask

	// backfillMode is set to true during backfill log replay to skip
	// existence writes (already written by normal processing).
	backfillMode bool

	// Batch size for normal index processing and backfill.
	batchSize int

	// Reusable scratch objects to reduce allocations in the hot loop.
	kb       *dal.KeyBuilder
	wb       *readstore.WriteBatch
	accounts map[string]struct{}
}

// NewBuilder creates a new index builder.
// batchSize controls how many log entries are buffered per bbolt write transaction.
// Use 0 for the default (DefaultBatchSize).
func NewBuilder(
	pebbleStore *dal.Store,
	readStore *readstore.Store,
	logger logging.Logger,
	meter metric.Meter,
	batchSize int,
) *Builder {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &Builder{
		pebbleStore: pebbleStore,
		readStore:   readStore,
		logger:      logger.WithFields(map[string]any{"cmp": "index-builder"}),
		meter:       meter,
		batchSize:   batchSize,
		indexConfig: make(map[string]*ledgerIndexConfig),
		kb:          dal.NewKeyBuilder(),
		wb:          readstore.NewWriteBatch(),
		accounts:    make(map[string]struct{}, 64),
	}
}

// initIndexConfig scans all ledgers from Pebble and populates the index config cache.
// It also detects BUILDING indexes and creates backfill tasks, loading persisted
// cursors from bbolt so backfills survive restarts.
func (b *Builder) initIndexConfig() {
	handle := b.pebbleStore.NewReadHandle()

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLedgers(context.Background(), handle)
	if err != nil {
		b.logger.Errorf("Failed to read ledgers for index config: %v", err)

		return
	}

	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			b.logger.Errorf("Error reading ledger info: %v", err)

			return
		}

		if info.GetDeletedAt() != nil {
			continue
		}

		b.loadLedgerIndexConfig(info)
	}

	// Load persisted backfill progress from bbolt.
	if len(b.backfillTasks) > 0 {
		_ = b.readStore.View(func(tx *bolt.Tx) error {
			for _, task := range b.backfillTasks {
				if c, ok := b.readStore.ReadBackfillProgress(tx, task.bbKey); ok {
					task.cursor = c
				}
			}

			return nil
		})
		b.logger.WithFields(map[string]any{
			"count": len(b.backfillTasks),
		}).Infof("Loaded backfill tasks for BUILDING indexes")
	}
}

// loadLedgerIndexConfig populates the index config cache for a single ledger.
// Both READY and BUILDING indexes are included so that normal processing writes
// to new indexes immediately (covering logs after CreateIndex).
func (b *Builder) loadLedgerIndexConfig(info *commonpb.LedgerInfo) {
	cfg := newLedgerIndexConfig()

	// Metadata indexes — include both READY and BUILDING.
	if info.GetMetadataSchema() != nil {
		b.loadMetadataIndexes(cfg, info.GetName(), commonpb.TargetType_TARGET_TYPE_ACCOUNT, info.GetMetadataSchema().GetAccountFields())
		b.loadMetadataIndexes(cfg, info.GetName(), commonpb.TargetType_TARGET_TYPE_TRANSACTION, info.GetMetadataSchema().GetTransactionFields())
	}

	// Builtin transaction indexes (including address indexes) — include both READY and BUILDING.
	if bi := info.GetBuiltinIndexes(); bi != nil {
		for _, entry := range []struct {
			index   commonpb.TransactionBuiltinIndex
			enabled bool
			status  commonpb.IndexBuildStatus
		}{
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, bi.GetReference(), bi.GetReferenceStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, bi.GetTimestamp(), bi.GetTimestampStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS, bi.GetAddress(), bi.GetAddressStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS, bi.GetSourceAddress(), bi.GetSourceAddressStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS, bi.GetDestAddress(), bi.GetDestAddressStatus()},
		} {
			if !entry.enabled {
				continue
			}

			cfg.txBuiltinIndexed[entry.index] = true
			if entry.status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForTxBuiltin(info.GetName(), entry.index)
			}
		}
	}

	// Builtin log indexes — include both READY and BUILDING.
	if li := info.GetLogBuiltinIndexes(); li != nil {
		for _, entry := range []struct {
			index   commonpb.LogBuiltinIndex
			enabled bool
			status  commonpb.IndexBuildStatus
		}{
			{commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER, li.GetLedger(), li.GetLedgerStatus()},
		{commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, li.GetDate(), li.GetDateStatus()},
		} {
			if !entry.enabled {
				continue
			}

			cfg.logBuiltinIndexed[entry.index] = true
			if entry.status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForLogBuiltin(info.GetName(), entry.index)
			}
		}
	}

	b.indexConfig[info.GetName()] = cfg
}

// loadMetadataIndexes loads metadata indexes for a given target type.
func (b *Builder) loadMetadataIndexes(
	cfg *ledgerIndexConfig,
	ledger string,
	target commonpb.TargetType,
	fields map[string]*commonpb.MetadataFieldSchema,
) {
	for key, field := range fields {
		if !field.GetIndexed() {
			continue
		}

		switch target {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			cfg.acctMetadataIndexed[key] = true
			if field.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForAcctMetadata(ledger, key)
			}
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			cfg.txMetadataIndexed[key] = true
			if field.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForTxMetadata(ledger, key)
			}
		}
	}
}

// isMetadataIndexed checks if a specific metadata index is enabled and ready.
func (b *Builder) isMetadataIndexed(ledger string, target commonpb.TargetType, key string) bool {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		return false
	}

	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return cfg.acctMetadataIndexed[key]
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return cfg.txMetadataIndexed[key]
	default:
		return false
	}
}

// isBuiltinIndexed checks if a specific builtin index is enabled.
func (b *Builder) isBuiltinIndexed(ledger string, index commonpb.TransactionBuiltinIndex) bool {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		return false
	}

	return cfg.txBuiltinIndexed[index]
}

// isLogBuiltinIndexed checks if a specific log builtin index is enabled.
func (b *Builder) isLogBuiltinIndexed(ledger string, index commonpb.LogBuiltinIndex) bool {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		return false
	}

	return cfg.logBuiltinIndexed[index]
}

// SetNotifications sets the dedicated Notifications signal for the builder.
func (b *Builder) SetNotifications(n *signal.Notifications) {
	b.notifications = n
}

// SetProposer sets the Raft proposer and leader check function.
// Must be called before Start.
func (b *Builder) SetProposer(p Proposer, isLeader func() bool) {
	b.proposer = p
	b.isLeader = isLeader
}

// Start begins the background index-building loop and registers OTEL metrics.
func (b *Builder) Start() {
	if reg, err := b.registerMetrics(); err == nil {
		b.metricsRegistration = reg
	}

	b.w = worker.New()
	b.w.Run(b.loop)
}

// Stop gracefully stops the background loop and unregisters OTEL metrics.
func (b *Builder) Stop() {
	b.w.Stop()

	if b.metricsRegistration != nil {
		_ = b.metricsRegistration.Unregister()
	}
}

// LastIndexedSequence returns the last indexed sequence (from the atomic cache).
func (b *Builder) LastIndexedSequence() uint64 {
	return b.lastIndexedSeq.Load()
}

// PebbleLastSequence returns the last known Pebble sequence (from the atomic cache).
func (b *Builder) PebbleLastSequence() uint64 {
	return b.pebbleLastSeq.Load()
}

// registerMetrics registers observable gauges for index builder progress.
func (b *Builder) registerMetrics() (metric.Registration, error) {
	lastIndexedGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.last_indexed_sequence",
		metric.WithDescription("Last log sequence indexed in bbolt"),
	)
	if err != nil {
		return nil, err
	}

	pebbleLastGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.pebble_last_sequence",
		metric.WithDescription("Last log sequence in Pebble"),
	)
	if err != nil {
		return nil, err
	}

	lagGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.lag",
		metric.WithDescription("Number of logs the index builder is behind Pebble"),
	)
	if err != nil {
		return nil, err
	}

	logsIndexedGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.logs_indexed_total",
		metric.WithDescription("Total number of logs indexed since process start"),
	)
	if err != nil {
		return nil, err
	}

	return b.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			indexed := int64(b.lastIndexedSeq.Load())
			pebbleLast := int64(b.pebbleLastSeq.Load())

			lag := max(pebbleLast-indexed, 0)

			o.ObserveInt64(lastIndexedGauge, indexed)
			o.ObserveInt64(pebbleLastGauge, pebbleLast)
			o.ObserveInt64(lagGauge, lag)
			o.ObserveInt64(logsIndexedGauge, int64(b.logsIndexed.Load()))

			return nil
		},
		lastIndexedGauge,
		pebbleLastGauge,
		lagGauge,
		logsIndexedGauge,
	)
}

func (b *Builder) loop(stop <-chan struct{}) {
	// Initialize index config cache from Pebble before processing any logs.
	b.initIndexConfig()

	cursor, err := b.readStore.LastIndexedSequence()
	if err != nil {
		b.logger.Errorf("Failed to read progress: %v", err)

		return
	}

	b.lastIndexedSeq.Store(cursor)

	// Seed pebble last sequence.
	if pebbleLast, err := query.ReadLastSequence(b.pebbleStore); err == nil {
		b.pebbleLastSeq.Store(pebbleLast)
	}

	b.logger.WithFields(map[string]any{"cursor": cursor}).Infof("Index builder started")

	// Initial catch-up
	if cursor, err = b.processLogs(cursor); err != nil {
		b.logger.Errorf("Error during initial catch-up: %v", err)
	}

	b.processBackfills(stop, cursor)

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

		// Fast path: skip Pebble iterator + bbolt transaction when the FSM
		// hasn't advanced past our cursor.
		if cached := b.notifications.LastSequence.Load(); cached == 0 || cached > cursor {
			if cursor, err = b.processLogs(cursor); err != nil {
				b.logger.Errorf("Error processing logs: %v", err)
			}
		}

		b.processBackfills(stop, cursor)
	}
}

// processLogs reads logs from Pebble starting after the given cursor,
// indexes them in batches of indexBatchSize within a single bbolt
// transaction to amortize fsync overhead. Logs are consumed on the fly
// (no intermediate slice) so the proto object can be GC'd immediately.
func (b *Builder) processLogs(cursor uint64) (uint64, error) {
	logsCursor, err := query.ReadLogsSince(context.Background(), b.pebbleStore, cursor, dal.WithReuse())
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	for {
		var (
			batchCount int
			lastSeq    uint64
			eof        bool
		)

		// Open one bbolt write tx and stream up to batchSize logs into it.
		// Writes are buffered in a WriteBatch and flushed sorted by key to
		// minimize random B+ tree page access.
		if err := b.readStore.Update(func(tx *bolt.Tx) error {
			b.wb.Init(tx)

			for batchCount < b.batchSize {
				log, err := logsCursor.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						eof = true

						break
					}

					return err
				}

				if err := b.indexLogEntry(tx, log); err != nil {
					return err
				}

				lastSeq = log.GetSequence()
				batchCount++
			}

			if batchCount > 0 {
				if err := b.wb.Flush(); err != nil {
					return err
				}

				if err := b.readStore.WriteProgress(tx, lastSeq); err != nil {
					return err
				}

				// Look up the raft index that corresponds to this sequence
				// and persist it in bbolt so cross-store queries can cap
				// Pebble reads at bbolt's progress level.
				raftIdx, err := query.ReadRaftIndexForSequence(b.pebbleStore, lastSeq)
				if err != nil {
					return fmt.Errorf("reading raft index for sequence %d: %w", lastSeq, err)
				}

				if raftIdx > 0 {
					if err := b.readStore.WriteRaftIndexProgress(tx, raftIdx); err != nil {
						return err
					}
				}

				return nil
			}

			return nil
		}); err != nil {
			b.logger.WithFields(map[string]any{
				"batchSize": batchCount,
				"lastSeq":   lastSeq,
				"error":     err,
			}).Errorf("Error processing batch")

			return cursor, err
		}

		if batchCount == 0 {
			break
		}

		cursor = lastSeq
		b.lastIndexedSeq.Store(cursor)
		b.logsIndexed.Add(uint64(batchCount))
		b.readStore.NotifyProgress()

		// Sample pebble last sequence from the cached atomic (written by the FSM
		// before signalling LogCommitted). This avoids opening a Pebble iterator
		// and deserializing a protobuf just to read a counter.
		if cached := b.notifications.LastSequence.Load(); cached > 0 {
			b.pebbleLastSeq.Store(cached)
		}

		if eof {
			break
		}
	}

	return cursor, nil
}

// RebuildAll replays all system logs from scratch (starting at sequence 0),
// rebuilding the entire read index. This is intended for offline use
// (CLI backfill). Returns the last processed log sequence.
func (b *Builder) RebuildAll() (uint64, error) {
	return b.processLogs(0)
}

// indexLogEntry dispatches a single log entry to the appropriate index handler.
// It does NOT call WriteProgress — the caller batches that.
func (b *Builder) indexLogEntry(tx *bolt.Tx, log *commonpb.Log) error {
	if log.GetPayload() == nil {
		return nil
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		return nil
	}

	ledgerName := applyLog.Apply.GetLedgerName()

	ledgerLog := applyLog.Apply.GetLog()
	if ledgerLog == nil || ledgerLog.GetData() == nil {
		return nil
	}

	// Index ledger log for per-ledger listing (opt-in via log builtin index).
	if b.isLogBuiltinIndexed(ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER) {
		b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence())
		b.wb.WriteLogExistence(b.kb, ledgerName, ledgerLog.GetId())
	}

	// Index log date for date range filtering (opt-in via log date builtin index).
	if b.isLogBuiltinIndexed(ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
		b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId())
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(tx, b.kb, ledgerName, p.CreatedTransaction)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(tx, b.kb, ledgerName, p.RevertedTransaction)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(tx, b.kb, ledgerName, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(tx, b.kb, ledgerName, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema changes scan buckets directly with cursors — flush buffered
		// writes first so the cursors see a consistent state.
		if err := b.wb.Flush(); err != nil {
			return err
		}

		b.wb.Init(tx) // re-init after flush (Flush calls Reset)

		return b.indexSetMetadataFieldType(tx, b.kb, ledgerName, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreateIndexLog(ledgerName, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		b.handleDropIndexLog(ledgerName, p.DropIndex)
	case *commonpb.LedgerLogPayload_IndexReady:
		b.handleIndexReadyLog(ledgerName, p.IndexReady)
	}

	return nil
}

// indexCreatedTransaction handles CreatedTransaction logs by indexing:
// - transaction existence
// - account existence (for all accounts in postings + account_metadata)
// - account metadata (from account_metadata)
// - transaction metadata (from transaction.metadata)
// - account→transaction mapping.
func (b *Builder) indexCreatedTransaction(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	ledger string,
	ct *commonpb.CreatedTransaction,
) error {
	if ct.GetTransaction() == nil {
		return nil
	}

	txn := ct.GetTransaction()

	wb := b.wb

	// Transaction existence (skip during backfill — already written by normal processing)
	if !b.backfillMode {
		wb.WriteTransactionExistence(kb, ledger, txn.GetId())
	}

	// Collect unique accounts from postings (reuse builder's map)
	indexAny := b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	indexSrc := b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
	indexDst := b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS)

	clear(b.accounts)

	for _, posting := range txn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		// Account→transaction mapping (any role)
		if indexAny {
			wb.WriteAccountTxMapping(kb, ledger, posting.GetSource(), txn.GetId())
			wb.WriteAccountTxMapping(kb, ledger, posting.GetDestination(), txn.GetId())
		}
		// Role-specific mappings
		if indexSrc {
			wb.WriteSourceAccountTxMapping(kb, ledger, posting.GetSource(), txn.GetId())
		}

		if indexDst {
			wb.WriteDestAccountTxMapping(kb, ledger, posting.GetDestination(), txn.GetId())
		}
	}

	// Account existence for all accounts in postings (skip during backfill)
	if !b.backfillMode {
		for account := range b.accounts {
			wb.WriteAccountExistence(kb, ledger, account)
		}
	}

	// Account existence + metadata from account_metadata map
	for account, metadataSet := range ct.GetAccountMetadata() {
		if !b.backfillMode {
			wb.WriteAccountExistence(kb, ledger, account)
		}

		if metadataSet != nil {
			for _, md := range metadataSet.GetMetadata() {
				if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, md.GetKey()) {
					continue
				}

				reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.GetKey())
				encodedValue := readstore.EncodeMetadataValue(nil, md.GetValue())
				wb.UpdateMetadataIndex(
					kb, reverseKey,
					ledger, readstore.NamespaceAccount, md.GetKey(),
					encodedValue, []byte(account),
				)
			}
		}
	}

	// Transaction metadata
	if txn.GetMetadata() != nil {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.GetId())
		for _, md := range txn.GetMetadata().GetMetadata() {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.GetKey()) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txn.GetId(), md.GetKey())
			encodedValue := readstore.EncodeMetadataValue(nil, md.GetValue())
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.GetKey(),
				encodedValue, txIDBytes,
			)
		}
	}

	// Builtin indexes
	if b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE) && txn.GetReference() != "" {
		wb.WriteTransactionReferenceIndex(kb, ledger, txn.GetReference(), txn.GetId())
	}

	if b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP) && txn.GetTimestamp() != nil {
		wb.WriteTransactionTimestampIndex(kb, ledger, txn.GetTimestamp().GetData(), txn.GetId())
	}

	return nil
}

// indexRevertedTransaction handles RevertedTransaction logs by indexing:
// - revert transaction existence
// - account existence for revert postings
// - account→transaction mapping for revert postings.
func (b *Builder) indexRevertedTransaction(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	ledger string,
	rt *commonpb.RevertedTransaction,
) error {
	if rt.GetRevertTransaction() == nil {
		return nil
	}

	revertTxn := rt.GetRevertTransaction()
	wb := b.wb

	// Revert transaction existence (skip during backfill)
	if !b.backfillMode {
		wb.WriteTransactionExistence(kb, ledger, revertTxn.GetId())
	}

	// Account existence + account→tx mapping for revert postings (reuse builder's map)
	indexAny := b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	indexSrc := b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
	indexDst := b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS)

	clear(b.accounts)

	for _, posting := range revertTxn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}

		b.accounts[posting.GetDestination()] = struct{}{}
		if indexAny {
			wb.WriteAccountTxMapping(kb, ledger, posting.GetSource(), revertTxn.GetId())
			wb.WriteAccountTxMapping(kb, ledger, posting.GetDestination(), revertTxn.GetId())
		}
		// Role-specific mappings
		if indexSrc {
			wb.WriteSourceAccountTxMapping(kb, ledger, posting.GetSource(), revertTxn.GetId())
		}

		if indexDst {
			wb.WriteDestAccountTxMapping(kb, ledger, posting.GetDestination(), revertTxn.GetId())
		}
	}

	if !b.backfillMode {
		for account := range b.accounts {
			wb.WriteAccountExistence(kb, ledger, account)
		}
	}

	// Transaction metadata for the revert transaction
	if revertTxn.GetMetadata() != nil {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.GetId())
		for _, md := range revertTxn.GetMetadata().GetMetadata() {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.GetKey()) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, revertTxn.GetId(), md.GetKey())
			encodedValue := readstore.EncodeMetadataValue(nil, md.GetValue())
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.GetKey(),
				encodedValue, txIDBytes,
			)
		}
	}

	// Builtin indexes (no reference on revert transactions)
	if b.isBuiltinIndexed(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP) && revertTxn.GetTimestamp() != nil {
		wb.WriteTransactionTimestampIndex(kb, ledger, revertTxn.GetTimestamp().GetData(), revertTxn.GetId())
	}

	return nil
}

// indexSavedMetadata handles SavedMetadata logs.
func (b *Builder) indexSavedMetadata(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	ledger string,
	sm *commonpb.SavedMetadata,
) error {
	if sm.GetTarget() == nil || sm.GetMetadata() == nil {
		return nil
	}

	wb := b.wb

	switch t := sm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		account := t.Account.GetAddr()

		for _, md := range sm.GetMetadata().GetMetadata() {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, md.GetKey()) {
				continue
			}

			reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.GetKey())
			encodedValue := readstore.EncodeMetadataValue(nil, md.GetValue())
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceAccount, md.GetKey(),
				encodedValue, []byte(account),
			)
		}
	case *commonpb.Target_Transaction:
		txID := t.Transaction.GetId()
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)

		for _, md := range sm.GetMetadata().GetMetadata() {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.GetKey()) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, md.GetKey())
			encodedValue := readstore.EncodeMetadataValue(nil, md.GetValue())
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.GetKey(),
				encodedValue, txIDBytes,
			)
		}
	}

	return nil
}

// indexDeletedMetadata handles DeletedMetadata logs.
func (b *Builder) indexDeletedMetadata(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	ledger string,
	dm *commonpb.DeletedMetadata,
) error {
	if dm.GetTarget() == nil {
		return nil
	}

	wb := b.wb

	switch t := dm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, dm.GetKey()) {
			return nil
		}

		account := t.Account.GetAddr()
		reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, dm.GetKey())
		wb.DeleteMetadataEntry(
			kb, reverseKey,
			ledger, readstore.NamespaceAccount, dm.GetKey(),
			[]byte(account),
		)
	case *commonpb.Target_Transaction:
		if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, dm.GetKey()) {
			return nil
		}

		txID := t.Transaction.GetId()
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, dm.GetKey())
		wb.DeleteMetadataEntry(
			kb, reverseKey,
			ledger, readstore.NamespaceTransaction, dm.GetKey(),
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
	kb *dal.KeyBuilder,
	ledger string,
	smft *commonpb.SetMetadataFieldTypeLog,
) error {
	// Only re-encode if this metadata key is indexed.
	if !b.isMetadataIndexed(ledger, smft.GetTargetType(), smft.GetKey()) {
		return nil
	}

	var ns string

	switch smft.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		ns = readstore.NamespaceAccount
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		ns = readstore.NamespaceTransaction
	default:
		return nil
	}

	midxBucket := tx.Bucket(readstore.BucketMetadataIndex)
	eidxBucket := tx.Bucket(readstore.BucketEntityExists)
	rmapBucket := tx.Bucket(readstore.BucketReverseMap)

	// Iterate the reverse map for this namespace to find all entities with the key.
	rmapPrefix := kb.Reset().
		PutLedgerName(ledger).
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
		if metaKey != smft.GetKey() {
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
				"key":   smft.GetKey(),
				"error": err,
			}).Errorf("Failed to unmarshal reverse map value during schema change")

			continue
		}

		// Delete old forward index entry
		oldEncoded := readstore.EncodeMetadataValue(nil, oldMV)

		oldKey := readstore.MetadataIndexKey(kb, ledger, ns, smft.GetKey(), oldEncoded, e.entityID)
		if err := midxBucket.Delete(oldKey); err != nil {
			return err
		}

		// Convert to new type
		newMV := commonpb.ConvertMetadataValue(oldMV, smft.GetType())
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// Update eidx if null status changed
		oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull

		newIsNull := len(newEncoded) > 0 && newEncoded[0] == readstore.TypeTagNull
		if oldIsNull != newIsNull {
			oldEidxKey := readstore.EntityExistsKey(kb, ledger, ns, smft.GetKey(), oldIsNull, e.entityID)
			if err := eidxBucket.Delete(oldEidxKey); err != nil {
				return err
			}

			newEidxKey := readstore.EntityExistsKey(kb, ledger, ns, smft.GetKey(), newIsNull, e.entityID)
			if err := eidxBucket.Put(newEidxKey, nil); err != nil {
				return err
			}
		}

		// Write new forward index entry
		newKey := readstore.MetadataIndexKey(kb, ledger, ns, smft.GetKey(), newEncoded, e.entityID)
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
// For transactions: [ledger\x00][t:][txID(8B)][metadataKey].
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

// handleCreateIndexLog updates the index config cache when a CreateIndex log is processed.
// The index starts in BUILDING state — it is NOT marked as ready here.
// A backfill task is created to replay historical logs for the new index.
func (b *Builder) handleCreateIndexLog(ledger string, log *commonpb.CreateIndexLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)

	switch idx := log.GetIndex().(type) {
	case *commonpb.CreateIndexLog_Transaction:
		switch txIdx := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			cfg.txBuiltinIndexed[txIdx.Builtin] = true
			b.addBackfillTaskForTxBuiltin(ledger, txIdx.Builtin)
		case *commonpb.TransactionIndex_MetadataKey:
			cfg.txMetadataIndexed[txIdx.MetadataKey] = true
			b.addBackfillTaskForTxMetadata(ledger, txIdx.MetadataKey)
		}
	case *commonpb.CreateIndexLog_Account:
		switch acctIdx := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			cfg.acctBuiltinIndexed[acctIdx.Builtin] = true
			// No backfill function for account builtins yet — add when needed.
		case *commonpb.AccountIndex_MetadataKey:
			cfg.acctMetadataIndexed[acctIdx.MetadataKey] = true
			b.addBackfillTaskForAcctMetadata(ledger, acctIdx.MetadataKey)
		}
	case *commonpb.CreateIndexLog_LogBuiltin:
		cfg.logBuiltinIndexed[idx.LogBuiltin] = true
		b.addBackfillTaskForLogBuiltin(ledger, idx.LogBuiltin)
	}
}

// handleDropIndexLog updates the index config cache when a DropIndex log is processed.
// It also removes any active backfill task for the dropped index.
func (b *Builder) handleDropIndexLog(ledger string, log *commonpb.DropIndexLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)

	switch idx := log.GetIndex().(type) {
	case *commonpb.DropIndexLog_Transaction:
		switch txIdx := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			delete(cfg.txBuiltinIndexed, txIdx.Builtin)
			b.removeBackfillTask(indexID{transaction: idx.Transaction})
		case *commonpb.TransactionIndex_MetadataKey:
			delete(cfg.txMetadataIndexed, txIdx.MetadataKey)
			b.removeBackfillTask(indexID{transaction: idx.Transaction})
		}
	case *commonpb.DropIndexLog_Account:
		switch acctIdx := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			delete(cfg.acctBuiltinIndexed, acctIdx.Builtin)
			b.removeBackfillTask(indexID{account: idx.Account})
		case *commonpb.AccountIndex_MetadataKey:
			delete(cfg.acctMetadataIndexed, acctIdx.MetadataKey)
			b.removeBackfillTask(indexID{account: idx.Account})
		}
	case *commonpb.DropIndexLog_LogBuiltin:
		delete(cfg.logBuiltinIndexed, idx.LogBuiltin)
		b.removeBackfillTask(indexID{logBuiltin: &idx.LogBuiltin})
	}
}

// handleIndexReadyLog updates the index config cache when an IndexReady log is processed.
// This marks the index as READY and removes any residual backfill task.
func (b *Builder) handleIndexReadyLog(ledger string, log *commonpb.IndexReadyLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)

	switch idx := log.GetIndex().(type) {
	case *commonpb.IndexReadyLog_Transaction:
		switch txIdx := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			cfg.txBuiltinIndexed[txIdx.Builtin] = true
		case *commonpb.TransactionIndex_MetadataKey:
			cfg.txMetadataIndexed[txIdx.MetadataKey] = true
		}

		b.removeBackfillTask(indexID{transaction: idx.Transaction})
	case *commonpb.IndexReadyLog_Account:
		switch acctIdx := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			cfg.acctBuiltinIndexed[acctIdx.Builtin] = true
		case *commonpb.AccountIndex_MetadataKey:
			cfg.acctMetadataIndexed[acctIdx.MetadataKey] = true
		}

		b.removeBackfillTask(indexID{account: idx.Account})
	case *commonpb.IndexReadyLog_LogBuiltin:
		cfg.logBuiltinIndexed[idx.LogBuiltin] = true
		b.removeBackfillTask(indexID{logBuiltin: &idx.LogBuiltin})
	}
}

// newLedgerIndexConfig creates a new ledgerIndexConfig with all maps initialized.
func newLedgerIndexConfig() *ledgerIndexConfig {
	return &ledgerIndexConfig{
		txMetadataIndexed:   make(map[string]bool),
		txBuiltinIndexed:    make(map[commonpb.TransactionBuiltinIndex]bool),
		acctMetadataIndexed: make(map[string]bool),
		acctBuiltinIndexed:  make(map[commonpb.AccountBuiltinIndex]bool),
		logBuiltinIndexed:   make(map[commonpb.LogBuiltinIndex]bool),
	}
}

// getOrCreateLedgerConfig returns the index config for a ledger, creating it if needed.
func (b *Builder) getOrCreateLedgerConfig(ledger string) *ledgerIndexConfig {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		cfg = newLedgerIndexConfig()
		b.indexConfig[ledger] = cfg
	}

	return cfg
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

// --- Backfill helpers ---

// backfillBBKey builds the bbolt key for persisting backfill progress.
// Format:
//
//	TxBuiltin:    [ledger\x00]b[builtin_byte]
//	TxMetadata:   [ledger\x00]T[key]
//	AcctBuiltin:  [ledger\x00]A[builtin_byte]
//	AcctMetadata: [ledger\x00]a[key]
//	LogBuiltin:   [ledger\x00]l[builtin_byte]
func backfillBBKey(ledger string, id indexID) []byte {
	if id.transaction != nil {
		switch txIdx := id.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			key := make([]byte, 0, len(ledger)+3)
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindTxBuiltin, byte(txIdx.Builtin))

			return key
		case *commonpb.TransactionIndex_MetadataKey:
			key := make([]byte, 0, len(ledger)+2+len(txIdx.MetadataKey))
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindTxMetadata)
			key = append(key, txIdx.MetadataKey...)

			return key
		}
	}

	if id.account != nil {
		switch acctIdx := id.account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			key := make([]byte, 0, len(ledger)+3)
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindAcctBuiltin, byte(acctIdx.Builtin))

			return key
		case *commonpb.AccountIndex_MetadataKey:
			key := make([]byte, 0, len(ledger)+2+len(acctIdx.MetadataKey))
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindAcctMetadata)
			key = append(key, acctIdx.MetadataKey...)

			return key
		}
	}

	if id.logBuiltin != nil {
		key := make([]byte, 0, len(ledger)+3)
		key = append(key, ledger...)
		key = append(key, 0x00, readstore.BackfillKindLogBuiltin, byte(*id.logBuiltin))

		return key
	}

	return nil
}

// addBackfillTask is a helper that creates a backfill task for the given indexID,
// avoiding duplicates by checking the precomputed bbolt key.
func (b *Builder) addBackfillTask(ledger string, id indexID) {
	bbKey := backfillBBKey(ledger, id)
	for _, t := range b.backfillTasks {
		if string(t.bbKey) == string(bbKey) {
			return
		}
	}

	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger: ledger,
		index:  id,
		cursor: 0,
		bbKey:  bbKey,
	})
}

// addBackfillTaskForTxBuiltin creates a backfill task for a transaction builtin index.
func (b *Builder) addBackfillTaskForTxBuiltin(ledger string, index commonpb.TransactionBuiltinIndex) {
	b.addBackfillTask(ledger, indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: index},
	}})
}

// addBackfillTaskForTxMetadata creates a backfill task for a transaction metadata index.
func (b *Builder) addBackfillTaskForTxMetadata(ledger string, key string) {
	b.addBackfillTask(ledger, indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: key},
	}})
}

// addBackfillTaskForAcctMetadata creates a backfill task for an account metadata index.
func (b *Builder) addBackfillTaskForAcctMetadata(ledger string, key string) {
	b.addBackfillTask(ledger, indexID{account: &commonpb.AccountIndex{
		Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: key},
	}})
}

// addBackfillTaskForLogBuiltin creates a backfill task for a log builtin index.
func (b *Builder) addBackfillTaskForLogBuiltin(ledger string, index commonpb.LogBuiltinIndex) {
	b.addBackfillTask(ledger, indexID{logBuiltin: &index})
}

// removeBackfillTask removes a backfill task by index ID and deletes its
// progress from bbolt.
func (b *Builder) removeBackfillTask(id indexID) {
	for i, t := range b.backfillTasks {
		if matchesBackfillIndex(t.index, id) {
			// Delete persisted progress.
			_ = b.readStore.Update(func(tx *bolt.Tx) error {
				return b.readStore.DeleteBackfillProgress(tx, t.bbKey)
			})
			// Remove from slice (order doesn't matter).
			b.backfillTasks[i] = b.backfillTasks[len(b.backfillTasks)-1]
			b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

			return
		}
	}
}

// matchesBackfillIndex checks if two indexIDs represent the same index.
func matchesBackfillIndex(a, b indexID) bool {
	if a.transaction != nil && b.transaction != nil {
		return matchesTransactionIndex(a.transaction, b.transaction)
	}

	if a.account != nil && b.account != nil {
		return matchesAccountIndex(a.account, b.account)
	}

	if a.logBuiltin != nil && b.logBuiltin != nil {
		return *a.logBuiltin == *b.logBuiltin
	}

	return false
}

// matchesTransactionIndex checks if two TransactionIndex values represent the same index.
func matchesTransactionIndex(a, b *commonpb.TransactionIndex) bool {
	switch ak := a.GetKind().(type) {
	case *commonpb.TransactionIndex_Builtin:
		if bk, ok := b.GetKind().(*commonpb.TransactionIndex_Builtin); ok {
			return ak.Builtin == bk.Builtin
		}
	case *commonpb.TransactionIndex_MetadataKey:
		if bk, ok := b.GetKind().(*commonpb.TransactionIndex_MetadataKey); ok {
			return ak.MetadataKey == bk.MetadataKey
		}
	}

	return false
}

// matchesAccountIndex checks if two AccountIndex values represent the same index.
func matchesAccountIndex(a, b *commonpb.AccountIndex) bool {
	switch ak := a.GetKind().(type) {
	case *commonpb.AccountIndex_Builtin:
		if bk, ok := b.GetKind().(*commonpb.AccountIndex_Builtin); ok {
			return ak.Builtin == bk.Builtin
		}
	case *commonpb.AccountIndex_MetadataKey:
		if bk, ok := b.GetKind().(*commonpb.AccountIndex_MetadataKey); ok {
			return ak.MetadataKey == bk.MetadataKey
		}
	}

	return false
}

// processBackfills advances each active backfill task until caught up or stopped.
// A single Pebble iterator is kept open per task across multiple bbolt batches,
// avoiding the overhead of repeated NewIter/First calls during catch-up.
// When a backfill catches up to globalCursor, it proposes IndexReady (leader only).
// If the proposal fails (not leader or Raft error), the task is kept and retried.
func (b *Builder) processBackfills(stop <-chan struct{}, globalCursor uint64) {
	if len(b.backfillTasks) == 0 {
		return
	}

	// Process each task (iterate backward for safe removal).
	for i := len(b.backfillTasks) - 1; i >= 0; i-- {
		select {
		case <-stop:
			return
		default:
		}

		task := b.backfillTasks[i]

		if task.cursor >= globalCursor {
			// Backfill is caught up — propose IndexReady.
			if !b.proposeIndexReady(task) {
				// Proposal failed (not leader or Raft error) — keep the task
				// and retry on the next tick.
				continue
			}

			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"cursor": task.cursor,
			}).Infof("Backfill complete, IndexReady proposed")

			// Delete persisted progress.
			_ = b.readStore.Update(func(tx *bolt.Tx) error {
				return b.readStore.DeleteBackfillProgress(tx, task.bbKey)
			})

			// Remove from slice.
			b.backfillTasks[i] = b.backfillTasks[len(b.backfillTasks)-1]
			b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

			continue
		}

		// Process all available batches with a single iterator.
		if err := b.processBackfill(stop, task); err != nil {
			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"cursor": task.cursor,
				"error":  err,
			}).Errorf("Error processing backfill")
		}
	}
}

// processBackfill reads logs from Pebble using a single iterator and indexes
// them in batches of backfillBatchSize using only the backfilling index's
// configuration. The iterator stays open across batches to avoid repeated
// NewIter/First overhead during catch-up. Existence writes are skipped.
func (b *Builder) processBackfill(stop <-chan struct{}, task *backfillTask) error {
	logsCursor, err := query.ReadLogsSince(context.Background(), b.pebbleStore, task.cursor, dal.WithReuse())
	if err != nil {
		return err
	}

	defer func() { _ = logsCursor.Close() }()

	// Build a temporary index config with only the backfilling index enabled.
	tmpConfig := b.buildBackfillConfig(task)

	// Swap index config, enable backfill mode.
	origConfig := b.indexConfig
	b.indexConfig = tmpConfig
	b.backfillMode = true

	defer func() {
		b.indexConfig = origConfig
		b.backfillMode = false
	}()

	for {
		select {
		case <-stop:
			return nil
		default:
		}

		var (
			batchCount int
			lastSeq    uint64
			eof        bool
		)

		if err := b.readStore.Update(func(tx *bolt.Tx) error {
			b.wb.Init(tx)

			for batchCount < b.batchSize {
				log, err := logsCursor.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						eof = true

						break
					}

					return err
				}

				// Skip config-mutation log types during backfill.
				if !isDataLog(log) {
					lastSeq = log.GetSequence()
					batchCount++

					continue
				}

				if err := b.indexLogEntry(tx, log); err != nil {
					return err
				}

				lastSeq = log.GetSequence()
				batchCount++
			}

			// Persist backfill cursor.
			if batchCount > 0 {
				if err := b.wb.Flush(); err != nil {
					return err
				}

				return b.readStore.WriteBackfillProgress(tx, task.bbKey, lastSeq)
			}

			return nil
		}); err != nil {
			return err
		}

		if batchCount == 0 {
			break
		}

		task.cursor = lastSeq

		if eof {
			break
		}
	}

	return nil
}

// buildBackfillConfig creates a temporary indexConfig map containing only the
// backfilling index's ledger config with only that index enabled.
func (b *Builder) buildBackfillConfig(task *backfillTask) map[string]*ledgerIndexConfig {
	cfg := newLedgerIndexConfig()

	if task.index.transaction != nil {
		switch txIdx := task.index.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			cfg.txBuiltinIndexed[txIdx.Builtin] = true
		case *commonpb.TransactionIndex_MetadataKey:
			cfg.txMetadataIndexed[txIdx.MetadataKey] = true
		}
	}

	if task.index.account != nil {
		switch acctIdx := task.index.account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			cfg.acctBuiltinIndexed[acctIdx.Builtin] = true
		case *commonpb.AccountIndex_MetadataKey:
			cfg.acctMetadataIndexed[acctIdx.MetadataKey] = true
		}
	}

	if task.index.logBuiltin != nil {
		cfg.logBuiltinIndexed[*task.index.logBuiltin] = true
	}

	return map[string]*ledgerIndexConfig{task.ledger: cfg}
}

// isDataLog returns true if the log entry contains indexable data
// (transactions, metadata). Returns false for config-mutation logs
// (CreateIndex, DropIndex, IndexReady, etc.) which must be skipped during backfill.
func isDataLog(log *commonpb.Log) bool {
	if log.GetPayload() == nil {
		return false
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		return false
	}

	if applyLog.Apply.GetLog() == nil || applyLog.Apply.GetLog().GetData() == nil {
		return false
	}

	switch applyLog.Apply.GetLog().GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction,
		*commonpb.LedgerLogPayload_RevertedTransaction,
		*commonpb.LedgerLogPayload_SavedMetadata,
		*commonpb.LedgerLogPayload_DeletedMetadata:
		return true
	default:
		return false
	}
}

// proposeIndexReady proposes an IndexReady order through Raft (leader only).
// Returns true if the proposal was submitted successfully, false otherwise.
func (b *Builder) proposeIndexReady(task *backfillTask) bool {
	if b.proposer == nil || b.isLeader == nil || !b.isLeader() {
		return false
	}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: task.ledger,
			},
		},
	}

	switch {
	case task.index.transaction != nil:
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_Transaction{
					Transaction: task.index.transaction,
				},
			},
		}
	case task.index.account != nil:
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_Account{
					Account: task.index.account,
				},
			},
		}
	case task.index.logBuiltin != nil:
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_LogBuiltin{
					LogBuiltin: *task.index.logBuiltin,
				},
			},
		}
	}

	if err := b.proposer.ProposeOrders(order); err != nil {
		b.logger.WithFields(map[string]any{
			"ledger": task.ledger,
			"error":  err,
		}).Errorf("Failed to propose IndexReady")

		return false
	}

	return true
}

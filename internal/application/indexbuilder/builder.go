package indexbuilder

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/metric"
)

// DefaultBatchSize is the default number of log entries per bbolt write
// transaction. Can be overridden via --read-index-batch-size.
const DefaultBatchSize = 1000

// Proposer proposes Raft commands to the cluster.
// Implemented by a thin adapter around *node.Node (bootstrap.NodeProposer).
type Proposer interface {
	ProposeOrders(orders ...*raftcmdpb.Order) error
}

// indexID identifies a specific index (address role or metadata field).
type indexID struct {
	addressRole *commonpb.AddressRole         // set for address indexes
	metadata    *commonpb.MetadataIndexTarget // set for metadata indexes
}

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger string
	index  indexID
	cursor uint64 // current position (persisted in bbolt)
	bbKey  []byte // precomputed bbolt key for progress persistence
}

// metadataIndexKey identifies a metadata index by target type and key name.
type metadataIndexKey struct {
	Target commonpb.TargetType
	Key    string
}

// ledgerIndexConfig caches which indexes are enabled and ready for a ledger.
type ledgerIndexConfig struct {
	addressIndexed  map[commonpb.AddressRole]bool // true = indexed (READY or BUILDING)
	metadataIndexed map[metadataIndexKey]bool     // true = indexed (READY or BUILDING)
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
	kb       *readstore.KeyBuilder
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
		kb:          readstore.NewKeyBuilder(),
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
			if err == io.EOF {
				break
			}
			b.logger.Errorf("Error reading ledger info: %v", err)
			return
		}
		if info.DeletedAt != nil {
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
	cfg := &ledgerIndexConfig{
		addressIndexed:  make(map[commonpb.AddressRole]bool),
		metadataIndexed: make(map[metadataIndexKey]bool),
	}

	// Address indexes — include both READY and BUILDING.
	if ac := info.AddressIndexes; ac != nil {
		for _, entry := range []struct {
			role    commonpb.AddressRole
			enabled bool
			status  commonpb.IndexBuildStatus
		}{
			{commonpb.AddressRole_ADDRESS_ROLE_ANY, ac.Address, ac.AddressStatus},
			{commonpb.AddressRole_ADDRESS_ROLE_SOURCE, ac.Source, ac.SourceStatus},
			{commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, ac.Destination, ac.DestinationStatus},
		} {
			if !entry.enabled {
				continue
			}
			cfg.addressIndexed[entry.role] = true
			if entry.status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForAddress(info.Name, entry.role)
			}
		}
	}

	// Metadata indexes — include both READY and BUILDING.
	if info.MetadataSchema != nil {
		b.loadMetadataIndexes(cfg, info.Name, commonpb.TargetType_TARGET_TYPE_ACCOUNT, info.MetadataSchema.AccountFields)
		b.loadMetadataIndexes(cfg, info.Name, commonpb.TargetType_TARGET_TYPE_TRANSACTION, info.MetadataSchema.TransactionFields)
	}

	b.indexConfig[info.Name] = cfg
}

// loadMetadataIndexes loads metadata indexes for a given target type.
func (b *Builder) loadMetadataIndexes(
	cfg *ledgerIndexConfig,
	ledger string,
	target commonpb.TargetType,
	fields map[string]*commonpb.MetadataFieldSchema,
) {
	for key, field := range fields {
		if !field.Indexed {
			continue
		}
		mk := metadataIndexKey{Target: target, Key: key}
		cfg.metadataIndexed[mk] = true
		if field.IndexBuildStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			b.addBackfillTaskForMetadata(ledger, target, key)
		}
	}
}

// isAddressIndexed checks if a specific address role index is enabled and ready.
func (b *Builder) isAddressIndexed(ledger string, role commonpb.AddressRole) bool {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		return false
	}
	return cfg.addressIndexed[role]
}

// isMetadataIndexed checks if a specific metadata index is enabled and ready.
func (b *Builder) isMetadataIndexed(ledger string, target commonpb.TargetType, key string) bool {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		return false
	}
	return cfg.metadataIndexed[metadataIndexKey{Target: target, Key: key}]
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
			lag := pebbleLast - indexed
			if lag < 0 {
				lag = 0
			}
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
					if err == io.EOF {
						eof = true
						break
					}
					return err
				}

				if err := b.indexLogEntry(tx, log); err != nil {
					return err
				}
				lastSeq = log.Sequence
				batchCount++
			}

			if batchCount > 0 {
				if err := b.wb.Flush(); err != nil {
					return err
				}
				return b.readStore.WriteProgress(tx, lastSeq)
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
	if log.Payload == nil {
		return nil
	}

	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok {
		return nil
	}

	ledgerName := applyLog.Apply.LedgerName
	ledgerLog := applyLog.Apply.Log
	if ledgerLog == nil || ledgerLog.Data == nil {
		return nil
	}

	switch p := ledgerLog.Data.Payload.(type) {
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

	wb := b.wb

	// Transaction existence (skip during backfill — already written by normal processing)
	if !b.backfillMode {
		wb.WriteTransactionExistence(kb, ledger, txn.Id)
	}

	// Collect unique accounts from postings (reuse builder's map)
	indexAny := b.isAddressIndexed(ledger, commonpb.AddressRole_ADDRESS_ROLE_ANY)
	indexSrc := b.isAddressIndexed(ledger, commonpb.AddressRole_ADDRESS_ROLE_SOURCE)
	indexDst := b.isAddressIndexed(ledger, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION)

	clear(b.accounts)
	for _, posting := range txn.Postings {
		b.accounts[posting.Source] = struct{}{}
		b.accounts[posting.Destination] = struct{}{}

		// Account→transaction mapping (any role)
		if indexAny {
			wb.WriteAccountTxMapping(kb, ledger, posting.Source, txn.Id)
			wb.WriteAccountTxMapping(kb, ledger, posting.Destination, txn.Id)
		}
		// Role-specific mappings
		if indexSrc {
			wb.WriteSourceAccountTxMapping(kb, ledger, posting.Source, txn.Id)
		}
		if indexDst {
			wb.WriteDestAccountTxMapping(kb, ledger, posting.Destination, txn.Id)
		}
	}

	// Account existence for all accounts in postings (skip during backfill)
	if !b.backfillMode {
		for account := range b.accounts {
			wb.WriteAccountExistence(kb, ledger, account)
		}
	}

	// Account existence + metadata from account_metadata map
	for account, metadataSet := range ct.AccountMetadata {
		if !b.backfillMode {
			wb.WriteAccountExistence(kb, ledger, account)
		}
		if metadataSet != nil {
			for _, md := range metadataSet.Metadata {
				if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, md.Key) {
					continue
				}
				reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.Key)
				encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
				wb.UpdateMetadataIndex(
					kb, reverseKey,
					ledger, readstore.NamespaceAccount, md.Key,
					encodedValue, []byte(account),
				)
			}
		}
	}

	// Transaction metadata
	if txn.Metadata != nil {
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.Id)
		for _, md := range txn.Metadata.Metadata {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.Key) {
				continue
			}
			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txn.Id, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.Key,
				encodedValue, txIDBytes,
			)
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
	wb := b.wb

	// Revert transaction existence (skip during backfill)
	if !b.backfillMode {
		wb.WriteTransactionExistence(kb, ledger, revertTxn.Id)
	}

	// Account existence + account→tx mapping for revert postings (reuse builder's map)
	indexAny := b.isAddressIndexed(ledger, commonpb.AddressRole_ADDRESS_ROLE_ANY)
	indexSrc := b.isAddressIndexed(ledger, commonpb.AddressRole_ADDRESS_ROLE_SOURCE)
	indexDst := b.isAddressIndexed(ledger, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION)

	clear(b.accounts)
	for _, posting := range revertTxn.Postings {
		b.accounts[posting.Source] = struct{}{}
		b.accounts[posting.Destination] = struct{}{}
		if indexAny {
			wb.WriteAccountTxMapping(kb, ledger, posting.Source, revertTxn.Id)
			wb.WriteAccountTxMapping(kb, ledger, posting.Destination, revertTxn.Id)
		}
		// Role-specific mappings
		if indexSrc {
			wb.WriteSourceAccountTxMapping(kb, ledger, posting.Source, revertTxn.Id)
		}
		if indexDst {
			wb.WriteDestAccountTxMapping(kb, ledger, posting.Destination, revertTxn.Id)
		}
	}
	if !b.backfillMode {
		for account := range b.accounts {
			wb.WriteAccountExistence(kb, ledger, account)
		}
	}

	// Transaction metadata for the revert transaction
	if revertTxn.Metadata != nil {
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.Id)
		for _, md := range revertTxn.Metadata.Metadata {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.Key) {
				continue
			}
			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, revertTxn.Id, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.Key,
				encodedValue, txIDBytes,
			)
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

	wb := b.wb

	switch t := sm.Target.Target.(type) {
	case *commonpb.Target_Account:
		account := t.Account.Addr
		for _, md := range sm.Metadata.Metadata {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, md.Key) {
				continue
			}
			reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceAccount, md.Key,
				encodedValue, []byte(account),
			)
		}
	case *commonpb.Target_Transaction:
		txID := t.Transaction.Id
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		for _, md := range sm.Metadata.Metadata {
			if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.Key) {
				continue
			}
			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, md.Key)
			encodedValue := readstore.EncodeMetadataValue(nil, md.Value)
			wb.UpdateMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.Key,
				encodedValue, txIDBytes,
			)
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

	wb := b.wb

	switch t := dm.Target.Target.(type) {
	case *commonpb.Target_Account:
		if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, dm.Key) {
			return nil
		}
		account := t.Account.Addr
		reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, dm.Key)
		wb.DeleteMetadataEntry(
			kb, reverseKey,
			ledger, readstore.NamespaceAccount, dm.Key,
			[]byte(account),
		)
	case *commonpb.Target_Transaction:
		if !b.isMetadataIndexed(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, dm.Key) {
			return nil
		}
		txID := t.Transaction.Id
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, dm.Key)
		wb.DeleteMetadataEntry(
			kb, reverseKey,
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
	// Only re-encode if this metadata key is indexed.
	if !b.isMetadataIndexed(ledger, smft.TargetType, smft.Key) {
		return nil
	}

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
	eidxBucket := tx.Bucket(readstore.BucketEntityExists)
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

		// Update eidx if null status changed
		oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull
		newIsNull := len(newEncoded) > 0 && newEncoded[0] == readstore.TypeTagNull
		if oldIsNull != newIsNull {
			oldEidxKey := readstore.EntityExistsKey(kb, ledger, ns, smft.Key, oldIsNull, e.entityID)
			if err := eidxBucket.Delete(oldEidxKey); err != nil {
				return err
			}
			newEidxKey := readstore.EntityExistsKey(kb, ledger, ns, smft.Key, newIsNull, e.entityID)
			if err := eidxBucket.Put(newEidxKey, nil); err != nil {
				return err
			}
		}

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

// handleCreateIndexLog updates the index config cache when a CreateIndex log is processed.
// The index starts in BUILDING state — it is NOT marked as ready here.
// A backfill task is created to replay historical logs for the new index.
func (b *Builder) handleCreateIndexLog(ledger string, log *commonpb.CreateIndexLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)
	switch idx := log.Index.(type) {
	case *commonpb.CreateIndexLog_AddressRole:
		cfg.addressIndexed[idx.AddressRole] = true
		b.addBackfillTaskForAddress(ledger, idx.AddressRole)
	case *commonpb.CreateIndexLog_Metadata:
		cfg.metadataIndexed[metadataIndexKey{
			Target: idx.Metadata.Target,
			Key:    idx.Metadata.Key,
		}] = true
		b.addBackfillTaskForMetadata(ledger, idx.Metadata.Target, idx.Metadata.Key)
	}
}

// handleDropIndexLog updates the index config cache when a DropIndex log is processed.
// It also removes any active backfill task for the dropped index.
func (b *Builder) handleDropIndexLog(ledger string, log *commonpb.DropIndexLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)
	switch idx := log.Index.(type) {
	case *commonpb.DropIndexLog_AddressRole:
		delete(cfg.addressIndexed, idx.AddressRole)
		role := idx.AddressRole
		b.removeBackfillTask(indexID{addressRole: &role})
	case *commonpb.DropIndexLog_Metadata:
		delete(cfg.metadataIndexed, metadataIndexKey{
			Target: idx.Metadata.Target,
			Key:    idx.Metadata.Key,
		})
		b.removeBackfillTask(indexID{metadata: idx.Metadata})
	}
}

// handleIndexReadyLog updates the index config cache when an IndexReady log is processed.
// This marks the index as READY and removes any residual backfill task.
func (b *Builder) handleIndexReadyLog(ledger string, log *commonpb.IndexReadyLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)
	switch idx := log.Index.(type) {
	case *commonpb.IndexReadyLog_AddressRole:
		cfg.addressIndexed[idx.AddressRole] = true
		role := idx.AddressRole
		b.removeBackfillTask(indexID{addressRole: &role})
	case *commonpb.IndexReadyLog_Metadata:
		cfg.metadataIndexed[metadataIndexKey{
			Target: idx.Metadata.Target,
			Key:    idx.Metadata.Key,
		}] = true
		b.removeBackfillTask(indexID{metadata: idx.Metadata})
	}
}

// getOrCreateLedgerConfig returns the index config for a ledger, creating it if needed.
func (b *Builder) getOrCreateLedgerConfig(ledger string) *ledgerIndexConfig {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		cfg = &ledgerIndexConfig{
			addressIndexed:  make(map[commonpb.AddressRole]bool),
			metadataIndexed: make(map[metadataIndexKey]bool),
		}
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
//	Address: [ledger\x00]a[role_byte]
//	Metadata: [ledger\x00]m[target_byte][key]
func backfillBBKey(ledger string, id indexID) []byte {
	if id.addressRole != nil {
		key := make([]byte, 0, len(ledger)+3)
		key = append(key, ledger...)
		key = append(key, 0x00, 'a', byte(*id.addressRole))
		return key
	}
	if id.metadata != nil {
		key := make([]byte, 0, len(ledger)+3+len(id.metadata.Key))
		key = append(key, ledger...)
		key = append(key, 0x00, 'm', byte(id.metadata.Target))
		key = append(key, id.metadata.Key...)
		return key
	}
	return nil
}

// addBackfillTaskForAddress creates a backfill task for an address index.
func (b *Builder) addBackfillTaskForAddress(ledger string, role commonpb.AddressRole) {
	id := indexID{addressRole: &role}
	// Avoid duplicates.
	for _, t := range b.backfillTasks {
		if string(t.bbKey) == string(backfillBBKey(ledger, id)) {
			return
		}
	}
	task := &backfillTask{
		ledger: ledger,
		index:  id,
		cursor: 0,
		bbKey:  backfillBBKey(ledger, id),
	}
	b.backfillTasks = append(b.backfillTasks, task)
}

// addBackfillTaskForMetadata creates a backfill task for a metadata index.
func (b *Builder) addBackfillTaskForMetadata(ledger string, target commonpb.TargetType, key string) {
	id := indexID{metadata: &commonpb.MetadataIndexTarget{Target: target, Key: key}}
	bbKey := backfillBBKey(ledger, id)
	// Avoid duplicates.
	for _, t := range b.backfillTasks {
		if string(t.bbKey) == string(bbKey) {
			return
		}
	}
	task := &backfillTask{
		ledger: ledger,
		index:  id,
		cursor: 0,
		bbKey:  bbKey,
	}
	b.backfillTasks = append(b.backfillTasks, task)
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
	if a.addressRole != nil && b.addressRole != nil {
		return *a.addressRole == *b.addressRole
	}
	if a.metadata != nil && b.metadata != nil {
		return a.metadata.Target == b.metadata.Target && a.metadata.Key == b.metadata.Key
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
					if err == io.EOF {
						eof = true
						break
					}
					return err
				}

				// Skip config-mutation log types during backfill.
				if !isDataLog(log) {
					lastSeq = log.Sequence
					batchCount++
					continue
				}

				if err := b.indexLogEntry(tx, log); err != nil {
					return err
				}
				lastSeq = log.Sequence
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
	cfg := &ledgerIndexConfig{
		addressIndexed:  make(map[commonpb.AddressRole]bool),
		metadataIndexed: make(map[metadataIndexKey]bool),
	}
	if task.index.addressRole != nil {
		cfg.addressIndexed[*task.index.addressRole] = true
	}
	if task.index.metadata != nil {
		cfg.metadataIndexed[metadataIndexKey{
			Target: task.index.metadata.Target,
			Key:    task.index.metadata.Key,
		}] = true
	}
	return map[string]*ledgerIndexConfig{task.ledger: cfg}
}

// isDataLog returns true if the log entry contains indexable data
// (transactions, metadata). Returns false for config-mutation logs
// (CreateIndex, DropIndex, IndexReady, etc.) which must be skipped during backfill.
func isDataLog(log *commonpb.Log) bool {
	if log.Payload == nil {
		return false
	}
	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok {
		return false
	}
	if applyLog.Apply.Log == nil || applyLog.Apply.Log.Data == nil {
		return false
	}
	switch applyLog.Apply.Log.Data.Payload.(type) {
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

	if task.index.addressRole != nil {
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_AddressRole{
					AddressRole: *task.index.addressRole,
				},
			},
		}
	} else if task.index.metadata != nil {
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_Metadata{
					Metadata: task.index.metadata,
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

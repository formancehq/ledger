package ctrl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/analysis"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

var tracer = otel.Tracer("ctrl")

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller_default.go -destination controller_default_generated_test.go -package ctrl . Admission
type Admission interface {
	Admit(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
}

// DefaultController is the default implementation of the Controller interface.
// It is responsible for forwarding requests to the Raft admission layer.
// The FSM is responsible for interpreting requests, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency in the Raft log.
type DefaultController struct {
	logger    logging.Logger
	admission Admission
	store     *dal.Store
	attrs     *attributes.Attributes
	readStore *readstore.Store
}

// NewDefaultController creates a new default controller.
func NewDefaultController(
	admission Admission,
	store *dal.Store,
	logger logging.Logger,
	attrs *attributes.Attributes,
	readStore *readstore.Store,
) *DefaultController {
	return &DefaultController{
		logger:    logger,
		admission: admission,
		store:     store,
		attrs:     attrs,
		readStore: readStore,
	}
}

// ListLedgers returns a cursor over all active (non-deleted) ledgers.
func (ctrl *DefaultController) ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	handle := ctrl.store.NewReadHandle()

	cursor, err := query.ReadLedgers(ctx, handle)
	if err != nil {
		_ = handle.Close()

		return nil, err
	}
	// Filter out soft-deleted ledgers, close handle when cursor closes
	filtered := dal.NewFilteredCursor(cursor, func(ledger *commonpb.LedgerInfo) bool {
		return ledger.GetDeletedAt() == nil
	})

	return dal.NewClosingCursor(filtered, handle), nil
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	_, span := tracer.Start(ctx, "ctrl.get_transaction",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int64("transaction_id", int64(transactionID)),
		))
	defer span.End()

	ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	return buildTransaction(ctx, handle, ledgerInfo.GetName(), transactionID, ledgerInfo.GetMetadataSchema())
}

// buildTransaction builds a transaction from updates and logs using the given reader.
func buildTransaction(ctx context.Context, reader dal.PebbleReader, ledger string, transactionID uint64, schema *commonpb.MetadataSchema) (*commonpb.Transaction, error) {
	updates, err := query.ReadTransactionUpdates(ctx, reader, ledger, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", transactionID, err)
	}

	return assembleTransaction(ctx, reader, transactionID, updates, schema)
}

// enforceTransactionSchema converts transaction metadata values in-place
// according to the ledger's declared metadata schema. Mirrors enforceAccountSchema.
func enforceTransactionSchema(schema *commonpb.MetadataSchema, metadata []*commonpb.Metadata) {
	if schema == nil || len(schema.GetTransactionFields()) == 0 {
		return
	}

	for _, m := range metadata {
		fieldSchema, ok := schema.GetTransactionFields()[m.GetKey()]
		if !ok || m.GetValue() == nil {
			continue
		}

		if !commonpb.TypeMatches(m.GetValue(), fieldSchema.GetType()) {
			m.Value = commonpb.ConvertMetadataValue(m.GetValue(), fieldSchema.GetType())
		}
	}
}

// assembleTransaction builds a transaction from a slice of updates and a log reader.
// The updates must be in chronological order (lowest byLog first).
// If schema is non-nil, read-time type enforcement is applied to the final metadata.
func assembleTransaction(ctx context.Context, reader dal.PebbleReader, transactionID uint64, updates []*commonpb.TransactionUpdate, schema *commonpb.MetadataSchema) (*commonpb.Transaction, error) {
	var (
		sequence         uint64
		reverted         bool
		metadataToAdd    = make(map[string]*commonpb.MetadataValue)
		metadataToDelete = make(map[string]struct{})
	)

	for _, update := range updates {
		for _, updateType := range update.GetUpdates() {
			if updateType.GetTransactionInit() != nil {
				sequence = update.GetByLog()
			}

			if updateType.GetTransactionModificationRevert() != nil {
				reverted = true
			}

			if addMeta := updateType.GetTransactionModificationAddMetadata(); addMeta != nil {
				metadataToAdd[addMeta.GetMetadata().GetKey()] = addMeta.GetMetadata().GetValue()
				delete(metadataToDelete, addMeta.GetMetadata().GetKey())
			}

			if delMeta := updateType.GetTransactionModificationDeleteMetadata(); delMeta != nil {
				metadataToDelete[delMeta.GetKey()] = struct{}{}
				delete(metadataToAdd, delMeta.GetKey())
			}
		}
	}

	if sequence == 0 {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	log, err := query.ReadLogBySequence(ctx, reader, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}

	if log == nil {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.GetLog() == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", sequence)
	}

	ledgerLog := applyLog.Apply.GetLog()

	var tx *commonpb.Transaction

	switch payload := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.GetTransaction() == nil {
			return nil, errors.New("invalid log payload: missing transaction")
		}

		tx = payload.CreatedTransaction.GetTransaction()
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.GetRevertTransaction() == nil {
			return nil, errors.New("invalid log payload: missing revert transaction")
		}

		tx = payload.RevertedTransaction.GetRevertTransaction()
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a transaction", ledgerLog.GetId())
	}

	tx.Reverted = reverted

	if len(metadataToAdd) > 0 || len(metadataToDelete) > 0 {
		// Build a map from existing metadata (preserving typed values).
		existing := make(map[string]*commonpb.MetadataValue)

		if tx.GetMetadata() != nil {
			for _, m := range tx.GetMetadata().GetMetadata() {
				existing[m.GetKey()] = m.GetValue()
			}
		}

		maps.Copy(existing, metadataToAdd)

		for key := range metadataToDelete {
			delete(existing, key)
		}
		// Rebuild the MetadataSet from typed values.
		mdList := make([]*commonpb.Metadata, 0, len(existing))
		for key, value := range existing {
			mdList = append(mdList, &commonpb.Metadata{Key: key, Value: value})
		}

		tx.Metadata = &commonpb.MetadataSet{Metadata: mdList}
	}

	// Apply read-time schema enforcement for transaction metadata.
	if tx.GetMetadata() != nil {
		enforceTransactionSchema(schema, tx.GetMetadata().GetMetadata())
	}

	return tx, nil
}

// ListTransactions returns a cursor over transactions for a ledger.
// API convention: reverse=false means newest-first (descending), reverse=true means oldest-first.
// Internally listEntities uses reverse=true for descending, so we invert the flag here.
func (ctrl *DefaultController) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Transaction], error) {
	ctx, span := tracer.Start(ctx, "ctrl.list_transactions",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int("page_size", int(pageSize)),
			attribute.Bool("reverse", reverse),
		))
	defer span.End()

	profile := query.ProfileFromContext(ctx)

	// Create a Pebble snapshot first so that GetLedgerByName and the listing
	// read from the same consistent point-in-time view.
	handle := ctrl.store.NewReadHandle()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		_ = handle.Close()

		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	if pageSize == 0 {
		pageSize = math.MaxUint32
	}

	schemaFields := query.SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)

	indexStart := time.Now()

	result, err := listEntities(ctrl.readStore, entityListParams[uint64]{
		target:       commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		ledger:       ledgerInfo.GetName(),
		pageSize:     pageSize,
		after:        afterTxID,
		filter:       filter,
		reverse:      !reverse, // API: reverse=false → newest-first (desc); listEntities: reverse=true → desc
		schema:       schemaFields,
		builtinCfg:   ledgerInfo.GetBuiltinIndexes(),
		profile:      profile,
		pebbleReader: handle,
		afterToBytes: func(id uint64) []byte {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, id)

			return b
		},
	})

	if profile != nil {
		profile.IndexDuration = time.Since(indexStart)
	}

	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing transactions from index: %w", err)
	}

	// Enrich each transaction ID from Pebble
	enrichStart := time.Now()

	txns := make([]*commonpb.Transaction, 0, len(result.entityIDs))
	for _, eid := range result.entityIDs {
		txID := binary.BigEndian.Uint64(eid)

		tx, txErr := buildTransaction(ctx, handle, ledgerInfo.GetName(), txID, ledgerInfo.GetMetadataSchema())
		if txErr != nil {
			_ = handle.Close()

			return nil, txErr
		}

		txns = append(txns, tx)
	}

	if profile != nil {
		profile.EnrichmentDuration = time.Since(enrichStart)
		profile.EnrichedCount = len(txns)
		profile.ItemsCollected = len(result.entityIDs)
	}

	return dal.NewClosingCursor(dal.NewSliceCursor(txns), handle), nil
}

// ListAccounts returns a cursor over accounts for a ledger.
// Default order (reverse=false) is ascending (A→Z); reverse=true gives reverse-alphabetical (Z→A).
// Uses the bbolt read index for entity discovery and Pebble for enrichment.
func (ctrl *DefaultController) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Account], error) {
	ctx, span := tracer.Start(ctx, "ctrl.list_accounts",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int("page_size", int(pageSize)),
			attribute.Bool("reverse", reverse),
		))
	defer span.End()

	profile := query.ProfileFromContext(ctx)

	// Create a Pebble snapshot first so that GetLedgerByName and the listing
	// read from the same consistent point-in-time view.
	handle := ctrl.store.NewReadHandle()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		_ = handle.Close()

		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	if pageSize == 0 {
		pageSize = math.MaxUint32
	}

	schemaFields := query.SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)

	indexStart := time.Now()

	result, err := listEntities(ctrl.readStore, entityListParams[string]{
		target:       commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		ledger:       ledgerInfo.GetName(),
		pageSize:     pageSize,
		after:        afterAddress,
		filter:       filter,
		reverse:      reverse,
		schema:       schemaFields,
		builtinCfg:   ledgerInfo.GetBuiltinIndexes(),
		profile:      profile,
		pebbleReader: handle,
		afterToBytes: func(addr string) []byte {
			return []byte(addr)
		},
	})

	if profile != nil {
		profile.IndexDuration = time.Since(indexStart)
	}

	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing accounts from index: %w", err)
	}

	// Enrich each account from Pebble
	enrichStart := time.Now()

	accounts := make([]*commonpb.Account, 0, len(result.entityIDs))
	for _, addr := range result.entityIDs {
		acc, accErr := scanAccount(handle, ctrl.attrs, ledgerInfo.GetName(), string(addr), ledgerInfo.GetMetadataSchema())
		if accErr != nil {
			_ = handle.Close()

			return nil, accErr
		}

		accounts = append(accounts, acc)
	}

	if profile != nil {
		profile.EnrichmentDuration = time.Since(enrichStart)
		profile.EnrichedCount = len(accounts)
		profile.ItemsCollected = len(result.entityIDs)
	}

	return dal.NewClosingCursor(dal.NewSliceCursor(accounts), handle), nil
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	_, span := tracer.Start(ctx, "ctrl.get_account",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.String("address", address),
		))
	defer span.End()

	ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	// Single-entity lookup reads directly from Pebble without bbolt filtering,
	// so no cross-store consistency cap is needed. Use ^uint64(0) to read all entries.
	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	return scanAccount(handle, ctrl.attrs, ledgerInfo.GetName(), address, ledgerInfo.GetMetadataSchema())
}

// GetLedgerStats returns aggregate statistics (account count, transaction count) for a ledger.
func (ctrl *DefaultController) GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
	_, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	var stats commonpb.LedgerStats

	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	// Count accounts from Pebble attributes zone
	accountIter, err := readstore.NewPebbleAccountIterator(handle, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("creating account iterator for stats: %w", err)
	}

	for accountIter.Next() {
		stats.AccountCount++
	}

	accountIter.Close()

	// Count transactions from Pebble cold zone
	txIter, err := readstore.NewPebbleTxIterator(handle, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("creating tx iterator for stats: %w", err)
	}

	for txIter.Next() {
		stats.TransactionCount++
	}

	txIter.Close()

	return &stats, nil
}

func (ctrl *DefaultController) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	_, span := tracer.Start(ctx, "ctrl.get_ledger",
		trace.WithAttributes(attribute.String("ledger", name)))
	defer span.End()

	ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, name)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", name)
		}

		return nil, err
	}

	// Enrich mirror ledgers with sync progress computed from Pebble state
	if ledgerInfo.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		progress, err := query.ReadMirrorSyncProgress(ctx, ctrl.store, name)
		if err != nil {
			return nil, fmt.Errorf("reading mirror sync progress: %w", err)
		}

		ledgerInfo.MirrorSyncProgress = progress
	}

	return ledgerInfo, nil
}

// GetMetadataSchemaStatus returns the conversion status of all declared metadata fields.
func (ctrl *DefaultController) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	resp := &servicepb.GetMetadataSchemaStatusResponse{
		AccountFields:     make(map[string]*servicepb.MetadataFieldStatus),
		TransactionFields: make(map[string]*servicepb.MetadataFieldStatus),
	}

	if ledgerInfo.GetMetadataSchema() != nil {
		for key, field := range ledgerInfo.GetMetadataSchema().GetAccountFields() {
			resp.AccountFields[key] = &servicepb.MetadataFieldStatus{
				DeclaredType:  field.GetType(),
				Status:        field.GetStatus(),
				TotalKeys:     field.GetTotalKeys(),
				ConvertedKeys: field.GetConvertedKeys(),
			}
		}

		for key, field := range ledgerInfo.GetMetadataSchema().GetTransactionFields() {
			resp.TransactionFields[key] = &servicepb.MetadataFieldStatus{
				DeclaredType:  field.GetType(),
				Status:        field.GetStatus(),
				TotalKeys:     field.GetTotalKeys(),
				ConvertedKeys: field.GetConvertedKeys(),
			}
		}
	}

	return resp, nil
}

// AnalyzeAccounts scans all accounts in a ledger and suggests a Chart of Accounts.
// Uses a direct Pebble key scan to extract account addresses, asset names, and
// metadata key names without reading values or going through the bbolt read index.
func (ctrl *DefaultController) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
	if _, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, fmt.Errorf("validating ledger for analysis: %w", err)
	}

	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	it, err := query.NewCompactAccountIterator(handle, ledgerName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = it.Close() }()

	return analysis.AnalyzeFromIterator(it.Next, variableThreshold, onProgress)
}

// AnalyzeTransactions scans all transactions in a ledger and discovers flow patterns.
// Uses two sequential Pebble log scans with streaming processing to avoid loading
// all transactions into memory (O(unique addresses + unique signatures) instead of O(N)).
func (ctrl *DefaultController) AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	if _, err := query.GetLedgerByName(ctx, handle, ledgerName); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, fmt.Errorf("validating ledger for analysis: %w", err)
	}

	// makeStreamIter returns an iterator function that streams CompactTransactions
	// from a fresh log scan. Each call creates a new scan cursor.
	var (
		totalReverted uint64
		pass1Done     bool
	)

	makeStreamIter := func() (func() (analysis.CompactTransaction, error), func()) {
		var (
			cursor dal.Cursor[*commonpb.Log]
			done   bool
		)

		next := func() (analysis.CompactTransaction, error) {
			if done {
				return analysis.CompactTransaction{}, io.EOF
			}
			// Lazy cursor creation on first call
			if cursor == nil {
				var err error

				cursor, err = query.ReadLogsSince(ctx, handle, 0, dal.WithReuse())
				if err != nil {
					return analysis.CompactTransaction{}, fmt.Errorf("creating log cursor for analysis: %w", err)
				}
			}

			for {
				log, err := cursor.Next()
				if errors.Is(err, io.EOF) {
					done = true

					return analysis.CompactTransaction{}, io.EOF
				}

				if err != nil {
					return analysis.CompactTransaction{}, fmt.Errorf("reading log for analysis: %w", err)
				}

				if log.GetPayload() == nil {
					continue
				}

				applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
				if !ok {
					continue
				}

				if applyLog.Apply.GetLedgerName() != ledgerName {
					continue
				}

				ledgerLog := applyLog.Apply.GetLog()
				if ledgerLog == nil || ledgerLog.GetData() == nil {
					continue
				}

				switch p := ledgerLog.GetData().GetPayload().(type) {
				case *commonpb.LedgerLogPayload_CreatedTransaction:
					if p.CreatedTransaction.GetTransaction() == nil {
						continue
					}

					return analysis.ExtractCompactTransaction(p.CreatedTransaction.GetTransaction()), nil

				case *commonpb.LedgerLogPayload_RevertedTransaction:
					// Count reverted during pass 1 only
					if !pass1Done {
						totalReverted++
					}

					if p.RevertedTransaction.GetRevertTransaction() != nil {
						return analysis.ExtractCompactTransaction(p.RevertedTransaction.GetRevertTransaction()), nil
					}

					continue
				default:
					continue
				}
			}
		}
		cleanup := func() {
			if cursor != nil {
				_ = cursor.Close()
			}
		}

		return next, cleanup
	}

	pass1, cleanup1 := makeStreamIter()
	defer cleanup1()

	pass2Fn, cleanup2 := makeStreamIter()
	defer cleanup2()

	// After pass1 completes inside AnalyzeTransactionsFromIterators, totalReverted
	// will have been counted. Mark pass1Done to avoid double counting during pass2.
	wrappedPass1 := func() (analysis.CompactTransaction, error) {
		ct, err := pass1()
		if errors.Is(err, io.EOF) {
			pass1Done = true
		}

		return ct, err
	}

	return analysis.AnalyzeTransactionsFromIterators(wrappedPass1, pass2Fn, func() uint64 { return totalReverted }, variableThreshold, onProgress)
}

// AggregateVolumes returns per-asset aggregated volumes for filtered accounts.
func (ctrl *DefaultController) AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter) (*commonpb.AggregateResult, error) {
	ctx, span := tracer.Start(ctx, "ctrl.aggregate_volumes",
		trace.WithAttributes(attribute.String("ledger", ledgerName)))
	defer span.End()

	profile := query.ProfileFromContext(ctx)

	ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	// Fast path: unfiltered aggregation scans Pebble volumes in a single pass.
	// No bbolt interaction needed — Pebble snapshot is the source of truth.
	if filter == nil {
		enrichStart := time.Now()

		result, aggErr := query.AggregateAllVolumes(handle, ctrl.attrs.Volume, ledgerInfo.GetName())
		if aggErr != nil {
			return nil, fmt.Errorf("aggregating volumes: %w", aggErr)
		}

		if profile != nil {
			profile.EnrichmentDuration = time.Since(enrichStart)
		}

		return result, nil
	}

	schemaFields := query.SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)

	var result *commonpb.AggregateResult

	err = ctrl.readStore.View(func(tx *bolt.Tx) error {
		kb := dal.NewKeyBuilder()

		indexStart := time.Now()

		iter, compileErr := query.Compile(tx, kb, filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerInfo.GetName(), nil, schemaFields, ledgerInfo.GetBuiltinIndexes(), nil, profile, handle)
		if compileErr != nil {
			return fmt.Errorf("compiling filter: %w", compileErr)
		}
		defer iter.Close()

		if profile != nil {
			profile.IndexDuration = time.Since(indexStart)
		}

		enrichStart := time.Now()

		result, err = query.AggregateVolumes(handle, ctrl.attrs.Volume, ledgerInfo.GetName(), iter)

		if profile != nil {
			profile.EnrichmentDuration = time.Since(enrichStart)
		}

		return err
	})
	if err != nil {
		return nil, fmt.Errorf("aggregating volumes: %w", err)
	}

	return result, nil
}

// ListLogs returns a cursor over logs. When filter contains a ledger condition, only logs for
// that ledger are returned using the Compile framework (supports boolean filters, date ranges).
// Otherwise all logs are returned in global sequence order, paginated by afterSequence.
func (ctrl *DefaultController) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (dal.Cursor[*commonpb.Log], error) {
	handle := ctrl.store.NewReadHandle()

	if ledger := extractLedgerFilter(filter); ledger != "" {
		// Strip the LedgerCondition from the filter tree — the ledger is set
		// as context for the Compile framework, not as a filter node.
		remainingFilter := stripLedgerFilter(filter)

		ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledger)
		if err != nil {
			_ = handle.Close()

			if errors.Is(err, domain.ErrNotFound) {
				return nil, commonpb.NewNotFoundError("ledger %s not found", ledger)
			}

			return nil, err
		}

		if pageSize == 0 {
			pageSize = math.MaxUint32
		}

		var logIDs [][]byte

		err = ctrl.readStore.View(func(tx *bolt.Tx) error {
			kb := dal.NewKeyBuilder()

			iter, compileErr := query.Compile(
				tx, kb, remainingFilter,
				commonpb.QueryTarget_QUERY_TARGET_LOGS,
				ledgerInfo.GetName(), nil, nil,
				nil, ledgerInfo.GetLogBuiltinIndexes(), nil, handle,
			)
			if compileErr != nil {
				return fmt.Errorf("compiling log filter: %w", compileErr)
			}
			defer iter.Close()

			logIDs, _ = readstore.PaginateForward(iter, pageSize, nil)

			return nil
		})
		if err != nil {
			_ = handle.Close()

			return nil, fmt.Errorf("listing ledger logs: %w", err)
		}

		var c dal.Cursor[*commonpb.Log]

		err = ctrl.readStore.View(func(tx *bolt.Tx) error {
			var readErr error
			c, readErr = query.ReadLedgerLogsCompiled(handle, tx, ledgerInfo.GetName(), logIDs)

			return readErr
		})
		if err != nil {
			_ = handle.Close()

			return nil, fmt.Errorf("reading ledger logs: %w", err)
		}

		return dal.NewClosingCursor(c, handle), nil
	}

	c, err := query.ReadLogsSince(ctx, handle, afterSequence)
	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing logs: %w", err)
	}

	cursor := dal.NewClosingCursor(c, handle)
	if pageSize > 0 {
		cursor = dal.NewLimitedCursor(cursor, pageSize)
	}

	return cursor, nil
}

// extractLedgerFilter walks a QueryFilter tree and returns the ledger name if a
// LedgerCondition with a hardcoded string value is found. Returns "" if absent.
func extractLedgerFilter(f *commonpb.QueryFilter) string {
	if f == nil {
		return ""
	}

	switch v := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Ledger:
		if v.Ledger != nil && v.Ledger.GetCond() != nil {
			if h := v.Ledger.GetCond().GetHardcoded(); h != "" {
				return h
			}
		}
	case *commonpb.QueryFilter_And:
		for _, sub := range v.And.GetFilters() {
			if l := extractLedgerFilter(sub); l != "" {
				return l
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, sub := range v.Or.GetFilters() {
			if l := extractLedgerFilter(sub); l != "" {
				return l
			}
		}
	case *commonpb.QueryFilter_Not:
		return extractLedgerFilter(v.Not.GetFilter())
	}

	return ""
}

// stripLedgerFilter returns a new filter tree with all LedgerCondition nodes
// removed. AND filters with a single remaining child are unwrapped.
// Returns nil if the entire tree was a single LedgerCondition.
func stripLedgerFilter(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	if f == nil {
		return nil
	}

	switch v := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Ledger:
		return nil
	case *commonpb.QueryFilter_And:
		var remaining []*commonpb.QueryFilter

		for _, sub := range v.And.GetFilters() {
			stripped := stripLedgerFilter(sub)
			if stripped != nil {
				remaining = append(remaining, stripped)
			}
		}

		if len(remaining) == 0 {
			return nil
		}

		if len(remaining) == 1 {
			return remaining[0]
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: remaining},
			},
		}
	case *commonpb.QueryFilter_Or:
		var remaining []*commonpb.QueryFilter

		for _, sub := range v.Or.GetFilters() {
			stripped := stripLedgerFilter(sub)
			if stripped != nil {
				remaining = append(remaining, stripped)
			}
		}

		if len(remaining) == 0 {
			return nil
		}

		if len(remaining) == 1 {
			return remaining[0]
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Or{
				Or: &commonpb.OrFilter{Filters: remaining},
			},
		}
	case *commonpb.QueryFilter_Not:
		inner := stripLedgerFilter(v.Not.GetFilter())
		if inner == nil {
			return nil
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{Filter: inner},
			},
		}
	default:
		return f
	}
}

// ListAuditEntries returns a cursor over audit entries, applying optional filters.
func (ctrl *DefaultController) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error) {
	handle := ctrl.store.NewReadHandle()

	cursor, err := query.ReadAuditEntries(ctx, handle, afterSequence)
	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing audit entries: %w", err)
	}

	var result = dal.NewClosingCursor(cursor, handle)

	if failuresOnly {
		result = dal.NewFilteredCursor(result, func(entry *auditpb.AuditEntry) bool {
			return entry.GetFailure() != nil
		})
	}

	if pageSize > 0 {
		result = dal.NewLimitedCursor(result, pageSize)
	}

	return result, nil
}

// GetLog returns a single system log by sequence number.
func (ctrl *DefaultController) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	log, err := query.ReadLogBySequence(ctx, handle, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting log %d: %w", sequence, err)
	}

	if log == nil {
		return nil, commonpb.NewNotFoundError("log %d not found", sequence)
	}

	return log, nil
}

// GetAuditEntry returns a single audit entry by sequence number.
func (ctrl *DefaultController) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	entry, err := query.ReadAuditEntry(ctx, handle, sequence)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("audit entry %d not found", sequence)
		}

		return nil, fmt.Errorf("getting audit entry %d: %w", sequence, err)
	}

	return entry, nil
}

// ListPeriods returns a cursor over all non-purged periods from the store.
func (ctrl *DefaultController) ListPeriods(ctx context.Context) (dal.Cursor[*commonpb.Period], error) {
	handle := ctrl.store.NewReadHandle()

	cursor, err := query.ReadPeriods(ctx, handle)
	if err != nil {
		_ = handle.Close()

		return nil, err
	}

	return dal.NewClosingCursor(cursor, handle), nil
}

// ListSigningKeys returns a cursor over all registered signing keys.
func (ctrl *DefaultController) ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	handle := ctrl.store.NewReadHandle()

	cursor, err := query.ReadSigningKeysCursor(ctx, handle)
	if err != nil {
		_ = handle.Close()

		return nil, err
	}

	return dal.NewClosingCursor(cursor, handle), nil
}

// ListPreparedQueries returns all prepared queries for a ledger.
func (ctrl *DefaultController) ListPreparedQueries(ctx context.Context, ledger string) ([]*commonpb.PreparedQuery, error) {
	return query.ReadPreparedQueries(ctx, ctrl.store, ledger)
}

// ExecutePreparedQuery executes a prepared query against the read index store.
func (ctrl *DefaultController) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	profile := query.ProfileFromContext(ctx)

	return query.Execute(ctx, ctrl.readStore, ctrl.store, ctrl.attrs.Volume, req, profile)
}

// GetNumscript returns a numscript by name and optional version ("" = latest).
func (ctrl *DefaultController) GetNumscript(ctx context.Context, name string, version string) (*commonpb.NumscriptInfo, error) {
	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	info, err := query.ReadNumscript(ctx, handle, name, version)
	if err != nil {
		return nil, fmt.Errorf("reading numscript %q: %w", name, err)
	}

	if info == nil {
		return nil, commonpb.NewNotFoundError("numscript %q not found", name)
	}

	return info, nil
}

// ListNumscripts returns the latest version of all numscripts.
func (ctrl *DefaultController) ListNumscripts(ctx context.Context) ([]*commonpb.NumscriptInfo, error) {
	handle := ctrl.store.NewReadHandle()

	defer func() { _ = handle.Close() }()

	return query.ReadAllNumscripts(ctx, handle)
}

// Apply applies a list of requests and returns the resulting logs.
// The controller forwards requests to the Raft admission layer.
// The FSM is responsible for interpreting orders, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency.
func (ctrl *DefaultController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	ctx, span := tracer.Start(ctx, "ctrl.apply",
		trace.WithAttributes(attribute.Int("request_count", len(requests))))
	defer span.End()

	if len(requests) == 0 {
		return nil, errors.New("at least one request is required")
	}

	logs, err := ctrl.admission.Admit(ctx, requests...)
	if err != nil {
		return nil, fmt.Errorf("applying raft requests: %w", err)
	}

	return logs, nil
}

var _ Controller = (*DefaultController)(nil)

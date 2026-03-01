package ctrl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/application/analysis"
	"github.com/formancehq/ledger-v3-poc/internal/application/preparedquery"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	bolt "go.etcd.io/bbolt"
)

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

// NewDefaultController creates a new default controller
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

// ListLedgers returns a cursor over all active (non-deleted) ledgers
func (ctrl *DefaultController) ListLedgers(_ context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := query.ReadLedgers(handle)
	if err != nil {
		_ = handle.Close()
		return nil, err
	}
	// Filter out soft-deleted ledgers, close handle when cursor closes
	filtered := dal.NewFilteredCursor(cursor, func(ledger *commonpb.LedgerInfo) bool {
		return ledger.DeletedAt == nil
	})
	return dal.NewClosingCursor(filtered, handle), nil
}

func (ctrl *DefaultController) GetTransaction(_ context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	ledgerInfo, err := query.GetLedgerByName(ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	return buildTransaction(handle, ledgerInfo.Name, transactionID, ledgerInfo.MetadataSchema)
}

// buildTransaction builds a transaction from updates and logs using the given reader.
func buildTransaction(reader dal.PebbleReader, ledger string, transactionID uint64, schema *commonpb.MetadataSchema) (*commonpb.Transaction, error) {
	updates, err := query.ReadTransactionUpdates(reader, ledger, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", transactionID, err)
	}

	return assembleTransaction(reader, transactionID, updates, schema)
}

// enforceTransactionSchema converts transaction metadata values in-place
// according to the ledger's declared metadata schema. Mirrors enforceAccountSchema.
func enforceTransactionSchema(schema *commonpb.MetadataSchema, metadata []*commonpb.Metadata) {
	if schema == nil || len(schema.TransactionFields) == 0 {
		return
	}
	for _, m := range metadata {
		fieldSchema, ok := schema.TransactionFields[m.Key]
		if !ok || m.Value == nil {
			continue
		}
		if !commonpb.TypeMatches(m.Value, fieldSchema.Type) {
			m.Value = commonpb.ConvertMetadataValue(m.Value, fieldSchema.Type)
		}
	}
}

// assembleTransaction builds a transaction from a slice of updates and a log reader.
// The updates must be in chronological order (lowest byLog first).
// If schema is non-nil, read-time type enforcement is applied to the final metadata.
func assembleTransaction(reader dal.PebbleReader, transactionID uint64, updates []*commonpb.TransactionUpdate, schema *commonpb.MetadataSchema) (*commonpb.Transaction, error) {
	var (
		sequence         uint64
		reverted         bool
		metadataToAdd    = make(map[string]*commonpb.MetadataValue)
		metadataToDelete = make(map[string]struct{})
	)
	for _, update := range updates {
		for _, updateType := range update.Updates {
			if updateType.GetTransactionInit() != nil {
				sequence = update.ByLog
			}
			if updateType.GetTransactionModificationRevert() != nil {
				reverted = true
			}
			if addMeta := updateType.GetTransactionModificationAddMetadata(); addMeta != nil {
				metadataToAdd[addMeta.Metadata.Key] = addMeta.Metadata.Value
				delete(metadataToDelete, addMeta.Metadata.Key)
			}
			if delMeta := updateType.GetTransactionModificationDeleteMetadata(); delMeta != nil {
				metadataToDelete[delMeta.Key] = struct{}{}
				delete(metadataToAdd, delMeta.Key)
			}
		}
	}

	if sequence == 0 {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	log, err := query.ReadLogBySequence(reader, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.Log == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", sequence)
	}
	ledgerLog := applyLog.Apply.Log

	var tx *commonpb.Transaction
	switch payload := ledgerLog.Data.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		tx = payload.CreatedTransaction.Transaction
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.RevertTransaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing revert transaction")
		}
		tx = payload.RevertedTransaction.RevertTransaction
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a transaction", ledgerLog.Id)
	}

	tx.Reverted = reverted
	if len(metadataToAdd) > 0 || len(metadataToDelete) > 0 {
		// Build a map from existing metadata (preserving typed values).
		existing := make(map[string]*commonpb.MetadataValue)
		if tx.Metadata != nil {
			for _, m := range tx.Metadata.Metadata {
				existing[m.Key] = m.Value
			}
		}
		for key, value := range metadataToAdd {
			existing[key] = value
		}
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
	if tx.Metadata != nil {
		enforceTransactionSchema(schema, tx.Metadata.Metadata)
	}

	return tx, nil
}

// ListTransactions returns a cursor over transactions for a ledger.
// Default order is newest-first; reverse=true gives oldest-first.
// Uses the bbolt read index for entity discovery and Pebble for enrichment.
func (ctrl *DefaultController) ListTransactions(_ context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Transaction], error) {
	ledgerInfo, err := query.GetLedgerByName(ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	if pageSize == 0 {
		pageSize = math.MaxUint32
	}

	var entityIDs [][]byte
	err = ctrl.readStore.View(func(tx *bolt.Tx) error {
		if reverse {
			// Oldest-first (ascending txID): compiled iterator is naturally ascending
			return ctrl.listTransactionsAscending(tx, ledgerInfo.Name, pageSize, afterTxID, filter, &entityIDs)
		}
		// Newest-first (descending txID) — default
		if filter != nil {
			return ctrl.listTransactionsDescFiltered(tx, ledgerInfo.Name, pageSize, afterTxID, filter, &entityIDs)
		}
		return ctrl.listTransactionsDescUnfiltered(tx, ledgerInfo.Name, pageSize, afterTxID, &entityIDs)
	})
	if err != nil {
		return nil, fmt.Errorf("listing transactions from index: %w", err)
	}

	// Enrich each transaction ID from Pebble
	handle := ctrl.store.NewReadHandle()
	txns := make([]*commonpb.Transaction, 0, len(entityIDs))
	for _, eid := range entityIDs {
		txID := binary.BigEndian.Uint64(eid)
		tx, txErr := buildTransaction(handle, ledgerInfo.Name, txID, ledgerInfo.MetadataSchema)
		if txErr != nil {
			_ = handle.Close()
			return nil, txErr
		}
		txns = append(txns, tx)
	}

	return dal.NewClosingCursor(dal.NewSliceCursor(txns), handle), nil
}

// listTransactionsAscending returns transactions in oldest-first order.
// The compiled iterator (filtered or not) is naturally ascending, so we
// paginate forward directly.
func (ctrl *DefaultController) listTransactionsAscending(tx *bolt.Tx, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, out *[][]byte) error {
	kb := readstore.NewKeyBuilder()
	iter, err := preparedquery.Compile(
		tx, kb, filter,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		ledgerName, nil,
	)
	if err != nil {
		return fmt.Errorf("compiling transaction filter: %w", err)
	}
	defer iter.Close()

	var after []byte
	if afterTxID > 0 {
		after = make([]byte, 8)
		binary.BigEndian.PutUint64(after, afterTxID)
	}
	*out, _ = readstore.PaginateForward(iter, pageSize, after)
	return nil
}

// listTransactionsDescUnfiltered uses reverse prefix iteration for newest-first ordering.
func (ctrl *DefaultController) listTransactionsDescUnfiltered(tx *bolt.Tx, ledgerName string, pageSize uint32, afterTxID uint64, out *[][]byte) error {
	b := tx.Bucket(readstore.BucketExistence)
	if b == nil {
		return nil
	}
	kb := readstore.NewKeyBuilder()
	prefix := readstore.ExistencePrefix(kb, ledgerName, readstore.NamespaceTransaction)
	iter := readstore.NewReversePrefixIterator(b.Cursor(), prefix, len(prefix), 8)

	var before []byte
	if afterTxID > 0 {
		before = make([]byte, 8)
		binary.BigEndian.PutUint64(before, afterTxID)
	}
	*out, _ = readstore.PaginateReverse(iter, pageSize, before)
	return nil
}

// listTransactionsDescFiltered compiles a filter, collects matching IDs, reverses
// them for newest-first ordering, and applies pagination.
func (ctrl *DefaultController) listTransactionsDescFiltered(tx *bolt.Tx, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, out *[][]byte) error {
	kb := readstore.NewKeyBuilder()
	iter, err := preparedquery.Compile(
		tx, kb, filter,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		ledgerName, nil,
	)
	if err != nil {
		return fmt.Errorf("compiling transaction filter: %w", err)
	}
	defer iter.Close()

	// Collect all matching IDs (forward sorted, ascending)
	var all [][]byte
	for iter.Next() {
		cp := make([]byte, len(iter.Current()))
		copy(cp, iter.Current())
		all = append(all, cp)
	}

	// Reverse for newest-first ordering
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}

	// Apply pagination: skip past afterTxID
	if afterTxID > 0 {
		afterBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(afterBytes, afterTxID)
		skip := 0
		for _, id := range all {
			if bytes.Compare(id, afterBytes) >= 0 {
				skip++
			} else {
				break
			}
		}
		all = all[skip:]
	}

	if uint32(len(all)) > pageSize {
		all = all[:pageSize]
	}

	*out = all
	return nil
}

// ListAccounts returns a cursor over accounts for a ledger.
// Default order is alphabetical (A→Z); reverse=true gives reverse-alphabetical (Z→A).
// Uses the bbolt read index for entity discovery and Pebble for enrichment.
func (ctrl *DefaultController) ListAccounts(_ context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Account], error) {
	ledgerInfo, err := query.GetLedgerByName(ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	if pageSize == 0 {
		pageSize = math.MaxUint32
	}

	var addresses [][]byte
	err = ctrl.readStore.View(func(tx *bolt.Tx) error {
		if reverse {
			return ctrl.listAccountsDescending(tx, ledgerInfo.Name, pageSize, afterAddress, filter, &addresses)
		}
		return ctrl.listAccountsAscending(tx, ledgerInfo.Name, pageSize, afterAddress, filter, &addresses)
	})
	if err != nil {
		return nil, fmt.Errorf("listing accounts from index: %w", err)
	}

	// Enrich each account from Pebble
	handle := ctrl.store.NewReadHandle()
	accounts := make([]*commonpb.Account, 0, len(addresses))
	for _, addr := range addresses {
		acc, accErr := scanAccount(handle, ctrl.attrs, ledgerInfo.Name, string(addr), ledgerInfo.MetadataSchema)
		if accErr != nil {
			_ = handle.Close()
			return nil, accErr
		}
		accounts = append(accounts, acc)
	}

	return dal.NewClosingCursor(dal.NewSliceCursor(accounts), handle), nil
}

// listAccountsAscending returns accounts in alphabetical order (A→Z).
func (ctrl *DefaultController) listAccountsAscending(tx *bolt.Tx, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, out *[][]byte) error {
	kb := readstore.NewKeyBuilder()
	iter, err := preparedquery.Compile(
		tx, kb, filter,
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		ledgerName, nil,
	)
	if err != nil {
		return fmt.Errorf("compiling account filter: %w", err)
	}
	defer iter.Close()

	var after []byte
	if afterAddress != "" {
		after = []byte(afterAddress)
	}
	*out, _ = readstore.PaginateForward(iter, pageSize, after)
	return nil
}

// listAccountsDescending returns accounts in reverse-alphabetical order (Z→A).
func (ctrl *DefaultController) listAccountsDescending(tx *bolt.Tx, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, out *[][]byte) error {
	if filter != nil {
		// Filtered: collect all ascending, reverse, then paginate manually
		kb := readstore.NewKeyBuilder()
		iter, err := preparedquery.Compile(
			tx, kb, filter,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			ledgerName, nil,
		)
		if err != nil {
			return fmt.Errorf("compiling account filter: %w", err)
		}
		defer iter.Close()

		var all [][]byte
		for iter.Next() {
			cp := make([]byte, len(iter.Current()))
			copy(cp, iter.Current())
			all = append(all, cp)
		}

		// Reverse for Z→A ordering
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}

		// Apply pagination: skip past afterAddress
		if afterAddress != "" {
			afterBytes := []byte(afterAddress)
			skip := 0
			for _, addr := range all {
				if bytes.Compare(addr, afterBytes) >= 0 {
					skip++
				} else {
					break
				}
			}
			all = all[skip:]
		}

		if uint32(len(all)) > pageSize {
			all = all[:pageSize]
		}

		*out = all
		return nil
	}

	// Unfiltered: use ReversePrefixIterator directly on the existence bucket
	b := tx.Bucket(readstore.BucketExistence)
	if b == nil {
		return nil
	}
	kb := readstore.NewKeyBuilder()
	prefix := readstore.ExistencePrefix(kb, ledgerName, readstore.NamespaceAccount)
	iter := readstore.NewReversePrefixIterator(b.Cursor(), prefix, len(prefix), 0)

	var before []byte
	if afterAddress != "" {
		before = []byte(afterAddress)
	}
	*out, _ = readstore.PaginateReverse(iter, pageSize, before)
	return nil
}

func (ctrl *DefaultController) GetAccount(_ context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	ledgerInfo, err := query.GetLedgerByName(ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	return scanAccount(handle, ctrl.attrs, ledgerInfo.Name, address, ledgerInfo.MetadataSchema)
}

func (ctrl *DefaultController) GetLedgerByName(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
	ledgerInfo, err := query.GetLedgerByName(ctrl.store, name)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", name)
		}
		return nil, err
	}

	// Enrich mirror ledgers with sync progress computed from Pebble state
	if ledgerInfo.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		progress, err := query.ReadMirrorSyncProgress(ctrl.store, name)
		if err != nil {
			return nil, fmt.Errorf("reading mirror sync progress: %w", err)
		}
		ledgerInfo.MirrorSyncProgress = progress
	}

	return ledgerInfo, nil
}

// GetMetadataSchemaStatus returns the conversion status of all declared metadata fields.
func (ctrl *DefaultController) GetMetadataSchemaStatus(_ context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	ledgerInfo, err := query.GetLedgerByName(ctrl.store, ledgerName)
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
	if ledgerInfo.MetadataSchema != nil {
		for key, field := range ledgerInfo.MetadataSchema.AccountFields {
			resp.AccountFields[key] = &servicepb.MetadataFieldStatus{
				DeclaredType:  field.Type,
				Status:        field.Status,
				TotalKeys:     field.TotalKeys,
				ConvertedKeys: field.ConvertedKeys,
			}
		}
		for key, field := range ledgerInfo.MetadataSchema.TransactionFields {
			resp.TransactionFields[key] = &servicepb.MetadataFieldStatus{
				DeclaredType:  field.Type,
				Status:        field.Status,
				TotalKeys:     field.TotalKeys,
				ConvertedKeys: field.ConvertedKeys,
			}
		}
	}
	return resp, nil
}

// AnalyzeAccounts scans all accounts in a ledger and suggests a Chart of Accounts.
func (ctrl *DefaultController) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error) {
	// Reuse ListAccounts with pageSize=0 (no limit) to get all accounts
	cursor, err := ctrl.ListAccounts(ctx, ledgerName, 0, "", nil, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close() }()

	var accounts []*commonpb.Account
	for {
		acc, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading accounts for analysis: %w", err)
		}
		accounts = append(accounts, acc)
	}

	return analysis.Analyze(accounts, variableThreshold), nil
}

// ListLogs returns a cursor over system logs.
func (ctrl *DefaultController) ListLogs(_ context.Context, afterSequence uint64, pageSize uint32) (dal.Cursor[*commonpb.Log], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := query.ReadLogsSince(handle, afterSequence)
	if err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("listing logs: %w", err)
	}

	var result = dal.NewClosingCursor(cursor, handle)
	if pageSize > 0 {
		result = dal.NewLimitedCursor(result, pageSize)
	}

	return result, nil
}

// ListAuditEntries returns a cursor over audit entries, applying optional filters.
func (ctrl *DefaultController) ListAuditEntries(_ context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := query.ReadAuditEntries(handle, afterSequence)
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
func (ctrl *DefaultController) GetLog(_ context.Context, sequence uint64) (*commonpb.Log, error) {
	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	log, err := query.ReadLogBySequence(handle, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, commonpb.NewNotFoundError("log %d not found", sequence)
	}
	return log, nil
}

// GetAuditEntry returns a single audit entry by sequence number.
func (ctrl *DefaultController) GetAuditEntry(_ context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	entry, err := query.ReadAuditEntry(handle, sequence)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("audit entry %d not found", sequence)
		}
		return nil, fmt.Errorf("getting audit entry %d: %w", sequence, err)
	}
	return entry, nil
}

// ListPeriods returns a cursor over all non-purged periods from the store.
func (ctrl *DefaultController) ListPeriods(_ context.Context) (dal.Cursor[*commonpb.Period], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := query.ReadPeriods(handle)
	if err != nil {
		_ = handle.Close()
		return nil, err
	}
	return dal.NewClosingCursor(cursor, handle), nil
}

// ListSigningKeys returns a cursor over all registered signing keys.
func (ctrl *DefaultController) ListSigningKeys(_ context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := query.ReadSigningKeysCursor(handle)
	if err != nil {
		_ = handle.Close()
		return nil, err
	}
	return dal.NewClosingCursor(cursor, handle), nil
}

// ListPreparedQueries returns all prepared queries for a ledger.
func (ctrl *DefaultController) ListPreparedQueries(_ context.Context, ledger string) ([]*commonpb.PreparedQuery, error) {
	return query.ReadPreparedQueries(ctrl.store, ledger)
}

// ExecutePreparedQuery executes a prepared query against the read index store.
func (ctrl *DefaultController) ExecutePreparedQuery(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	return preparedquery.Execute(ctrl.readStore, ctrl.store, ctrl.attrs.Volume, req)
}

// Apply applies a list of requests and returns the resulting logs.
// The controller forwards requests to the Raft admission layer.
// The FSM is responsible for interpreting orders, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency.
func (ctrl *DefaultController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	if len(requests) == 0 {
		return nil, fmt.Errorf("at least one request is required")
	}

	logs, err := ctrl.admission.Admit(ctx, requests...)
	if err != nil {
		return nil, fmt.Errorf("applying raft requests: %w", err)
	}

	return logs, nil
}

var _ Controller = (*DefaultController)(nil)

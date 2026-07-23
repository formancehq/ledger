package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

const defaultPageSize = 100

// ErrReadIndexNotCaughtUp is returned when the read index has not yet processed
// the requested minimum log sequence.
type ErrReadIndexNotCaughtUp struct {
	Requested uint64
	Current   uint64
}

func (e *ErrReadIndexNotCaughtUp) Error() string {
	return fmt.Sprintf("read index has not caught up to sequence %d (current: %d)", e.Requested, e.Current)
}

// EntityEnricher provides functions to hydrate raw entity IDs into full objects.
type EntityEnricher struct {
	EnrichAccount     func(reader dal.PebbleReader, ledgerName string, address string) (*commonpb.Account, error)
	EnrichTransaction func(ctx context.Context, reader dal.PebbleReader, ledgerName string, txID uint64) (*commonpb.Transaction, error)
}

// Execute runs a prepared query against the read index and, for
// AGGREGATE_VOLUMES mode, crosses into Pebble for volume data.
func Execute(
	ctx context.Context,
	rs *readstore.Store,
	pebbleStore *dal.Store,
	coldReader *coldstorage.ColdReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	preparedQueryAttr *attributes.Attribute[*commonpb.PreparedQuery],
	indexAttr *attributes.Attribute[*commonpb.Index],
	req *servicepb.ExecutePreparedQueryRequest,
	profile *QueryProfile,
	enricher *EntityEnricher,
) (*servicepb.ExecutePreparedQueryResponse, error) {
	ctx, span := queryTracer.Start(ctx, "query.execute_prepared",
		trace.WithAttributes(
			attribute.String("ledger", req.GetLedger()),
			attribute.String("query", req.GetQueryName()),
		))
	defer span.End()

	// Fetch ledger info for schema-based filter validation and ledger ID resolution
	ledgerInfo, err := GetLedgerByName(ctx, pebbleStore, req.GetLedger())
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrLedgerNotFound{Name: req.GetLedger()}
		}

		return nil, fmt.Errorf("reading ledger info: %w", err)
	}

	// Read the prepared query from Pebble
	pq, err := ReadPreparedQuery(ctx, preparedQueryAttr, pebbleStore, ledgerInfo.GetName(), req.GetQueryName())
	if err != nil {
		return nil, fmt.Errorf("reading prepared query: %w", err)
	}

	if pq == nil {
		return nil, &domain.BusinessError{
			Err: &domain.ErrPreparedQueryNotFound{Ledger: req.GetLedger(), Name: req.GetQueryName()},
		}
	}

	// Validate mode compatibility
	if req.GetMode() == commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES &&
		pq.GetTarget() != commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return nil, errors.New("AGGREGATE_VOLUMES mode is only valid for ACCOUNTS target queries")
	}

	// Check min_log_sequence freshness
	if req.GetMinLogSequence() > 0 {
		lastIndexed, err := rs.LastIndexedSequence()
		if err != nil {
			return nil, fmt.Errorf("reading index progress: %w", err)
		}

		if lastIndexed < req.GetMinLogSequence() {
			return nil, &ErrReadIndexNotCaughtUp{
				Requested: req.GetMinLogSequence(),
				Current:   lastIndexed,
			}
		}
	}

	schema := SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), pq.GetTarget())

	// Take a Pebble snapshot for the read index for consistent reads.
	indexSnap := rs.NewSnapshot()
	defer func() { _ = indexSnap.Close() }()

	// Always open a read handle — needed for filter compilation and entity enrichment.
	handle, err := pebbleStore.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	kb := dal.NewKeyBuilder()

	indexRegistry := NewPebbleIndexReader(indexAttr, handle)

	// Resolve the per-replica version through THE SAME snapshot used
	// for iteration. Reading the version from the live DB while the
	// scan uses indexSnap would let a concurrent atomic version switch
	// (rewrite commit) return v_new from the resolver while the
	// snapshot still holds an incomplete v_new keyspace — silent
	// partial results.
	indexVersionFor := readstore.SnapshotVersionResolver(indexSnap, ledgerInfo.GetName())

	iter, compileErr := Compile(indexSnap, kb, pq.GetFilter(), pq.GetTarget(), ledgerInfo.GetName(), req.GetParameters(), schema, ledgerInfo, indexRegistry, indexVersionFor, profile, handle)
	if compileErr != nil {
		return nil, domain.WrapCompileError(compileErr)
	}
	defer iter.Close()

	var resp *servicepb.ExecutePreparedQueryResponse

	switch req.GetMode() {
	case commonpb.QueryMode_QUERY_MODE_LIST:
		resp, err = executeList(ctx, iter, pq.GetTarget(), req, profile, handle, coldReader, indexSnap, ledgerInfo.GetName(), enricher)
		if err != nil {
			return nil, err
		}

	case commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES:
		aggResult, aggErr := AggregateVolumes(handle, volumeAttr, ledgerInfo.GetName(), iter, AggregateOptions{})
		if aggErr != nil {
			return nil, aggErr
		}

		resp = &servicepb.ExecutePreparedQueryResponse{
			Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{
				Aggregate: aggResult,
			},
		}

	default:
		return nil, fmt.Errorf("unknown query mode: %v", req.GetMode())
	}

	return resp, nil
}

// executeList paginates entities from the iterator, enriches them into full
// objects, and returns a cursor response.
func executeList(
	ctx context.Context,
	iter readstore.EntityIterator,
	target commonpb.QueryTarget,
	req *servicepb.ExecutePreparedQueryRequest,
	profile *QueryProfile,
	reader dal.PebbleReader,
	coldReader *coldstorage.ColdReader,
	indexReader dal.PebbleReader,
	ledgerName string,
	enricher *EntityEnricher,
) (*servicepb.ExecutePreparedQueryResponse, error) {
	pageSize := req.GetPageSize()
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	// Decode cursor to get the after-entity for pagination
	var afterEntity []byte

	if req.GetCursor() != "" {
		var err error

		afterEntity, err = decodeCursor(req.GetCursor())
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}
	}

	entities, hasMore, err := readstore.PaginateForward(iter, pageSize, afterEntity)
	if err != nil {
		return nil, fmt.Errorf("paginating prepared query results: %w", err)
	}

	if profile != nil {
		profile.ItemsCollected = len(entities)
	}

	if len(entities) == 0 {
		return emptyListResponse(pageSize), nil
	}

	// Build response cursor
	cursor := &commonpb.PreparedQueryCursor{
		PageSize: pageSize,
		HasMore:  hasMore,
	}

	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		accounts, err := EnrichAccounts(entities, enricher, reader, ledgerName)
		if err != nil {
			return nil, err
		}

		cursor.AccountData = accounts
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		txns, err := EnrichTransactions(ctx, entities, enricher, reader, ledgerName)
		if err != nil {
			return nil, err
		}

		cursor.TransactionData = txns
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		logs, err := EnrichLogs(reader, coldReader, indexReader, ledgerName, entities)
		if err != nil {
			return nil, err
		}

		cursor.LogData = logs
	default:
		// The compiler only produces iterators for targets the switch above
		// handles; any other target here is a wiring bug (a new target added to
		// Compile without a matching enrichment branch), not a runtime
		// condition. Fail loudly rather than return an emptily-populated cursor.
		return nil, fmt.Errorf("invariant: unsupported prepared-query list target %v", target)
	}

	if hasMore {
		cursor.Next = encodeCursor(entities[len(entities)-1])
	}

	if req.GetCursor() != "" {
		cursor.Previous = req.GetCursor()
	}

	return &servicepb.ExecutePreparedQueryResponse{
		Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
			Cursor: cursor,
		},
	}, nil
}

// EnrichAccounts hydrates a slice of raw entity bytes into full Account objects.
func EnrichAccounts(entityIDs [][]byte, enricher *EntityEnricher, reader dal.PebbleReader, ledgerName string) ([]*commonpb.Account, error) {
	accounts := make([]*commonpb.Account, len(entityIDs))
	for i, e := range entityIDs {
		acc, err := enricher.EnrichAccount(reader, ledgerName, string(e))
		if err != nil {
			return nil, fmt.Errorf("enriching account %q: %w", string(e), err)
		}

		accounts[i] = acc
	}

	return accounts, nil
}

// EnrichTransactions hydrates a slice of raw entity bytes into full Transaction objects.
func EnrichTransactions(ctx context.Context, entityIDs [][]byte, enricher *EntityEnricher, reader dal.PebbleReader, ledgerName string) ([]*commonpb.Transaction, error) {
	txns := make([]*commonpb.Transaction, len(entityIDs))
	for i, e := range entityIDs {
		txID := binary.BigEndian.Uint64(e)

		tx, err := enricher.EnrichTransaction(ctx, reader, ledgerName, txID)
		if err != nil {
			return nil, fmt.Errorf("enriching transaction %d: %w", txID, err)
		}

		txns[i] = tx
	}

	return txns, nil
}

// EnrichLogs hydrates a slice of raw logID bytes (as produced by the LOGS
// compiled iterator) into full Log objects. It mirrors the direct ListLogs
// path: the compiled iterator yields per-ledger logIDs, ReadLedgerLogsCompiled
// resolves them to global sequences via the log read-index and reads the log
// payloads from Pebble. pebbleReader reads the log payloads (Cold zone);
// indexReader resolves logID → sequence through the same snapshot used for
// iteration.
func EnrichLogs(pebbleReader dal.PebbleReader, coldReader *coldstorage.ColdReader, indexReader dal.PebbleReader, ledgerName string, logIDs [][]byte) ([]*commonpb.Log, error) {
	c, err := ReadLedgerLogsCompiled(pebbleReader, coldReader, indexReader, ledgerName, logIDs)
	if err != nil {
		return nil, fmt.Errorf("reading ledger logs: %w", err)
	}

	logs, err := cursor.Collect(c)
	if err != nil {
		return nil, fmt.Errorf("collecting ledger logs: %w", err)
	}

	return logs, nil
}

func emptyListResponse(pageSize uint32) *servicepb.ExecutePreparedQueryResponse {
	return &servicepb.ExecutePreparedQueryResponse{
		Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
			Cursor: &commonpb.PreparedQueryCursor{
				PageSize: pageSize,
			},
		},
	}
}

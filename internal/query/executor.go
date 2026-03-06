package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
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

// Execute runs a prepared query against the read index (bbolt) and, for
// AGGREGATE_VOLUMES mode, crosses into Pebble for volume data.
func Execute(
	ctx context.Context,
	rs *readstore.Store,
	pebbleStore *dal.Store,
	volumeAttr *attributes.AccumulatingAttribute[*raftcmdpb.VolumePair],
	req *servicepb.ExecutePreparedQueryRequest,
	profile *QueryProfile,
) (*servicepb.ExecutePreparedQueryResponse, error) {
	ctx, span := queryTracer.Start(ctx, "query.execute_prepared",
		trace.WithAttributes(
			attribute.String("ledger", req.GetLedger()),
			attribute.String("query", req.GetQueryName()),
		))
	defer span.End()

	// Read the prepared query from Pebble
	pq, err := ReadPreparedQuery(ctx, pebbleStore, req.GetLedger(), req.GetQueryName())
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

	// Fetch ledger info for schema-based filter validation
	ledgerInfo, err := GetLedgerByName(ctx, pebbleStore, req.GetLedger())
	if err != nil {
		return nil, fmt.Errorf("reading ledger info: %w", err)
	}

	schema := SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), pq.GetTarget())

	// Execute within a bbolt read-only transaction (MVCC snapshot)
	var resp *servicepb.ExecutePreparedQueryResponse

	err = rs.View(func(tx *bolt.Tx) error {
		kb := readstore.NewKeyBuilder()

		// Compile filter into iterator tree
		iter, compileErr := Compile(tx, kb, pq.GetFilter(), pq.GetTarget(), req.GetLedger(), req.GetParameters(), schema, ledgerInfo.GetBuiltinIndexes(), profile)
		if compileErr != nil {
			return fmt.Errorf("compiling filter: %w", compileErr)
		}
		defer iter.Close()

		switch req.GetMode() {
		case commonpb.QueryMode_QUERY_MODE_LIST:
			var listErr error

			resp, listErr = executeList(iter, pq.GetTarget(), req, profile)

			return listErr

		case commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES:
			handle := pebbleStore.NewReadHandle()

			defer func() { _ = handle.Close() }()

			aggResult, aggErr := AggregateVolumes(handle, volumeAttr, req.GetLedger(), iter)
			if aggErr != nil {
				return aggErr
			}

			resp = &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{
					Aggregate: aggResult,
				},
			}

			return nil

		default:
			return fmt.Errorf("unknown query mode: %v", req.GetMode())
		}
	})

	return resp, err
}

// executeList paginates entities from the iterator and returns a cursor response.
func executeList(
	iter readstore.EntityIterator,
	target commonpb.QueryTarget,
	req *servicepb.ExecutePreparedQueryRequest,
	profile *QueryProfile,
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

	entities, hasMore := readstore.PaginateForward(iter, pageSize, afterEntity)
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
		accounts := make([]string, len(entities))
		for i, e := range entities {
			accounts[i] = string(e)
		}

		cursor.AccountData = accounts
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		txIDs := make([]uint64, len(entities))
		for i, e := range entities {
			txIDs[i] = binary.BigEndian.Uint64(e)
		}

		cursor.TransactionData = txIDs
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

func emptyListResponse(pageSize uint32) *servicepb.ExecutePreparedQueryResponse {
	return &servicepb.ExecutePreparedQueryResponse{
		Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
			Cursor: &commonpb.PreparedQueryCursor{
				PageSize: pageSize,
			},
		},
	}
}

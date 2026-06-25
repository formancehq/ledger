package ctrl

import (
	"bytes"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// entityListParams holds the parameters for a generic entity listing.
// T is the entity identifier type ([]byte for both txIDs and account addresses).
//
// Note: there is deliberately no `indexVersionFor` field on this struct.
// listEntities builds its own resolver bound to the snapshot it
// iterates — see the comment on listEntities for why the resolver
// MUST share the iteration snapshot.
type entityListParams[T interface{ ~string | ~uint64 }] struct {
	target        commonpb.QueryTarget
	ledgerName    string
	pageSize      uint32
	after         T
	filter        *commonpb.QueryFilter
	reverse       bool
	schema        map[string]*commonpb.MetadataFieldSchema
	info          *commonpb.LedgerInfo
	profile       *query.QueryProfile
	pebbleReader  dal.PebbleReader
	indexRegistry indexes.Lookup
	// indexVersionFor is filled in by listEntities (bound to the
	// iteration snapshot); leaf compilers should never see a nil here.
	indexVersionFor func(canonical string) (uint32, error)
	// afterToBytes converts the after cursor to a byte slice for pagination.
	afterToBytes func(T) []byte
}

// entityListResult holds the result of a listEntities call.
type entityListResult struct {
	entityIDs [][]byte
}

// listEntities is the shared logic for ListTransactions, ListAccounts, and ListLogs.
// reverse=false returns natural ascending order; reverse=true returns descending.
// It returns the raw entity ID bytes along with the last indexed raft index for
// cross-store consistency.
//
// The function takes a Pebble snapshot for the iteration and builds
// `params.indexVersionFor` from THAT snapshot. Callers must not set
// the field themselves — a resolver constructed against the live store
// would race with a concurrent atomic version switch and hand the
// scan a version that does not match the snapshot's keyspace
// (silent partial results).
func listEntities[T interface{ ~string | ~uint64 }](
	readStore *readstore.Store,
	params entityListParams[T],
) (entityListResult, error) {
	var result entityListResult

	snap := readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	params.indexVersionFor = readstore.SnapshotVersionResolver(snap, params.ledgerName)

	var err error

	if params.reverse {
		if params.filter != nil {
			err = listDescFiltered(snap, params, &result.entityIDs)
		} else {
			err = listDescUnfiltered(snap, params, &result.entityIDs)
		}
	} else {
		err = listAscending(snap, params, &result.entityIDs)
	}

	return result, err
}

// listAscending returns entities in natural ascending order using the compiled iterator.
func listAscending[T interface{ ~string | ~uint64 }](indexReader dal.PebbleReader, params entityListParams[T], out *[][]byte) error {
	kb := dal.NewKeyBuilder()

	iter, err := query.Compile(
		indexReader, kb, params.filter,
		params.target,
		params.ledgerName, nil, params.schema, params.info, params.indexRegistry, params.indexVersionFor, params.profile,
		params.pebbleReader,
	)
	if err != nil {
		return domain.WrapCompileError(err)
	}
	defer iter.Close()

	var after []byte

	var zero T
	if params.after != zero {
		after = params.afterToBytes(params.after)
	}

	items, _, err := readstore.PaginateForward(iter, params.pageSize, after)
	if err != nil {
		return fmt.Errorf("paginating filtered list: %w", err)
	}

	*out = items

	return nil
}

// listDescUnfiltered uses reverse iteration on the Pebble source of truth
// (accounts, transactions, logs).
func listDescUnfiltered[T interface{ ~string | ~uint64 }](indexReader dal.PebbleReader, params entityListParams[T], out *[][]byte) error {
	var before []byte

	var zero T
	if params.after != zero {
		before = params.afterToBytes(params.after)
	}

	iter, label, kind, bucket, err := newReverseIterator(indexReader, params)
	if err != nil {
		return err
	}
	defer iter.Close()

	if params.profile != nil {
		params.profile.Root = &query.IteratorStats{
			Label:  label,
			Kind:   kind,
			Prefix: bucket,
		}
	}

	items, _, err := readstore.PaginateReverse(iter, params.pageSize, before)
	if err != nil {
		return fmt.Errorf("paginating reverse list: %w", err)
	}

	*out = items

	return nil
}

// reverseCloser wraps a ReverseIterator with a Close method.
type reverseCloser struct {
	readstore.ReverseIterator

	close func()
}

func (r *reverseCloser) Close() { r.close() }

// newReverseIterator creates the appropriate reverse iterator for the target type.
func newReverseIterator[T interface{ ~string | ~uint64 }](indexReader dal.PebbleReader, params entityListParams[T]) (iter *reverseCloser, label, kind, bucket string, err error) {
	switch params.target {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		it, itErr := readstore.NewPebbleReverseTxIterator(params.pebbleReader, params.ledgerName)
		if itErr != nil {
			return nil, "", "", "", fmt.Errorf("creating reverse tx iterator: %w", itErr)
		}

		return &reverseCloser{it, it.Close},
			fmt.Sprintf("PebbleReverseTxIterator(%s)", params.ledgerName),
			"PebbleReverseTx", "pebble:txupdate", nil

	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		it, itErr := readstore.NewPebbleReverseAccountIterator(params.pebbleReader, params.ledgerName)
		if itErr != nil {
			return nil, "", "", "", fmt.Errorf("creating reverse account iterator: %w", itErr)
		}

		return &reverseCloser{it, it.Close},
			fmt.Sprintf("PebbleReverseAccountIterator(%s)", params.ledgerName),
			"PebbleReverseAccount", "pebble:attributes", nil

	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		kb := dal.NewKeyBuilder()
		prefix := readstore.LedgerLogPrefix(kb, params.ledgerName)
		entityOffset := len(prefix)

		it, itErr := readstore.NewReversePrefixIterator(indexReader, prefix, entityOffset, 8)
		if itErr != nil {
			return nil, "", "", "", fmt.Errorf("creating reverse log iterator: %w", itErr)
		}

		return &reverseCloser{it, it.Close},
			fmt.Sprintf("ReverseLedgerLogIterator(%s)", params.ledgerName),
			"ReverseLedgerLog", "pebble:llog", nil

	default:
		return nil, "", "", "", fmt.Errorf("unsupported target for reverse: %v", params.target)
	}
}

// listDescFiltered collects all ascending results, reverses them, and paginates.
func listDescFiltered[T interface{ ~string | ~uint64 }](indexReader dal.PebbleReader, params entityListParams[T], out *[][]byte) error {
	kb := dal.NewKeyBuilder()

	iter, err := query.Compile(
		indexReader, kb, params.filter,
		params.target,
		params.ledgerName, nil, params.schema, params.info, params.indexRegistry, params.indexVersionFor, params.profile,
		params.pebbleReader,
	)
	if err != nil {
		return domain.WrapCompileError(err)
	}
	defer iter.Close()

	var all [][]byte

	for iter.Next() {
		cp := make([]byte, len(iter.Current()))
		copy(cp, iter.Current())
		all = append(all, cp)
	}

	// Reverse for descending order
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}

	// Apply pagination: skip past after cursor
	var zero T
	if params.after != zero {
		afterBytes := params.afterToBytes(params.after)
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

	if uint32(len(all)) > params.pageSize {
		all = all[:params.pageSize]
	}

	*out = all

	return nil
}

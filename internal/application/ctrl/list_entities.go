package ctrl

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// entityListParams holds the parameters for a generic entity listing.
// T is the entity identifier type ([]byte for both txIDs and account addresses).
type entityListParams[T interface{ ~string | ~uint64 }] struct {
	target        commonpb.QueryTarget
	ledger        string
	pageSize      uint32
	after         T
	filter        *commonpb.QueryFilter
	reverse       bool
	schema        map[string]*commonpb.MetadataFieldSchema
	builtinCfg    *commonpb.BuiltinIndexConfig
	logBuiltinCfg *commonpb.LogBuiltinIndexConfig
	profile       *query.QueryProfile
	pebbleReader  dal.PebbleReader
	// afterToBytes converts the after cursor to a byte slice for bbolt pagination.
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
func listEntities[T interface{ ~string | ~uint64 }](
	readStore *readstore.Store,
	params entityListParams[T],
) (entityListResult, error) {
	var result entityListResult

	err := readStore.View(func(tx *bolt.Tx) error {
		if params.reverse {
			if params.filter != nil {
				return listDescFiltered(tx, params, &result.entityIDs)
			}

			return listDescUnfiltered(tx, params, &result.entityIDs)
		}

		return listAscending(tx, params, &result.entityIDs)
	})

	return result, err
}

// listAscending returns entities in natural ascending order using the compiled iterator.
func listAscending[T interface{ ~string | ~uint64 }](tx *bolt.Tx, params entityListParams[T], out *[][]byte) error {
	kb := dal.NewKeyBuilder()

	iter, err := query.Compile(
		tx, kb, params.filter,
		params.target,
		params.ledger, nil, params.schema, params.builtinCfg, params.logBuiltinCfg, params.profile,
		params.pebbleReader,
	)
	if err != nil {
		return fmt.Errorf("compiling filter: %w", err)
	}
	defer iter.Close()

	var after []byte

	var zero T
	if params.after != zero {
		after = params.afterToBytes(params.after)
	}

	*out, _ = readstore.PaginateForward(iter, params.pageSize, after)

	return nil
}

// listDescUnfiltered uses reverse iteration on the Pebble source of truth
// (accounts, transactions) or on bbolt BucketLedgerLogs (logs).
func listDescUnfiltered[T interface{ ~string | ~uint64 }](tx *bolt.Tx, params entityListParams[T], out *[][]byte) error {
	var before []byte

	var zero T
	if params.after != zero {
		before = params.afterToBytes(params.after)
	}

	iter, label, kind, bucket, err := newReverseIterator(tx, params)
	if err != nil {
		return err
	}
	defer iter.Close()

	if params.profile != nil {
		params.profile.Root = &query.IteratorStats{
			Label:  label,
			Kind:   kind,
			Bucket: bucket,
		}
	}

	*out, _ = readstore.PaginateReverse(iter, params.pageSize, before)

	return nil
}

// reverseCloser wraps a ReverseIterator with a Close method.
type reverseCloser struct {
	readstore.ReverseIterator

	close func()
}

func (r *reverseCloser) Close() { r.close() }

// newReverseIterator creates the appropriate reverse iterator for the target type.
func newReverseIterator[T interface{ ~string | ~uint64 }](tx *bolt.Tx, params entityListParams[T]) (iter *reverseCloser, label, kind, bucket string, err error) {
	switch params.target {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		it, itErr := readstore.NewPebbleReverseTxIterator(params.pebbleReader, params.ledger)
		if itErr != nil {
			return nil, "", "", "", fmt.Errorf("creating reverse tx iterator: %w", itErr)
		}

		return &reverseCloser{it, it.Close},
			fmt.Sprintf("PebbleReverseTxIterator(%s)", params.ledger),
			"PebbleReverseTx", "pebble:txupdate", nil

	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		it, itErr := readstore.NewPebbleReverseAccountIterator(params.pebbleReader, params.ledger)
		if itErr != nil {
			return nil, "", "", "", fmt.Errorf("creating reverse account iterator: %w", itErr)
		}

		return &reverseCloser{it, it.Close},
			fmt.Sprintf("PebbleReverseAccountIterator(%s)", params.ledger),
			"PebbleReverseAccount", "pebble:attributes", nil

	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		kb := dal.NewKeyBuilder()
		prefix := readstore.LedgerLogPrefix(kb, params.ledger)
		entityOffset := len(prefix)
		b := tx.Bucket(readstore.BucketLedgerLogs)
		cursor := b.Cursor()
		it := readstore.NewReversePrefixIterator(cursor, prefix, entityOffset, 8)

		return &reverseCloser{it, func() {}},
			fmt.Sprintf("ReverseLedgerLogIterator(%s)", params.ledger),
			"ReverseLedgerLog", "bbolt:llog", nil

	default:
		return nil, "", "", "", fmt.Errorf("unsupported target for reverse: %v", params.target)
	}
}

// listDescFiltered collects all ascending results, reverses them, and paginates.
func listDescFiltered[T interface{ ~string | ~uint64 }](tx *bolt.Tx, params entityListParams[T], out *[][]byte) error {
	kb := dal.NewKeyBuilder()

	iter, err := query.Compile(
		tx, kb, params.filter,
		params.target,
		params.ledger, nil, params.schema, params.builtinCfg, params.logBuiltinCfg, params.profile,
		params.pebbleReader,
	)
	if err != nil {
		return fmt.Errorf("compiling filter: %w", err)
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

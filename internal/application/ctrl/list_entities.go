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
	target     commonpb.QueryTarget
	ledger     string
	pageSize   uint32
	after      T
	filter     *commonpb.QueryFilter
	reverse    bool
	schema     map[string]*commonpb.MetadataFieldSchema
	builtinCfg *commonpb.BuiltinIndexConfig
	profile    *query.QueryProfile
	// namespace is the readstore namespace for unfiltered reverse iteration (e.g. "a:" or "t:").
	namespace string
	// afterToBytes converts the after cursor to a byte slice for bbolt pagination.
	afterToBytes func(T) []byte
	// idLen is the fixed byte length of entity IDs (8 for transactions, 0 for variable-length accounts).
	idLen int
}

// entityListResult holds the result of a listEntities call.
type entityListResult struct {
	entityIDs    [][]byte
	maxRaftIndex uint64
}

// listEntities is the shared logic for ListTransactions and ListAccounts.
// It returns the raw entity ID bytes collected from the bbolt read index,
// along with the last indexed raft index for cross-store consistency.
func listEntities[T interface{ ~string | ~uint64 }](
	readStore *readstore.Store,
	params entityListParams[T],
) (entityListResult, error) {
	var result entityListResult

	err := readStore.View(func(tx *bolt.Tx) error {
		// Read raft index progress for cross-store consistency capping.
		var readErr error

		result.maxRaftIndex, readErr = readStore.ReadRaftIndexProgress(tx)
		if readErr != nil {
			return fmt.Errorf("reading raft index progress: %w", readErr)
		}

		if params.reverse {
			if params.target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
				// Transactions: reverse = newest-first (default). With filter we collect+reverse.
				if params.filter != nil {
					return listDescFiltered(tx, params, &result.entityIDs)
				}

				return listDescUnfiltered(tx, params, &result.entityIDs)
			}
			// Accounts: reverse = Z→A. Same logic as transactions desc.
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
		params.ledger, nil, params.schema, params.builtinCfg, params.profile,
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

// listDescUnfiltered uses reverse prefix iteration on the existence bucket.
func listDescUnfiltered[T interface{ ~string | ~uint64 }](tx *bolt.Tx, params entityListParams[T], out *[][]byte) error {
	b := tx.Bucket(readstore.BucketExistence)
	if b == nil {
		return nil
	}

	kb := dal.NewKeyBuilder()
	prefix := readstore.ExistencePrefix(kb, params.ledger, params.namespace)
	iter := readstore.NewReversePrefixIterator(b.Cursor(), prefix, len(prefix), params.idLen)

	if params.profile != nil {
		params.profile.Root = &query.IteratorStats{
			Label:  fmt.Sprintf("ReversePrefixIterator(exist:%s:%s:)", params.ledger, params.namespace),
			Kind:   "Reverse",
			Bucket: "exist",
		}
	}

	var before []byte

	var zero T
	if params.after != zero {
		before = params.afterToBytes(params.after)
	}

	*out, _ = readstore.PaginateReverse(iter, params.pageSize, before)

	return nil
}

// listDescFiltered collects all ascending results, reverses them, and paginates.
func listDescFiltered[T interface{ ~string | ~uint64 }](tx *bolt.Tx, params entityListParams[T], out *[][]byte) error {
	kb := dal.NewKeyBuilder()

	iter, err := query.Compile(
		tx, kb, params.filter,
		params.target,
		params.ledger, nil, params.schema, params.builtinCfg, params.profile,
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

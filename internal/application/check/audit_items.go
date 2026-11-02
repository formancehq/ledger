package check

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// verifyAuditItems checks physical membership independently of the hash walk:
// the hash authenticates item values, but neither their parent key nor their
// physical order-index suffix. Even zero-log items must belong to a real entry.
// Call on the same pinned snapshot as the hash walk. The sorted scan retains
// only the preceding parent lookup, so sparse hostile sequences cannot require
// absent-range enumeration or memory proportional to the number of parents.
func (c *Checker) verifyAuditItems(ctx context.Context, reader dal.PebbleReader, callback func(*servicepb.CheckStoreEvent)) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneHistory, dal.SubHistoryAuditItem},
		UpperBound: dal.ZonePrefixUpperBound(dal.ZoneHistory, dal.SubHistoryAuditItem),
	})
	if err != nil {
		return fmt.Errorf("creating audit item membership iterator: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, iter.Close()) }()
	var (
		invalid       uint64
		samples       []string
		preceding     uint64
		hasParent     bool
		parentProblem string
	)
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		var problem string
		if len(key) != 14 {
			problem = "invalid physical key length"
		} else {
			sequence := binary.BigEndian.Uint64(key[2:10])
			index := binary.BigEndian.Uint32(key[10:14])
			switch sequence {
			case 0:
				problem = "reserved parent sequence zero"
			default:
				if !hasParent || preceding != sequence {
					preceding, hasParent, parentProblem = sequence, true, ""
					parent, parentErr := query.ReadAuditEntry(ctx, reader, sequence)
					switch {
					case errors.Is(parentErr, domain.ErrNotFound):
						parentProblem = "missing parent audit entry"
					case parentErr != nil:
						return fmt.Errorf("reading audit item parent %d: %w", sequence, parentErr)
					case parent.GetSequence() != sequence:
						parentProblem = "parent key and payload sequence differ"
					}
				}
				problem = parentProblem
			}
			value, err := iter.ValueAndErr()
			if err != nil {
				return fmt.Errorf("reading audit item membership value: %w", err)
			}
			item := &auditpb.AuditItem{}
			if err := item.UnmarshalVT(value); err != nil {
				return fmt.Errorf("decoding audit item membership value: %w", err)
			}
			if item.GetOrderIndex() != index {
				if problem != "" {
					problem += "; "
				}
				problem += "physical order index and item order index differ"
			}
		}
		if problem != "" {
			invalid++
			if len(samples) < 4 {
				// Hostile keys can be arbitrarily long; bound sample size as well as count.
				samples = append(samples, fmt.Sprintf("key %x (length %d): %s", key[:min(len(key), 14)], len(key), problem))
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating audit item membership: %w", err)
	}
	if invalid != 0 {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
			fmt.Sprintf("%d audit item rows have invalid physical identity or parent membership; samples: %s", invalid, strings.Join(samples, "; ")), 0, "", "", ""))
	}

	return ctx.Err()
}

package domain

import (
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// MetadataWalk carries the visitors WalkOrderMetadata invokes. Declared before
// its consumer.
//
// Each visitor returns the failure it wants to surface; a non-nil return stops
// the walk. Neither may mutate what it is handed: the maps are aliased straight
// out of the order, and an accepted order is immutable business intent
// (invariant #10).
type MetadataWalk struct {
	// VisitMap receives one entity's metadata map. account is the account
	// address for account-scoped maps and "" for the transaction- and
	// ledger-scoped ones, so a visitor can add the account context itself.
	VisitMap func(account string, m map[string]*commonpb.MetadataValue) Describable
	// VisitKey receives a bare metadata key: the delete-metadata and
	// metadata-field-type orders carry a key with no value.
	VisitKey func(key string) Describable
}

// WalkOrderMetadata invokes walk's visitors for every place an order can carry
// metadata: the ledger-apply variants, the ledger-metadata orders, and the
// mirror-ingest entries.
//
// This is the single source of truth for *where* metadata lives in an order.
// The shape validation, the size validation, and the per-command byte
// accounting all traverse through it, so the three can never disagree about
// which maps count — a drift would either let a map through unvalidated or make
// the per-command total exclude bytes the per-entity check already saw.
func WalkOrderMetadata(order *raftcmdpb.Order, walk MetadataWalk) Describable {
	ls := order.GetLedgerScoped()
	if ls == nil {
		return nil
	}

	switch p := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return walkApplyMetadata(p.Apply, walk)
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return walk.VisitMap("", p.SaveLedgerMetadata.GetMetadata())
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return walk.VisitKey(p.DeleteLedgerMetadata.GetKey())
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return walkMirrorMetadata(p.MirrorIngest.GetEntry(), walk)
	default:
		return nil
	}
}

// walkApplyMetadata walks the metadata carried by a LedgerApplyOrder.
func walkApplyMetadata(apply *raftcmdpb.LedgerApplyOrder, walk MetadataWalk) Describable {
	switch d := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		if err := walk.VisitMap("", d.CreateTransaction.GetMetadata()); err != nil {
			return err
		}

		return walkAccountMetadata(d.CreateTransaction.GetAccountMetadata(), walk)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		// processRevertTransaction stores order.GetMetadata() straight into
		// the revert log payload, so the metadata-key invariants (non-empty,
		// no NUL bytes) must be checked here too. Without this gate a
		// client-supplied empty or NUL-bearing key reaches the canonical
		// Pebble key layout via the revert log and corrupts read-index
		// entries (#322).
		return walk.VisitMap("", d.RevertTransaction.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		return walk.VisitMap("", d.AddMetadata.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		return walk.VisitKey(d.DeleteMetadata.GetKey())
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		return walk.VisitKey(d.SetMetadataFieldType.GetKey())
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		return walk.VisitKey(d.RemoveMetadataFieldType.GetKey())
	default:
		return nil
	}
}

// walkMirrorMetadata walks the metadata supplied by mirror ingest orders.
func walkMirrorMetadata(entry *raftcmdpb.MirrorLogEntry, walk MetadataWalk) Describable {
	switch d := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		if err := walk.VisitMap("", d.CreatedTransaction.GetMetadata()); err != nil {
			return err
		}

		return walkAccountMetadata(d.CreatedTransaction.GetAccountMetadata(), walk)
	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		return walk.VisitKey(d.DeletedMetadata.GetKey())
	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		return walk.VisitMap("", d.SavedMetadata.GetMetadata())
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		return walk.VisitMap("", d.RevertedTransaction.GetMetadata())
	default:
		return nil
	}
}

// walkAccountMetadata walks the per-account maps of a transaction order. A nil
// map value carries nothing and is skipped rather than reported: an absent map
// is not a validation failure.
func walkAccountMetadata(accountMetadata map[string]*commonpb.MetadataMap, walk MetadataWalk) Describable {
	for _, account := range slices.Sorted(maps.Keys(accountMetadata)) {
		mm := accountMetadata[account]
		if mm == nil {
			continue
		}

		if err := walk.VisitMap(account, mm.GetValues()); err != nil {
			return err
		}
	}

	return nil
}

// OrderMetadataSize is the measured metadata size of one order, summed over
// every map and bare key it carries. Summation is order-independent, so the
// result does not depend on Go map iteration order.
func OrderMetadataSize(order *raftcmdpb.Order) uint64 {
	var total uint64

	// The visitors only accumulate and never fail, so the walk cannot return an
	// error here.
	_ = WalkOrderMetadata(order, MetadataWalk{
		VisitMap: func(_ string, m map[string]*commonpb.MetadataValue) Describable {
			total += MetadataMapSize(m)

			return nil
		},
		VisitKey: func(key string) Describable {
			total += uint64(len(key))

			return nil
		},
	})

	return total
}

// ValidateOrderMetadata checks the shape and size of every metadata payload.
// Account and key traversal is sorted so replicated rejection details are stable.
func ValidateOrderMetadata(order *raftcmdpb.Order, limits MetadataLimits) Describable {
	return validateOrderMetadata(order, func(metadata map[string]*commonpb.MetadataValue) Describable {
		if err := validateMetadataShape(metadata); err != nil {
			return err
		}

		return limits.ValidateMap(metadata)
	}, func(key string) Describable {
		if err := ValidateMetadataKey(key); err != nil {
			return err
		}

		return limits.ValidateKey(key)
	})
}

// ValidateOrderMetadataShape checks storage-safe keys and values before the
// caller has loaded the committed size policy.
func ValidateOrderMetadataShape(order *raftcmdpb.Order) Describable {
	return validateOrderMetadata(order, validateMetadataShape, ValidateMetadataKey)
}

func validateOrderMetadata(order *raftcmdpb.Order, validateMap func(map[string]*commonpb.MetadataValue) Describable, validateKey func(string) Describable) Describable {
	return WalkOrderMetadata(order, MetadataWalk{
		VisitMap: func(account string, metadata map[string]*commonpb.MetadataValue) Describable {
			err := validateMap(metadata)
			if err == nil || account == "" {
				return err
			}

			return &ErrAccountValidation{Account: account, Cause: err}
		},
		VisitKey: validateKey,
	})
}

func validateMetadataShape(metadata map[string]*commonpb.MetadataValue) Describable {
	for _, key := range slices.Sorted(maps.Keys(metadata)) {
		if err := ValidateMetadataKey(key); err != nil {
			return err
		}
		if err := ValidateMetadataValue(metadata[key]); err != nil {
			return &ErrMetadataKeyValidation{Key: key, Cause: err}
		}
	}

	return nil
}

// ValidateCommandMetadata validates every order's shape and entity ceilings,
// then bounds their combined maps and bare keys by the command ceiling.
// Callers wrap the returned domain error at their application boundary.
func ValidateCommandMetadata(orders []*raftcmdpb.Order, limits MetadataLimits) Describable {
	var total uint64
	for _, order := range orders {
		if err := ValidateOrderMetadata(order, limits); err != nil {
			return err
		}
		total += OrderMetadataSize(order)
	}

	return limits.ValidateCommandBytes(total)
}

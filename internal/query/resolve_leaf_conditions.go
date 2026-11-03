package query

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// These resolvers prepare predicates and owned keys without constructing or
// retaining result iterators. ID point-existence probes open and close bounded
// Pebble iterators before returning. Callers own result traversal and cleanup.
func resolveReferenceCondition(ctx *compileCtx, rc *commonpb.ReferenceCondition) (string, []byte, error) {
	if _, err := requireIndexReady(ctx,
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		"reference"); err != nil {
		return "", nil, err
	}

	value, err := resolveString(rc.GetCond(), ctx.params)
	if err != nil {
		return "", nil, err
	}

	return value, readstore.TransactionReferencePrefix(ctx.kb, ctx.ledgerName, value), nil
}

func resolveAccountHasAssetPrefix(ctx *compileCtx, c *commonpb.AccountHasAssetCondition) ([]byte, error) {
	if _, err := requireIndexReady(ctx,
		indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
		"has asset"); err != nil {
		return nil, err
	}

	return readstore.AccountByAssetPrefix(ctx.kb, ctx.ledgerName, c.GetAssetBase(), uint8(c.GetPrecision())), nil
}

func resolveStringMetadataPrefix(ctx *compileCtx, mc *metadataCtx, cond *commonpb.StringCondition) ([]byte, error) {
	value, err := resolveString(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	return readstore.EncodeString(append([]byte{}, mc.prefix...), value), nil
}

func resolveBoolMetadataPrefix(ctx *compileCtx, mc *metadataCtx, cond *commonpb.BoolCondition) ([]byte, error) {
	value, err := resolveBool(cond, ctx.params)
	if err != nil {
		return nil, err
	}

	return readstore.EncodeBool(append([]byte{}, mc.prefix...), value), nil
}

// resolveExistsPrefixes binds both existence arms to the same served version.
// A nil null prefix means only the non-null arm was requested. The prefix
// helpers copy their keys, so preparing the second arm cannot overwrite the first.
func resolveExistsPrefixes(ctx *compileCtx, mc *metadataCtx, cond *commonpb.ExistsCondition) (nonNull, null []byte) {
	nonNull = readstore.EntityExistsNonNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)
	if cond.GetIncludeNull() {
		null = readstore.EntityExistsNullPrefixV(ctx.kb, ctx.ledgerName, mc.namespace, mc.metaKey, mc.version)
	}

	return nonNull, null
}

// resolveTxIDCondition preserves the equality existence shortcut. Missing
// transactions are an empty result; a non-equality scan remains streaming.
func resolveTxIDCondition(ctx *compileCtx, cond *commonpb.UintCondition) (resolvedUintBounds, []byte, error) {
	bounds, err := resolveUintBounds(cond, ctx.params)
	if err != nil || bounds.empty || !bounds.isEquality() {
		return bounds, nil, err
	}

	exists, err := pebbleTxExists(ctx.pebbleReader, ctx.ledgerName, bounds.min)
	if err != nil {
		return resolvedUintBounds{}, nil, fmt.Errorf("checking tx existence: %w", err)
	}

	if !exists {
		bounds.empty = true

		return bounds, nil, nil
	}

	return bounds, binary.BigEndian.AppendUint64(nil, bounds.min), nil
}

func resolveLogIDPoint(ctx *compileCtx, id uint64) ([]byte, error) {
	key := readstore.LedgerLogKey(ctx.kb, ctx.ledgerName, id)
	exists, err := pebbleKeyExists(ctx.indexReader, key)
	if err != nil {
		return nil, fmt.Errorf("checking log existence: %w", err)
	}

	if !exists {
		return nil, nil
	}

	return binary.BigEndian.AppendUint64(nil, id), nil
}

// entityIDRangeBounds encodes half-open ID bounds without incrementing their
// endpoints. In particular, an unbounded upper end still includes MaxUint64.
func entityIDRangeBounds(bounds resolvedUintBounds) (lower, upper []byte) {
	if bounds.hasMin {
		lower = binary.BigEndian.AppendUint64(nil, bounds.min)
	}

	if bounds.hasMax {
		upper = binary.BigEndian.AppendUint64(nil, bounds.max)
	}

	return lower, upper
}

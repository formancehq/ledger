package query

import (
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// leaves builds the leaf iterators whose PHYSICAL traversal depends on the
// direction of the page being served — a Pebble cursor driven
// First/Next versus Last/Prev, a bitset word walked from its lowest set bit
// versus its highest, an event group resolved forwards versus backwards.
//
// It exists so that everything else about compiling a filter — predicate
// resolution, schema validation, index-readiness gating, bound computation,
// boolean composition, error wrapping, resource cleanup and profile-tree
// construction — is written ONCE, in one recursion, and cannot be correct
// ascending while being wrong or missing descending (EN-1966).
//
// Deliberately absent: the materializing range leaves. Draining a scan into a
// sorted slice is direction-free, so the shared recursion builds the
// ascending range scan and hands the one sorted result to Slice[D]. A
// descending page over a materialized range therefore costs no second
// collection.
type leaves[D readstore.Direction] interface {
	// accountUniverse is every account in the ledger.
	accountUniverse(ctx *compileCtx) (readstore.Iterator[D], error)
	// txUniverse is every transaction in the ledger.
	txUniverse(ctx *compileCtx) (readstore.Iterator[D], error)
	// logUniverse is every log in the ledger.
	logUniverse(ctx *compileCtx) (readstore.Iterator[D], error)

	// accountPrefix is every account under a chart-of-accounts prefix.
	accountPrefix(ctx *compileCtx, addrPrefix string) (readstore.Iterator[D], error)
	// addressTx is the account→transaction union for the accounts produced by
	// accounts. The union is order-insensitive, so accounts may travel in
	// either direction.
	addressTx(ctx *compileCtx, accounts readstore.Iterator[D], rolePrefix byte) readstore.Iterator[D]

	// eventResolve resolves the latest metadata event at or below the
	// reader's pin for every entity under prefix.
	eventResolve(ctx *compileCtx, prefix []byte) (readstore.Iterator[D], error)

	// prefixScan is an entity-ordered scan of one index prefix.
	prefixScan(ctx *compileCtx, prefix []byte, entityOffset, entityLen int) (readstore.Iterator[D], error)
	// stampGatedPrefix is prefixScan with the fold-sequence gate armed at the
	// reader's pin. The gate MUST be present in both directions: a row
	// written past the pin that one direction hides and the other admits is a
	// direction-dependent visibility bug, and a whole-set parity test cannot
	// see it, because both directions are compared against the same pinned
	// view.
	stampGatedPrefix(ctx *compileCtx, prefix []byte, entityOffset, entityLen int) (readstore.Iterator[D], error)

	// txRange is the transaction universe restricted to [lower, upper) on the
	// transaction id. A nil bound is open on that side.
	txRange(ctx *compileCtx, lower, upper []byte) (readstore.Iterator[D], error)

	// bitset walks the set bits of bs as 8-byte big-endian transaction ids.
	bitset(bs *bitset.Bitset) readstore.Iterator[D]
}

// ascLeaves builds the ascending leaves.
type ascLeaves struct{}

func (ascLeaves) accountUniverse(ctx *compileCtx) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewPebbleAccountIterator(ctx.pebbleReader, ctx.ledgerName)
}

func (ascLeaves) txUniverse(ctx *compileCtx) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewPebbleTxIterator(ctx.pebbleReader, ctx.ledgerName)
}

func (ascLeaves) logUniverse(ctx *compileCtx) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewLedgerLogIterator(ctx.indexReader, ctx.kb, ctx.ledgerName)
}

func (ascLeaves) accountPrefix(ctx *compileCtx, addrPrefix string) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewPebbleAccountPrefixIterator(ctx.pebbleReader, ctx.ledgerName, addrPrefix)
}

func (ascLeaves) addressTx(ctx *compileCtx, accounts readstore.Iterator[readstore.Asc], rolePrefix byte) readstore.Iterator[readstore.Asc] {
	return readstore.NewAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, accounts, rolePrefix)
}

func (ascLeaves) eventResolve(ctx *compileCtx, prefix []byte) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewEventResolveIterator(ctx.indexReader, prefix, ctx.pin)
}

func (ascLeaves) prefixScan(ctx *compileCtx, prefix []byte, entityOffset, entityLen int) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewPrefixIterator(ctx.indexReader, prefix, entityOffset, entityLen)
}

func (ascLeaves) stampGatedPrefix(ctx *compileCtx, prefix []byte, entityOffset, entityLen int) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewStampGatedPrefixIterator(ctx.indexReader, prefix, entityOffset, entityLen, ctx.pin)
}

func (ascLeaves) txRange(ctx *compileCtx, lower, upper []byte) (readstore.Iterator[readstore.Asc], error) {
	return readstore.NewPebbleTxRangeIterator(ctx.pebbleReader, ctx.ledgerName, lower, upper)
}

func (ascLeaves) bitset(bs *bitset.Bitset) readstore.Iterator[readstore.Asc] {
	return readstore.NewBitsetIterator(bs)
}

// descLeaves builds the descending leaves. Every method is the physical
// mirror of its ascending twin; nothing about what the filter MEANS lives
// here.
type descLeaves struct{}

func (descLeaves) accountUniverse(ctx *compileCtx) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewPebbleReverseAccountIterator(ctx.pebbleReader, ctx.ledgerName)
}

func (descLeaves) txUniverse(ctx *compileCtx) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewPebbleReverseTxIterator(ctx.pebbleReader, ctx.ledgerName)
}

func (descLeaves) logUniverse(ctx *compileCtx) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewReverseLedgerLogIterator(ctx.indexReader, ctx.kb, ctx.ledgerName)
}

func (descLeaves) accountPrefix(ctx *compileCtx, addrPrefix string) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewPebbleReverseAccountPrefixIterator(ctx.pebbleReader, ctx.ledgerName, addrPrefix)
}

func (descLeaves) addressTx(ctx *compileCtx, accounts readstore.Iterator[readstore.Desc], rolePrefix byte) readstore.Iterator[readstore.Desc] {
	return readstore.NewReverseAddressTxIterator(ctx.indexReader, ctx.kb, ctx.ledgerName, accounts, rolePrefix)
}

func (descLeaves) eventResolve(ctx *compileCtx, prefix []byte) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewReverseEventResolveIterator(ctx.indexReader, prefix, ctx.pin)
}

func (descLeaves) prefixScan(ctx *compileCtx, prefix []byte, entityOffset, entityLen int) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewReversePrefixIterator(ctx.indexReader, prefix, entityOffset, entityLen)
}

func (descLeaves) stampGatedPrefix(ctx *compileCtx, prefix []byte, entityOffset, entityLen int) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewStampGatedReversePrefixIterator(ctx.indexReader, prefix, entityOffset, entityLen, ctx.pin)
}

func (descLeaves) txRange(ctx *compileCtx, lower, upper []byte) (readstore.Iterator[readstore.Desc], error) {
	return readstore.NewPebbleReverseTxRangeIterator(ctx.pebbleReader, ctx.ledgerName, lower, upper)
}

func (descLeaves) bitset(bs *bitset.Bitset) readstore.Iterator[readstore.Desc] {
	return readstore.NewReverseBitsetIterator(bs)
}

var (
	_ leaves[readstore.Asc]  = ascLeaves{}
	_ leaves[readstore.Desc] = descLeaves{}
)

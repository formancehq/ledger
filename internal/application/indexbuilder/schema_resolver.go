package indexbuilder

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// schemaResolver memoizes per-batch lookups of the ledger MetadataSchema from
// the FSM Pebble zone. The indexer needs the declared type to coerce client
// values before encoding them into the forward index — the schema is FSM
// state, owned and mutated only via the Raft apply path, so the indexer reads
// it through `attributes.Ledger` instead of maintaining a parallel mutable
// cache.
//
// A fresh resolver is created at the start of each batch and discarded at
// the end. By construction, when the resolver runs the FSM has already
// applied every log in this batch (the logs are read from Pebble, which the
// FSM populates), so the cache observes a schema view at least as fresh as
// every log the batch is about to process. Forward-index entries written by
// the batch are therefore keyed under the *current* declared type — which is
// what queries expect. Older entries written under a previous declared type
// are re-encoded by the schema rewrite task.
//
// Per-batch cost: O(distinct ledgers touched) Pebble Gets, each block-cache
// served in steady state. The lookup is dominant only on cold batches.
type schemaResolver struct {
	reader dal.PebbleGetter
	attr   *attributes.Attribute[*commonpb.LedgerInfo]
	cache  map[string]*commonpb.MetadataSchema
}

// newSchemaResolver builds a resolver bound to a Pebble reader and the
// ledger attribute table. Returns nil when attrs is nil — callers tolerate
// the nil resolver via b.coerceForLedger which surfaces it as an explicit
// error rather than silently encoding under the raw type tag.
func newSchemaResolver(reader dal.PebbleGetter, attrs *attributes.Attributes) *schemaResolver {
	if attrs == nil || attrs.Ledger == nil {
		return nil
	}

	return &schemaResolver{
		reader: reader,
		attr:   attrs.Ledger,
		cache:  make(map[string]*commonpb.MetadataSchema, 4),
	}
}

// For returns the cached MetadataSchema for a ledger, populating the cache
// from FSM Pebble on first access. Returns nil when the ledger has no
// declared schema (which is the not-found path that attributes.Attribute.Get
// turns into a zero-value LedgerInfo). Any other read failure is surfaced as
// an error so the indexer batch fails loudly — silently caching nil here
// would commit forward-index entries under the raw client type and no
// downstream path would ever repair them (the schema-rewrite task fires only
// on SetMetadataFieldType, never on read failures).
func (r *schemaResolver) For(ledger string) (*commonpb.MetadataSchema, error) {
	if r == nil {
		return nil, nil
	}

	if schema, ok := r.cache[ledger]; ok {
		return schema, nil
	}

	canonicalKey := domain.LedgerKey{Name: ledger}.Bytes()

	info, err := r.attr.Get(r.reader, canonicalKey)
	if err != nil {
		return nil, fmt.Errorf("reading metadata schema for ledger %q: %w", ledger, err)
	}

	schema := info.GetMetadataSchema()
	r.cache[ledger] = schema

	return schema, nil
}

// coerceFor returns v coerced to the declared type for (target, key) on the
// resolved ledger schema. Convenience wrapper for indexer write sites.
func (r *schemaResolver) coerceFor(ledger string, target commonpb.TargetType, key string, v *commonpb.MetadataValue) (*commonpb.MetadataValue, error) {
	schema, err := r.For(ledger)
	if err != nil {
		return nil, err
	}

	return commonpb.CoerceToDeclaredType(schema, target, key, v), nil
}

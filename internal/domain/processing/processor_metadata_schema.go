package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processSetMetadataFieldType updates the declared type of a metadata field.
//
// Stored values are immutable: reads return the verbatim client bytes
// regardless of declared_type. The declared type is an index hint — it
// governs how the indexer encodes forward-index entries. Declaring a type
// is therefore O(1) and never blocks on a background converter; type
// changes can be issued back-to-back without waiting.
//
// If an index covers this field, its forward_encoding_version is bumped so
// the indexer schedules a rewrite to re-encode forward entries under the
// new declared_type.
func processSetMetadataFieldType(ledger string, order *raftcmdpb.SetMetadataFieldTypeOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	s := ctx.Scope

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	info = info.CloneVT()

	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	// The revision counts this key's declarations within one lineage; the
	// incarnation names the lineage. A retype carries the prior incarnation
	// and increments the revision; a declaration with no prior — a first
	// declaration or one after a removal deleted the entry — opens a new
	// lineage identified by this log's own ledger-log id, and restarts the
	// revision at 1. processApply stamps that id on the log it returns from
	// the same boundary slot, before incrementing it.
	_, prior := commonpb.SchemaFieldForTarget(info.GetMetadataSchema(), order.GetTargetType(), order.GetKey())

	incarnation := prior.GetIncarnation()
	if prior == nil {
		incarnation = ctx.Boundaries.GetNextLogId()
	}

	field := &commonpb.MetadataFieldSchema{
		Type:        order.GetType(),
		Revision:    prior.GetRevision() + 1,
		Incarnation: incarnation,
	}

	switch order.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		if info.MetadataSchema.AccountFields == nil {
			info.MetadataSchema.AccountFields = make(map[string]*commonpb.MetadataFieldSchema)
		}

		info.MetadataSchema.AccountFields[order.GetKey()] = field
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		if info.MetadataSchema.TransactionFields == nil {
			info.MetadataSchema.TransactionFields = make(map[string]*commonpb.MetadataFieldSchema)
		}

		info.MetadataSchema.TransactionFields[order.GetKey()] = field
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		if info.MetadataSchema.LedgerFields == nil {
			info.MetadataSchema.LedgerFields = make(map[string]*commonpb.MetadataFieldSchema)
		}

		info.MetadataSchema.LedgerFields[order.GetKey()] = field
	}

	s.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)

	// If an index covers this field, bump its forward_encoding_version. The
	// version bump is what triggers each replica to rewrite locally into the
	// new versioned keyspace.
	//
	// The Index entry lives in the bucket-scoped registry (not in
	// LedgerInfo), so we Mutate() a copy and Put it back rather than mutating
	// the cached pointer in place — the Find returns the cache's pointer.
	//
	// The cascade keys off the command envelope, never the loaded projection's
	// mutable name field — the LedgerInfo above is already Put under
	// domain.LedgerKey{Name: ledger}, so keying the index registry off
	// info.GetName() would let a divergent name split the two apart.
	id := indexes.MetadataID(order.GetTargetType(), order.GetKey())
	existing, findErr := indexes.Find(s.Indexes(), ledger, id)
	if findErr != nil {
		return nil, domain.StoreFailure("looking up index for schema change", findErr)
	}

	if existing != nil {
		updated := existing.Mutate()
		updated.ForwardEncodingVersion++
		indexes.Put(s.Indexes(), ledger, updated)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
				TargetType:  order.GetTargetType(),
				Key:         order.GetKey(),
				Type:        order.GetType(),
				Revision:    field.GetRevision(),
				Incarnation: field.GetIncarnation(),
			},
		},
	}, nil
}

// processRemoveMetadataFieldType drops the declared type for a metadata field.
// O(1) on the apply path: the field is removed from the schema and any index
// attached to it is dropped. Existing stored values are untouched (they remain
// in their original type; reads no longer coerce them).
func processRemoveMetadataFieldType(ledger string, order *raftcmdpb.RemoveMetadataFieldTypeOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	s := ctx.Scope

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	info = info.CloneVT()

	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	switch order.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		delete(info.GetMetadataSchema().GetAccountFields(), order.GetKey())
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		delete(info.GetMetadataSchema().GetTransactionFields(), order.GetKey())
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		delete(info.GetMetadataSchema().GetLedgerFields(), order.GetKey())
	}

	s.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)

	// Cascade: removing a schema field drops any index attached to it. The
	// dropped IndexID is carried in the log so the indexbuilder can purge
	// read-store entries within the same handler pass. We probe the registry
	// for an entry before deleting so the log only carries DroppedIndex when
	// an index actually existed.
	var droppedIndex *commonpb.IndexID

	// Keyed off the command envelope, never the loaded projection's mutable
	// name field (see processSetMetadataFieldType).
	id := indexes.MetadataID(order.GetTargetType(), order.GetKey())
	existing, findErr := indexes.Find(s.Indexes(), ledger, id)
	if findErr != nil {
		return nil, domain.StoreFailure("looking up index for schema removal", findErr)
	}

	if existing != nil {
		if err := indexes.Remove(s.Indexes(), ledger, id); err != nil {
			return nil, domain.StoreFailure("removing metadata field index", err)
		}
		droppedIndex = id
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
				TargetType:   order.GetTargetType(),
				Key:          order.GetKey(),
				DroppedIndex: droppedIndex,
			},
		},
	}, nil
}

// populateInitialSchema builds a MetadataSchema from initial_schema commands
// at ledger creation time. No conversion lifecycle is needed: a brand-new
// ledger has no stored values to convert.
//
// These declarations open their lineages at the ledger's first log id, the
// value a fresh LedgerBoundaries starts from — so a field declared with the
// ledger and one declared by a later apply can never share an incarnation
// with a re-declaration of themselves, which is the only comparison the
// gate makes.
func populateInitialSchema(commands []*commonpb.SetMetadataFieldTypeCommand, incarnation uint64) *commonpb.MetadataSchema {
	if len(commands) == 0 {
		return nil
	}

	schema := &commonpb.MetadataSchema{}

	for _, cmd := range commands {
		field := &commonpb.MetadataFieldSchema{Type: cmd.GetType(), Revision: 1, Incarnation: incarnation}
		switch cmd.GetTargetType() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			if schema.AccountFields == nil {
				schema.AccountFields = make(map[string]*commonpb.MetadataFieldSchema)
			}

			schema.AccountFields[cmd.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			if schema.TransactionFields == nil {
				schema.TransactionFields = make(map[string]*commonpb.MetadataFieldSchema)
			}

			schema.TransactionFields[cmd.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_LEDGER:
			if schema.LedgerFields == nil {
				schema.LedgerFields = make(map[string]*commonpb.MetadataFieldSchema)
			}

			schema.LedgerFields[cmd.GetKey()] = field
		}
	}

	return schema
}

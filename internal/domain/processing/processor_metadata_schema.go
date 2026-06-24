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
// If an index covers this field, its BuildStatus is flipped to BUILDING so
// the indexer schedules a rewrite to re-encode forward entries under the
// new declared_type.
func (p *RequestProcessor) processSetMetadataFieldType(
	ledgerName string,
	order *raftcmdpb.SetMetadataFieldTypeOrder,
	s Scope,
) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(s, ledgerName)
	if loadErr != nil {
		return nil, loadErr
	}

	info = info.CloneVT()

	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	field := &commonpb.MetadataFieldSchema{Type: order.GetType()}

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

	// If an index covers this field, flip it back to BUILDING: the existing
	// forward entries were encoded under the previous declared_type and must
	// be rewritten under the new one.
	id := indexes.MetadataID(order.GetTargetType(), order.GetKey())
	if existing := indexes.Find(info, id); existing != nil {
		existing.BuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING
	}

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
				TargetType: order.GetTargetType(),
				Key:        order.GetKey(),
				Type:       order.GetType(),
			},
		},
	}, nil
}

// processRemoveMetadataFieldType drops the declared type for a metadata field.
// O(1) on the apply path: the field is removed from the schema and any index
// attached to it is dropped. Existing stored values are untouched (they remain
// in their original type; reads no longer coerce them).
func (p *RequestProcessor) processRemoveMetadataFieldType(
	ledgerName string,
	order *raftcmdpb.RemoveMetadataFieldTypeOrder,
	s Scope,
) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(s, ledgerName)
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

	// Cascade: removing a schema field drops any index attached to it. The
	// dropped IndexID is carried in the log so the indexbuilder can purge
	// read-store entries within the same handler pass.
	var droppedIndex *commonpb.IndexID

	id := indexes.MetadataID(order.GetTargetType(), order.GetKey())
	if indexes.Remove(info, id) {
		droppedIndex = id
	}

	s.PutLedger(ledgerName, info)

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
func populateInitialSchema(commands []*commonpb.SetMetadataFieldTypeCommand) *commonpb.MetadataSchema {
	if len(commands) == 0 {
		return nil
	}

	schema := &commonpb.MetadataSchema{}

	for _, cmd := range commands {
		field := &commonpb.MetadataFieldSchema{Type: cmd.GetType()}
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

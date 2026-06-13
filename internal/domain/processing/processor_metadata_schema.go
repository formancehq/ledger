package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSetMetadataFieldType(
	ledgerName string,
	order *raftcmdpb.SetMetadataFieldTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	info = info.CloneVT()

	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	field := &commonpb.MetadataFieldSchema{
		Type:   order.GetType(),
		Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
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

	// If an index covers this field, flip it back to BUILDING for the
	// duration of the conversion: stored entries mix old and new encodings
	// until the background scan completes.
	id := indexes.MetadataID(order.GetTargetType(), order.GetKey())
	if existing := indexes.Find(info, id); existing != nil {
		existing.BuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING
	}

	s.PutLedger(ledgerName, info)

	// Both account and transaction metadata need the conversion lifecycle.
	// Account metadata triggers background scan+convert; transaction metadata
	// completes immediately (read-time enforcement handles existing data).
	s.AddMetadataConvertRequest(ledgerName, order.GetTargetType(), order.GetKey(), order.GetType())

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

func (p *RequestProcessor) processRemoveMetadataFieldType(
	ledgerName string,
	order *raftcmdpb.RemoveMetadataFieldTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	info = info.CloneVT()

	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	// Delete the field from the schema immediately. Existing stored typed
	// values (e.g., int_value) remain in Pebble; without a schema declaration,
	// reads return them as-is (no conversion enforced).
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

// enforceSchemaMap converts metadata values in-place according to the ledger's
// declared metadata schema. Values for keys with a declared type are converted
// using the conversion matrix; keys without a declared type are left as-is.
func enforceSchemaMap(schema *commonpb.MetadataSchema, targetType commonpb.TargetType, metadata map[string]*commonpb.MetadataValue) {
	if schema == nil || len(metadata) == 0 {
		return
	}

	var fields map[string]*commonpb.MetadataFieldSchema

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.GetAccountFields()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.GetTransactionFields()
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		fields = schema.GetLedgerFields()
	}

	if len(fields) == 0 {
		return
	}

	for key, value := range metadata {
		fieldSchema, ok := fields[key]
		if !ok || value == nil {
			continue
		}

		if !commonpb.TypeMatches(value, fieldSchema.GetType()) {
			metadata[key] = commonpb.ConvertMetadataValue(value, fieldSchema.GetType())
		}
	}
}

// populateInitialSchema builds a MetadataSchema from initial_schema commands
// and sets all fields to COMPLETE status (no background conversion needed for
// a brand-new ledger).
func populateInitialSchema(commands []*commonpb.SetMetadataFieldTypeCommand) *commonpb.MetadataSchema {
	if len(commands) == 0 {
		return nil
	}

	schema := &commonpb.MetadataSchema{}

	for _, cmd := range commands {
		field := &commonpb.MetadataFieldSchema{
			Type:   cmd.GetType(),
			Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
		}
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

// SchemaFieldForTarget returns the field map and field schema for the given
// target type and key. Returns nil field if the schema, field map, or key
// does not exist.
func SchemaFieldForTarget(schema *commonpb.MetadataSchema, targetType commonpb.TargetType, key string) (map[string]*commonpb.MetadataFieldSchema, *commonpb.MetadataFieldSchema) {
	if schema == nil {
		return nil, nil
	}

	var fields map[string]*commonpb.MetadataFieldSchema

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.GetAccountFields()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.GetTransactionFields()
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		fields = schema.GetLedgerFields()
	}

	if fields == nil {
		return nil, nil
	}

	fs, ok := fields[key]
	if !ok {
		return fields, nil
	}

	return fields, fs
}

package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSetMetadataFieldType(
	ledgerName string,
	order *raftcmdpb.SetMetadataFieldTypeOrder,
	s Store,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &ErrLedgerNotFound{Name: ledgerName}
	}

	if info.MetadataSchema == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	field := &commonpb.MetadataFieldSchema{
		Type:   order.Type,
		Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
	}

	switch order.TargetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		if info.MetadataSchema.AccountFields == nil {
			info.MetadataSchema.AccountFields = make(map[string]*commonpb.MetadataFieldSchema)
		}
		info.MetadataSchema.AccountFields[order.Key] = field
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		if info.MetadataSchema.TransactionFields == nil {
			info.MetadataSchema.TransactionFields = make(map[string]*commonpb.MetadataFieldSchema)
		}
		info.MetadataSchema.TransactionFields[order.Key] = field
	}

	s.PutLedger(ledgerName, info)

	// Both account and transaction metadata need the conversion lifecycle.
	// Account metadata triggers background scan+convert; transaction metadata
	// completes immediately (read-time enforcement handles existing data).
	s.AddMetadataConvertRequest(ledgerName, order.TargetType, order.Key, order.Type)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
				TargetType: order.TargetType,
				Key:        order.Key,
				Type:       order.Type,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRemoveMetadataFieldType(
	ledgerName string,
	order *raftcmdpb.RemoveMetadataFieldTypeOrder,
	s Store,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &ErrLedgerNotFound{Name: ledgerName}
	}

	if info.MetadataSchema != nil {
		switch order.TargetType {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			delete(info.MetadataSchema.AccountFields, order.Key)
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			delete(info.MetadataSchema.TransactionFields, order.Key)
		}
		s.PutLedger(ledgerName, info)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
				TargetType: order.TargetType,
				Key:        order.Key,
			},
		},
	}, nil
}

// enforceSchema converts metadata values in-place according to the ledger's
// declared metadata schema. Values for keys with a declared type are converted
// using the conversion matrix; keys without a declared type are left as-is.
func enforceSchema(schema *commonpb.MetadataSchema, targetType commonpb.TargetType, metadata []*commonpb.Metadata) {
	if schema == nil || len(metadata) == 0 {
		return
	}

	var fields map[string]*commonpb.MetadataFieldSchema
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.AccountFields
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.TransactionFields
	}

	if len(fields) == 0 {
		return
	}

	for _, m := range metadata {
		fieldSchema, ok := fields[m.Key]
		if !ok || m.Value == nil {
			continue
		}
		if !commonpb.TypeMatches(m.Value, fieldSchema.Type) {
			m.Value = commonpb.ConvertMetadataValue(m.Value, fieldSchema.Type)
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
			Type:   cmd.Type,
			Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
		}
		switch cmd.TargetType {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			if schema.AccountFields == nil {
				schema.AccountFields = make(map[string]*commonpb.MetadataFieldSchema)
			}
			schema.AccountFields[cmd.Key] = field
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			if schema.TransactionFields == nil {
				schema.TransactionFields = make(map[string]*commonpb.MetadataFieldSchema)
			}
			schema.TransactionFields[cmd.Key] = field
		}
	}
	return schema
}

// schemaFieldForTarget returns the field map and field schema for the given
// target type and key. Returns nil field if the schema, field map, or key
// does not exist.
func schemaFieldForTarget(schema *commonpb.MetadataSchema, targetType commonpb.TargetType, key string) (map[string]*commonpb.MetadataFieldSchema, *commonpb.MetadataFieldSchema) {
	if schema == nil {
		return nil, nil
	}
	var fields map[string]*commonpb.MetadataFieldSchema
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = schema.AccountFields
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = schema.TransactionFields
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

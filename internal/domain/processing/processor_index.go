package processing

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processCreateIndex(ledger string, order *raftcmdpb.CreateIndexOrder, ctx *Context) (*ledgerpb.LedgerLogPayload, domain.SerializableError) {
	info, loadErr := loadLedgerReader(ctx.Scope, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	id := order.GetId()
	existing, err := indexes.Find(ctx.Scope.Indexes(), ledger, id)
	if err != nil {
		return nil, domain.StoreFailure("loading index", err)
	}
	if existing != nil {
		return nil, &domain.ErrIndexAlreadyExists{Index: indexes.Canonical(id)}
	}

	if err := validateIndexTarget(info, id); err != nil {
		return nil, err
	}

	// The registry is keyed off the command envelope, never the loaded
	// projection's mutable name field, so a divergent LedgerInfo.name cannot
	// redirect the write to another ledger's index keys. The Ledger field
	// below carries the same envelope value, keeping key and payload
	// consistent. The existence check above rejects a fresh duplicate before
	// any registry mutation or CreatedIndexLog is produced.
	boundType, boundTypeDeclared := indexBoundType(info, id)

	indexes.Put(ctx.Scope.Indexes(), ledger, &ledgerpb.Index{
		Id:        id,
		CreatedAt: ctx.Scope.GetDate().Mutate(),
		Ledger:    ledger,
		// First version each replica will build into when the initial
		// backfill runs (cf. EN-1323 per-replica versioning).
		ForwardEncodingVersion: 1,
	})

	return buildCreatedIndexLogPayload(id, boundType, boundTypeDeclared), nil
}

func processDropIndex(ledger string, order *raftcmdpb.DropIndexOrder, ctx *Context) (*ledgerpb.LedgerLogPayload, domain.SerializableError) {
	// The loaded projection is only needed to validate that the ledger exists
	// and is not soft-deleted; the registry key comes from the envelope below.
	if _, loadErr := loadLedgerReader(ctx.Scope, ledger); loadErr != nil {
		return nil, loadErr
	}

	id := order.GetId()
	// Key off the command envelope, never the loaded projection's mutable
	// name field (see processCreateIndex).
	if err := indexes.Remove(ctx.Scope.Indexes(), ledger, id); err != nil {
		return nil, domain.StoreFailure("dropping index", err)
	}

	return &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_DropIndex{
			DropIndex: &ledgerpb.DroppedIndexLog{Id: id},
		},
	}, nil
}

// validateIndexTarget enforces invariants on what an IndexID can refer to
// before an Index entry is persisted. Built-in indexes are always valid by
// virtue of the enum; metadata indexes require that the schema field has been
// declared with SetMetadataFieldType first.
func validateIndexTarget(info ledgerpb.LedgerInfoReader, id *ledgerpb.IndexID) domain.SerializableError {
	if id == nil {
		return nil
	}

	meta, ok := id.GetKind().(*ledgerpb.IndexID_Metadata)
	if !ok {
		return nil
	}

	field, _ := schemaFieldForTarget(info, meta.Metadata.GetTarget(), meta.Metadata.GetKey())
	if field == nil {
		return &domain.ErrMetadataFieldNotInSchema{
			Target: meta.Metadata.GetTarget().String(),
			Key:    meta.Metadata.GetKey(),
		}
	}

	return nil
}

// schemaFieldForTarget is the reader-based twin of
// ledgerpb.SchemaFieldForTarget: it resolves the declared metadata field
// for (targetType, key) from the immutable LedgerInfo reader without
// cloning the schema.
func schemaFieldForTarget(info ledgerpb.LedgerInfoReader, targetType ledgerpb.TargetType, key string) (ledgerpb.MetadataFieldSchemaReader, bool) {
	if info == nil {
		return nil, false
	}

	schema := info.GetMetadataSchema()
	if schema == nil {
		return nil, false
	}

	var field ledgerpb.MetadataFieldSchemaReader

	switch targetType {
	case ledgerpb.TargetType_TARGET_TYPE_ACCOUNT:
		field, _ = schema.GetAccountFields().Get(key)
	case ledgerpb.TargetType_TARGET_TYPE_TRANSACTION:
		field, _ = schema.GetTransactionFields().Get(key)
	case ledgerpb.TargetType_TARGET_TYPE_LEDGER:
		field, _ = schema.GetLedgerFields().Get(key)
	}

	return field, field != nil
}

// indexBoundType resolves the declared type the index's first version binds
// to: the schema entry for the indexed metadata field at this point in the
// order stream. The log carries the binding so replicas folding it at any
// replay distance bind the same type (see CreatedIndexLog.bound_type).
// Builtin indexes have no metadata field and carry no binding.
func indexBoundType(info ledgerpb.LedgerInfoReader, id *ledgerpb.IndexID) (ledgerpb.MetadataType, bool) {
	meta, ok := id.GetKind().(*ledgerpb.IndexID_Metadata)
	if !ok || meta.Metadata == nil {
		return 0, false
	}

	field, ok := schemaFieldForTarget(info, meta.Metadata.GetTarget(), meta.Metadata.GetKey())
	if !ok {
		return 0, false
	}

	return field.GetType(), true
}

func buildCreatedIndexLogPayload(id *ledgerpb.IndexID, boundType ledgerpb.MetadataType, boundTypeDeclared bool) *ledgerpb.LedgerLogPayload {
	return &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &ledgerpb.CreatedIndexLog{
				Id:                id,
				BoundType:         boundType,
				BoundTypeDeclared: boundTypeDeclared,
			},
		},
	}
}

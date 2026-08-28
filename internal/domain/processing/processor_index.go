package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processCreateIndex(ledger string, order *raftcmdpb.CreateIndexOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(ctx.Scope, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	id := order.GetId()
	if err := validateIndexTarget(info, id); err != nil {
		return nil, err
	}

	// The registry is keyed off the command envelope, never the loaded
	// projection's mutable name field, so a divergent LedgerInfo.name cannot
	// redirect the write to another ledger's index keys. The Ledger field
	// below carries the same envelope value, keeping key and payload
	// consistent. A duplicate CreateIndex overwrites the row; the
	// indexbuilder's handleCreatedIndexLog guards against re-scheduling a
	// backfill by consulting its per-replica IndexVersionState.
	indexes.Put(ctx.Scope.Indexes(), ledger, &commonpb.Index{
		Id:        id,
		CreatedAt: ctx.Scope.GetDate().Mutate(),
		Ledger:    ledger,
		// First version each replica will build into when the initial
		// backfill runs (cf. EN-1323 per-replica versioning).
		ForwardEncodingVersion: 1,
	})

	return buildCreatedIndexLogPayload(id, ctx.isBornEmpty(ledger)), nil
}

func processDropIndex(ledger string, order *raftcmdpb.DropIndexOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	// The loaded projection is only needed to validate that the ledger exists
	// and is not soft-deleted; the registry key comes from the envelope below.
	if _, loadErr := loadLedger(ctx.Scope, ledger); loadErr != nil {
		return nil, loadErr
	}

	id := order.GetId()
	// Key off the command envelope, never the loaded projection's mutable
	// name field (see processCreateIndex).
	if err := indexes.Remove(ctx.Scope.Indexes(), ledger, id); err != nil {
		return nil, domain.StoreFailure("dropping index", err)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DropIndex{
			DropIndex: &commonpb.DroppedIndexLog{Id: id},
		},
	}, nil
}

// validateIndexTarget enforces invariants on what an IndexID can refer to
// before an Index entry is persisted. Built-in indexes are always valid by
// virtue of the enum; metadata indexes require that the schema field has been
// declared with SetMetadataFieldType first.
func validateIndexTarget(info *commonpb.LedgerInfo, id *commonpb.IndexID) domain.Describable {
	if id == nil {
		return nil
	}

	meta, ok := id.GetKind().(*commonpb.IndexID_Metadata)
	if !ok {
		return nil
	}

	_, field := commonpb.SchemaFieldForTarget(info.GetMetadataSchema(), meta.Metadata.GetTarget(), meta.Metadata.GetKey())
	if field == nil {
		return &domain.ErrMetadataFieldNotInSchema{
			Target: meta.Metadata.GetTarget().String(),
			Key:    meta.Metadata.GetKey(),
		}
	}

	return nil
}

func buildCreatedIndexLogPayload(id *commonpb.IndexID, initial bool) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &commonpb.CreatedIndexLog{Id: id, Initial: initial},
		},
	}
}

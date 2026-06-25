package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateIndex(
	ledgerName string,
	order *raftcmdpb.CreateIndexOrder,
	s Scope,
) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(s, ledgerName)
	if loadErr != nil {
		return nil, loadErr
	}

	id := order.GetId()
	if err := validateIndexTarget(info, id); err != nil {
		return nil, err
	}

	// Short-circuit when an index is already present and ready: the registry
	// entry is left untouched (no Pebble write, no BuildStatus regression)
	// but processApply still wraps the returned payload into a LogPayload_Apply
	// — so a CreatedIndexLog IS appended to the ledger log. The indexbuilder's
	// handleCreatedIndexLog must then guard against re-scheduling a backfill
	// by consulting the registry (cfg.byCanonical alone can lag behind the
	// applied READY state).
	existing, findErr := indexes.Find(s, info.GetName(), id)
	if findErr != nil {
		return nil, &domain.ErrStorageOperation{Operation: "looking up existing index", Cause: findErr}
	}

	if existing != nil && existing.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
		return buildCreatedIndexLogPayload(id), nil
	}

	indexes.Put(s, info.GetName(), &commonpb.Index{
		Id:          id,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
		CreatedAt:   s.GetDate(),
		Ledger:      ledgerName,
	})

	return buildCreatedIndexLogPayload(id), nil
}

func (p *RequestProcessor) processDropIndex(
	ledgerName string,
	order *raftcmdpb.DropIndexOrder,
	s Scope,
) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(s, ledgerName)
	if loadErr != nil {
		return nil, loadErr
	}

	id := order.GetId()
	indexes.Remove(s, info.GetName(), id)

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

func buildCreatedIndexLogPayload(id *commonpb.IndexID) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &commonpb.CreatedIndexLog{Id: id},
		},
	}
}

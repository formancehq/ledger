package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateIndex(
	ledgerName string,
	order *raftcmdpb.CreateIndexOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	logPayload := &commonpb.CreateIndexLog{}

	switch idx := order.Index.(type) {
	case *raftcmdpb.CreateIndexOrder_AddressRole:
		if info.AddressIndexes == nil {
			info.AddressIndexes = &commonpb.AddressIndexConfig{}
		}
		// Idempotent: if already indexed+READY, no-op
		if isAddressRoleIndexedAndReady(info.AddressIndexes, idx.AddressRole) {
			return buildCreateIndexLogPayload(logPayload), nil
		}
		setAddressRoleIndexed(info.AddressIndexes, idx.AddressRole, true, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING)
		logPayload.Index = &commonpb.CreateIndexLog_AddressRole{AddressRole: idx.AddressRole}

	case *raftcmdpb.CreateIndexOrder_Metadata:
		if info.MetadataSchema == nil {
			info.MetadataSchema = &commonpb.MetadataSchema{}
		}
		fields, field := schemaFieldForTarget(info.MetadataSchema, idx.Metadata.Target, idx.Metadata.Key)
		if field != nil && field.Indexed && field.IndexBuildStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
			return buildCreateIndexLogPayload(logPayload), nil
		}
		if field == nil {
			field = &commonpb.MetadataFieldSchema{
				Type: commonpb.MetadataType_METADATA_TYPE_STRING,
			}
			if fields == nil {
				fields = make(map[string]*commonpb.MetadataFieldSchema)
				switch idx.Metadata.Target {
				case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
					info.MetadataSchema.AccountFields = fields
				case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
					info.MetadataSchema.TransactionFields = fields
				}
			}
			fields[idx.Metadata.Key] = field
		}
		field.Indexed = true
		field.IndexBuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING
		logPayload.Index = &commonpb.CreateIndexLog_Metadata{Metadata: idx.Metadata}
	}

	s.PutLedger(ledgerName, info)

	return buildCreateIndexLogPayload(logPayload), nil
}

func (p *RequestProcessor) processDropIndex(
	ledgerName string,
	order *raftcmdpb.DropIndexOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	logPayload := &commonpb.DropIndexLog{}

	switch idx := order.Index.(type) {
	case *raftcmdpb.DropIndexOrder_AddressRole:
		if info.AddressIndexes != nil {
			setAddressRoleIndexed(info.AddressIndexes, idx.AddressRole, false, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED)
		}
		logPayload.Index = &commonpb.DropIndexLog_AddressRole{AddressRole: idx.AddressRole}

	case *raftcmdpb.DropIndexOrder_Metadata:
		_, field := schemaFieldForTarget(info.MetadataSchema, idx.Metadata.Target, idx.Metadata.Key)
		if field != nil {
			field.Indexed = false
			field.IndexBuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED
		}
		logPayload.Index = &commonpb.DropIndexLog_Metadata{Metadata: idx.Metadata}
	}

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DropIndex{
			DropIndex: logPayload,
		},
	}, nil
}

func (p *RequestProcessor) processIndexReady(
	ledgerName string,
	order *raftcmdpb.IndexReadyOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	logPayload := &commonpb.IndexReadyLog{}

	switch idx := order.Index.(type) {
	case *raftcmdpb.IndexReadyOrder_AddressRole:
		if info.AddressIndexes != nil {
			setAddressRoleStatus(info.AddressIndexes, idx.AddressRole, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
		}
		logPayload.Index = &commonpb.IndexReadyLog_AddressRole{AddressRole: idx.AddressRole}

	case *raftcmdpb.IndexReadyOrder_Metadata:
		_, field := schemaFieldForTarget(info.MetadataSchema, idx.Metadata.Target, idx.Metadata.Key)
		if field != nil {
			field.IndexBuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
		}
		logPayload.Index = &commonpb.IndexReadyLog_Metadata{Metadata: idx.Metadata}
	}

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_IndexReady{
			IndexReady: logPayload,
		},
	}, nil
}

func buildCreateIndexLogPayload(log *commonpb.CreateIndexLog) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: log,
		},
	}
}

func isAddressRoleIndexedAndReady(cfg *commonpb.AddressIndexConfig, role commonpb.AddressRole) bool {
	if cfg == nil {
		return false
	}
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_ANY:
		return cfg.Address && cfg.AddressStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return cfg.Source && cfg.SourceStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return cfg.Destination && cfg.DestinationStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	}
	return false
}

func setAddressRoleIndexed(cfg *commonpb.AddressIndexConfig, role commonpb.AddressRole, enabled bool, status commonpb.IndexBuildStatus) {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_ANY:
		cfg.Address = enabled
		cfg.AddressStatus = status
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		cfg.Source = enabled
		cfg.SourceStatus = status
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		cfg.Destination = enabled
		cfg.DestinationStatus = status
	}
}

func setAddressRoleStatus(cfg *commonpb.AddressIndexConfig, role commonpb.AddressRole, status commonpb.IndexBuildStatus) {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_ANY:
		cfg.AddressStatus = status
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		cfg.SourceStatus = status
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		cfg.DestinationStatus = status
	}
}

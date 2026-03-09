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

	switch idx := order.GetIndex().(type) {
	case *raftcmdpb.CreateIndexOrder_Transaction:
		switch kind := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			if info.GetBuiltinIndexes() == nil {
				info.BuiltinIndexes = &commonpb.BuiltinIndexConfig{}
			}

			if isBuiltinIndexedAndReady(info.GetBuiltinIndexes(), kind.Builtin) {
				return buildCreateIndexLogPayload(logPayload), nil
			}

			setBuiltinIndexed(info.GetBuiltinIndexes(), kind.Builtin, true, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING)

		case *commonpb.TransactionIndex_MetadataKey:
			if alreadyReady := processCreateMetadataIndex(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, kind.MetadataKey); alreadyReady {
				return buildCreateIndexLogPayload(logPayload), nil
			}
		}

		logPayload.Index = &commonpb.CreateIndexLog_Transaction{Transaction: idx.Transaction}

	case *raftcmdpb.CreateIndexOrder_Account:
		switch kind := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			// No account builtins yet; ignore.
			_ = kind

		case *commonpb.AccountIndex_MetadataKey:
			if alreadyReady := processCreateMetadataIndex(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, kind.MetadataKey); alreadyReady {
				return buildCreateIndexLogPayload(logPayload), nil
			}
		}

		logPayload.Index = &commonpb.CreateIndexLog_Account{Account: idx.Account}

	case *raftcmdpb.CreateIndexOrder_LogBuiltin:
		if info.GetLogBuiltinIndexes() == nil {
			info.LogBuiltinIndexes = &commonpb.LogBuiltinIndexConfig{}
		}

		if isLogBuiltinIndexedAndReady(info.GetLogBuiltinIndexes(), idx.LogBuiltin) {
			return buildCreateIndexLogPayload(logPayload), nil
		}

		setLogBuiltinIndexed(info.GetLogBuiltinIndexes(), idx.LogBuiltin, true, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING)
		logPayload.Index = &commonpb.CreateIndexLog_LogBuiltin{LogBuiltin: idx.LogBuiltin}
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

	switch idx := order.GetIndex().(type) {
	case *raftcmdpb.DropIndexOrder_Transaction:
		switch kind := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			if info.GetBuiltinIndexes() != nil {
				setBuiltinIndexed(info.GetBuiltinIndexes(), kind.Builtin, false, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED)
			}

		case *commonpb.TransactionIndex_MetadataKey:
			processDropMetadataIndex(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, kind.MetadataKey)
		}

		logPayload.Index = &commonpb.DropIndexLog_Transaction{Transaction: idx.Transaction}

	case *raftcmdpb.DropIndexOrder_Account:
		switch kind := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			// No account builtins yet; ignore.
			_ = kind

		case *commonpb.AccountIndex_MetadataKey:
			processDropMetadataIndex(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, kind.MetadataKey)
		}

		logPayload.Index = &commonpb.DropIndexLog_Account{Account: idx.Account}

	case *raftcmdpb.DropIndexOrder_LogBuiltin:
		if info.GetLogBuiltinIndexes() != nil {
			setLogBuiltinIndexed(info.GetLogBuiltinIndexes(), idx.LogBuiltin, false, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED)
		}

		logPayload.Index = &commonpb.DropIndexLog_LogBuiltin{LogBuiltin: idx.LogBuiltin}
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

	switch idx := order.GetIndex().(type) {
	case *raftcmdpb.IndexReadyOrder_Transaction:
		switch kind := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			if info.GetBuiltinIndexes() != nil {
				setBuiltinStatus(info.GetBuiltinIndexes(), kind.Builtin, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
			}

		case *commonpb.TransactionIndex_MetadataKey:
			processIndexReadyMetadata(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, kind.MetadataKey)
		}

		logPayload.Index = &commonpb.IndexReadyLog_Transaction{Transaction: idx.Transaction}

	case *raftcmdpb.IndexReadyOrder_Account:
		switch kind := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			// No account builtins yet; ignore.
			_ = kind

		case *commonpb.AccountIndex_MetadataKey:
			processIndexReadyMetadata(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, kind.MetadataKey)
		}

		logPayload.Index = &commonpb.IndexReadyLog_Account{Account: idx.Account}

	case *raftcmdpb.IndexReadyOrder_LogBuiltin:
		if info.GetLogBuiltinIndexes() != nil {
			setLogBuiltinStatus(info.GetLogBuiltinIndexes(), idx.LogBuiltin, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
		}

		logPayload.Index = &commonpb.IndexReadyLog_LogBuiltin{LogBuiltin: idx.LogBuiltin}
	}

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_IndexReady{
			IndexReady: logPayload,
		},
	}, nil
}

// processCreateMetadataIndex handles the metadata index creation logic shared
// by both transaction and account index types. It returns true if the index is
// already built and ready (i.e. no log entry is needed).
func processCreateMetadataIndex(info *commonpb.LedgerInfo, target commonpb.TargetType, key string) bool {
	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	fields, field := schemaFieldForTarget(info.GetMetadataSchema(), target, key)
	if field != nil && field.GetIndexed() && field.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
		return true
	}

	if field == nil {
		field = &commonpb.MetadataFieldSchema{
			Type: commonpb.MetadataType_METADATA_TYPE_STRING,
		}

		if fields == nil {
			fields = make(map[string]*commonpb.MetadataFieldSchema)

			switch target {
			case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
				info.MetadataSchema.AccountFields = fields
			case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
				info.MetadataSchema.TransactionFields = fields
			}
		}

		fields[key] = field
	}

	field.Indexed = true
	field.IndexBuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING

	return false
}

// processDropMetadataIndex handles the metadata index removal logic shared
// by both transaction and account index types.
func processDropMetadataIndex(info *commonpb.LedgerInfo, target commonpb.TargetType, key string) {
	_, field := schemaFieldForTarget(info.GetMetadataSchema(), target, key)
	if field != nil {
		field.Indexed = false
		field.IndexBuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED
	}
}

// processIndexReadyMetadata handles the metadata index ready notification logic
// shared by both transaction and account index types.
func processIndexReadyMetadata(info *commonpb.LedgerInfo, target commonpb.TargetType, key string) {
	_, field := schemaFieldForTarget(info.GetMetadataSchema(), target, key)
	if field != nil {
		field.IndexBuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	}
}

func buildCreateIndexLogPayload(log *commonpb.CreateIndexLog) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: log,
		},
	}
}

func isBuiltinIndexedAndReady(cfg *commonpb.BuiltinIndexConfig, builtin commonpb.TransactionBuiltinIndex) bool {
	if cfg == nil {
		return false
	}

	switch builtin {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
		return cfg.GetReference() && cfg.GetReferenceStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		return cfg.GetTimestamp() && cfg.GetTimestampStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
		return cfg.GetAddress() && cfg.GetAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
		return cfg.GetSourceAddress() && cfg.GetSourceAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
		return cfg.GetDestAddress() && cfg.GetDestAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	}

	return false
}

func setBuiltinIndexed(cfg *commonpb.BuiltinIndexConfig, builtin commonpb.TransactionBuiltinIndex, enabled bool, status commonpb.IndexBuildStatus) {
	switch builtin {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
		cfg.Reference = enabled
		cfg.ReferenceStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		cfg.Timestamp = enabled
		cfg.TimestampStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
		cfg.Address = enabled
		cfg.AddressStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
		cfg.SourceAddress = enabled
		cfg.SourceAddressStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
		cfg.DestAddress = enabled
		cfg.DestAddressStatus = status
	}
}

func setBuiltinStatus(cfg *commonpb.BuiltinIndexConfig, builtin commonpb.TransactionBuiltinIndex, status commonpb.IndexBuildStatus) {
	switch builtin {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
		cfg.ReferenceStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		cfg.TimestampStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
		cfg.AddressStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
		cfg.SourceAddressStatus = status
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
		cfg.DestAddressStatus = status
	}
}

func isLogBuiltinIndexedAndReady(cfg *commonpb.LogBuiltinIndexConfig, builtin commonpb.LogBuiltinIndex) bool {
	if cfg == nil {
		return false
	}

	switch builtin {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER:
		return cfg.GetLedger() && cfg.GetLedgerStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		return cfg.GetDate() && cfg.GetDateStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	}

	return false
}

func setLogBuiltinIndexed(cfg *commonpb.LogBuiltinIndexConfig, builtin commonpb.LogBuiltinIndex, enabled bool, status commonpb.IndexBuildStatus) {
	switch builtin {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER:
		cfg.Ledger = enabled
		cfg.LedgerStatus = status
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		cfg.Date = enabled
		cfg.DateStatus = status
	}
}

func setLogBuiltinStatus(cfg *commonpb.LogBuiltinIndexConfig, builtin commonpb.LogBuiltinIndex, status commonpb.IndexBuildStatus) {
	switch builtin {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER:
		cfg.LedgerStatus = status
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		cfg.DateStatus = status
	}
}

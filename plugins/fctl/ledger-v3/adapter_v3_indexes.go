package ledgerv3

import (
	"context"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func executeV3Indexes(ctx context.Context, _ sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch command.ID {
	case "ledger.v3.indexes.create", "ledger.v3.indexes.drop":
		id, err := parseIndexID(decoded)
		if err != nil {
			return true, err
		}
		var request *servicepb.Request
		operation := opApplyCreateIndex.id
		if command.ID == "ledger.v3.indexes.create" {
			request = &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: decoded.text(argLedger), Id: id}}}
		} else {
			operation = opApplyDropIndex.id
			request = &servicepb.Request{Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{Ledger: decoded.text(argLedger), Id: id}}}
		}
		_, err = applyV3(ctx, host, command, operation, decoded.text(flagIdempotencyKey), request)
		if err != nil {
			return true, err
		}
		return true, emitEmpty(host, operation)
	case "ledger.v3.indexes.inspect":
		target, err := commonpb.ParseTargetType(decoded.text(flagTargetType))
		if err != nil {
			return true, invalidArgument("invalid target type")
		}
		checkpoint, _, err := decoded.optionalUint64(flagCheckpointID)
		if err != nil {
			return true, err
		}
		mode := servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES
		switch decoded.text(flagInspectMode) {
		case "facets":
			mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS
		case "summary":
			mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY
		}
		response, err := unaryV3(ctx, host, command, opInspectIndex.id, &servicepb.InspectIndexRequest{Ledger: decoded.text(argLedger), TargetType: target, MetadataKey: decoded.text(flagMetadataKey), Mode: mode, PageSize: uint32(decoded.int32(flagPageSize)), Cursor: decoded.text(flagCursor), CheckpointId: checkpoint}, &servicepb.InspectIndexResponse{})
		if err != nil {
			return true, v3Failure("inspect index: %v", err)
		}
		return true, emitProto(host, opInspectIndex.id, response)
	case "ledger.v3.indexes.list":
		stream, err := streamV3(ctx, host, command, opListIndexes.id, &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: decoded.text(argLedger)}, func() *commonpb.Index { return &commonpb.Index{} })
		if err != nil {
			return true, v3Failure("list indexes: %v", err)
		}
		items, _, err := drainV3Stream(stream)
		if err != nil {
			return true, err
		}
		return true, emitProtoList(host, opListIndexes.id, items, nil)
	default:
		return false, nil
	}
}

func parseIndexID(decoded input) (*commonpb.IndexID, error) {
	switch decoded.text(flagIndexKind) {
	case "metadata":
		target, err := commonpb.ParseTargetType(decoded.text(flagTargetType))
		if err != nil || decoded.text(flagMetadataKey) == "" || decoded.text(flagBuiltin) != "" {
			return nil, invalidArgument("metadata index requires target-type and metadata-key only")
		}
		return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{Target: target, Key: decoded.text(flagMetadataKey)}}}, nil
	case "tx-builtin":
		values := map[string]commonpb.TransactionBuiltinIndex{
			"reference": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, "timestamp": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
			"id": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, "address": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
			"source-address": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS, "destination-address": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
			"inserted-at": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, "reverted-at": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
		}
		value, ok := values[decoded.text(flagBuiltin)]
		if !ok {
			return nil, invalidArgument("invalid transaction builtin index")
		}
		return &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: value}}, nil
	case "account-builtin":
		if decoded.text(flagBuiltin) != "asset" {
			return nil, invalidArgument("account builtin index requires builtin asset")
		}
		return &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET}}, nil
	case "log-builtin":
		if decoded.text(flagBuiltin) != "date" {
			return nil, invalidArgument("log builtin index requires builtin date")
		}
		return &commonpb.IndexID{Kind: &commonpb.IndexID_LogBuiltin{LogBuiltin: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE}}, nil
	default:
		return nil, invalidArgument("invalid index kind")
	}
}

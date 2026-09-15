package ledgerv3

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	ledgerconfig "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3/internal/ledgerconfig"
	"google.golang.org/protobuf/proto"
)

// executeV3Reads handles the non-configuration reads for ledgers, accounts,
// audit entries and logs. It deliberately declines every other family so the
// central dispatcher can hand the command to its owning adapter.
func executeV3Reads(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch request.CommandID {
	case "ledger.v3.ledgers.get":
		read, err := v3ReadOptions(decoded)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opGetLedger.id, &servicepb.GetLedgerRequest{Ledger: decoded.text(argLedger), Read: read}, &commonpb.LedgerInfo{})
		if err != nil {
			return true, v3Failure("get ledger: %v", err)
		}
		return true, emitLedgerInfo(host, opGetLedger.id, response)
	case "ledger.v3.ledgers.get-schema":
		response, err := unaryV3(ctx, host, command, opGetMetadataSchema.id, &servicepb.GetMetadataSchemaStatusRequest{Ledger: decoded.text(argLedger)}, &servicepb.GetMetadataSchemaStatusResponse{})
		if err != nil {
			return true, v3Failure("get metadata schema status: %v", err)
		}
		return true, emitProto(host, opGetMetadataSchema.id, response)
	case "ledger.v3.ledgers.stats":
		checkpoint, err := decoded.uint64(flagCheckpointID)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opGetLedgerStats.id, &servicepb.GetLedgerStatsRequest{Ledger: decoded.text(argLedger), CheckpointId: checkpoint}, &commonpb.LedgerStats{})
		if err != nil {
			return true, v3Failure("get ledger stats: %v", err)
		}
		return true, emitProto(host, opGetLedgerStats.id, response)
	case "ledger.v3.accounts.get":
		checkpoint, err := decoded.uint64(flagCheckpointID)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opGetAccount.id, &servicepb.GetAccountRequest{Ledger: decoded.text(argLedger), Address: decoded.text(argAddress), CheckpointId: checkpoint, CollapseColors: decoded.boolean(flagCollapseColors)}, &commonpb.Account{})
		if err != nil {
			return true, v3Failure("get account: %v", err)
		}
		return true, emitProto(host, opGetAccount.id, response)
	case "ledger.v3.accounts.aggregate-volumes":
		return true, executeV3AggregateVolumes(ctx, decoded, command, host)
	case "ledger.v3.audit.get":
		sequence, err := decoded.uint64(argSequence)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opGetAuditEntry.id, &servicepb.GetAuditEntryRequest{Sequence: sequence}, &auditpb.AuditEntry{})
		if err != nil {
			return true, v3Failure("get audit entry: %v", err)
		}
		return true, emitProto(host, opGetAuditEntry.id, response)
	case "ledger.v3.logs.get":
		sequence, err := decoded.uint64(argSequence)
		if err != nil {
			return true, err
		}
		checkpoint, err := decoded.uint64(flagCheckpointID)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opGetLog.id, &servicepb.GetLogRequest{Sequence: sequence, CheckpointId: checkpoint}, &commonpb.Log{})
		if err != nil {
			return true, v3Failure("get log: %v", err)
		}
		return true, emitProto(host, opGetLog.id, response)
	case "ledger.v3.ledgers.list":
		return true, executeV3ListLedgers(ctx, request, decoded, command, host)
	case "ledger.v3.accounts.list":
		return true, executeV3ListAccounts(ctx, request, decoded, command, host)
	case "ledger.v3.audit.list":
		return true, executeV3ListAudit(ctx, request, decoded, command, host)
	case "ledger.v3.logs.list":
		return true, executeV3ListLogs(ctx, request, decoded, command, host)
	case "ledger.v3.accounts.analyze":
		return true, executeV3AnalyzeAccounts(ctx, decoded, command, host)
	default:
		return false, nil
	}
}

func executeV3AggregateVolumes(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	checkpoint, err := decoded.uint64(flagCheckpointID)
	if err != nil {
		return err
	}
	filter, err := v3Filter(decoded, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if err != nil {
		return err
	}
	response, err := unaryV3(ctx, host, command, opAggregateVolumes.id, &servicepb.AggregateVolumesRequest{
		Ledger: decoded.text(argLedger), Filter: filter,
		UseMaxPrecision: decoded.boolean(flagMaxPrecision), GroupByPrefixes: decoded.list(flagGroupBy),
		CheckpointId: checkpoint, CollapseColors: decoded.boolean(flagCollapseColors),
	}, &commonpb.AggregateResult{})
	if err != nil {
		return v3Failure("aggregate account volumes: %v", err)
	}
	return emitProto(host, opAggregateVolumes.id, response)
}

func executeV3ListLedgers(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) error {
	base, err := v3ListOptionsWithoutFilter(decoded)
	if err != nil {
		return err
	}
	items, page, err := collectV3Pages(request.Continuation, base.Cursor, func(cursor string) ([]proto.Message, string, error) {
		options := proto.Clone(base).(*commonpb.ListOptions)
		options.Cursor = cursor
		stream, err := streamV3(ctx, host, command, opListLedgers.id, &servicepb.ListLedgersRequest{Options: options}, func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} })
		if err != nil {
			return nil, "", v3Failure("list ledgers: %v", err)
		}
		return drainV3Stream(stream)
	})
	if err != nil {
		return err
	}
	return emitLedgerInfoList(host, opListLedgers.id, items, page)
}

func v3ListOptionsWithoutFilter(decoded input) (*commonpb.ListOptions, error) {
	pageSize := decoded.int32(flagPageSize)
	if pageSize < 0 {
		return nil, invalidArgument("flag %q expects a non-negative integer", flagPageSize)
	}
	read, err := v3ReadOptions(decoded)
	if err != nil {
		return nil, err
	}
	return &commonpb.ListOptions{Read: read, PageSize: uint32(pageSize), Cursor: decoded.text(flagCursor), Reverse: decoded.boolean(flagReverse)}, nil
}

func executeV3ListAccounts(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) error {
	base, err := v3ListOptions(decoded, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if err != nil {
		return err
	}
	items, page, err := collectV3Pages(request.Continuation, base.Cursor, func(cursor string) ([]proto.Message, string, error) {
		options := proto.Clone(base).(*commonpb.ListOptions)
		options.Cursor = cursor
		stream, err := streamV3(ctx, host, command, opListAccounts.id, &servicepb.ListAccountsRequest{Ledger: decoded.text(argLedger), Options: options}, func() *commonpb.Account { return &commonpb.Account{} })
		if err != nil {
			return nil, "", v3Failure("list accounts: %v", err)
		}
		return drainV3Stream(stream)
	})
	if err != nil {
		return err
	}
	return emitProtoList(host, opListAccounts.id, items, page)
}

func executeV3ListAudit(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) error {
	base, err := v3ListOptions(decoded, commonpb.QueryTarget_QUERY_TARGET_AUDIT)
	if err != nil {
		return err
	}
	items, page, err := collectV3Pages(request.Continuation, base.Cursor, func(cursor string) ([]proto.Message, string, error) {
		options := proto.Clone(base).(*commonpb.ListOptions)
		options.Cursor = cursor
		stream, err := streamV3(ctx, host, command, opListAuditEntries.id, &servicepb.ListAuditEntriesRequest{Options: options}, func() *auditpb.AuditEntry { return &auditpb.AuditEntry{} })
		if err != nil {
			return nil, "", v3Failure("list audit entries: %v", err)
		}
		return drainV3Stream(stream)
	})
	if err != nil {
		return err
	}
	return emitProtoList(host, opListAuditEntries.id, items, page)
}

func executeV3ListLogs(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) error {
	base, err := v3ListOptions(decoded, commonpb.QueryTarget_QUERY_TARGET_LOGS)
	if err != nil {
		return err
	}
	items, page, err := collectV3Pages(request.Continuation, base.Cursor, func(cursor string) ([]proto.Message, string, error) {
		options := proto.Clone(base).(*commonpb.ListOptions)
		options.Cursor = cursor
		stream, err := streamV3(ctx, host, command, opListLogs.id, &servicepb.ListLogsRequest{Ledger: decoded.text(argLedger), Options: options}, func() *commonpb.Log { return &commonpb.Log{} })
		if err != nil {
			return nil, "", v3Failure("list logs: %v", err)
		}
		return drainV3Stream(stream)
	})
	if err != nil {
		return err
	}
	return emitProtoList(host, opListLogs.id, items, page)
}

func executeV3AnalyzeAccounts(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	threshold := decoded.int32(flagVariableThreshold)
	if threshold < 0 {
		return invalidArgument("flag %q expects a non-negative integer", flagVariableThreshold)
	}
	stream, err := streamV3(ctx, host, command, opAnalyzeAccounts.id, &servicepb.AnalyzeAccountsRequest{Ledger: decoded.text(argLedger), VariableThreshold: uint32(threshold)}, func() *servicepb.AnalyzeAccountsEvent { return &servicepb.AnalyzeAccountsEvent{} })
	if err != nil {
		return v3Failure("analyze accounts: %v", err)
	}
	var result *servicepb.AnalyzeAccountsResponse
	for {
		event := stream.newItem()
		recvErr := stream.stream.RecvInto(event)
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return v3Failure("receive account analysis: %v", recvErr)
		}
		if progress := event.GetProgress(); progress != nil {
			if emitErr := emitAnalyzeProgress(host, progress); emitErr != nil {
				return emitErr
			}
		}
		if candidate := event.GetResult(); candidate != nil {
			if result != nil {
				return v3Failure("account analysis returned more than one result")
			}
			result = candidate
		}
	}
	if result == nil {
		return v3Failure("account analysis stream ended without a result")
	}
	return emitAnalyzeAccountsResult(host, opAnalyzeAccounts.id, result)
}

func v3ReadOptions(decoded input) (*commonpb.ReadOptions, error) {
	checkpoint, err := decoded.uint64(flagCheckpointID)
	if err != nil {
		return nil, err
	}
	if checkpoint == 0 {
		return nil, nil
	}
	return &commonpb.ReadOptions{CheckpointId: checkpoint}, nil
}

func v3ListOptions(decoded input, target commonpb.QueryTarget) (*commonpb.ListOptions, error) {
	pageSize := decoded.int32(flagPageSize)
	if pageSize < 0 {
		return nil, invalidArgument("flag %q expects a non-negative integer", flagPageSize)
	}
	read, err := v3ReadOptions(decoded)
	if err != nil {
		return nil, err
	}
	filter, err := v3Filter(decoded, target)
	if err != nil {
		return nil, err
	}
	return &commonpb.ListOptions{Read: read, PageSize: uint32(pageSize), Cursor: decoded.text(flagCursor), Reverse: decoded.boolean(flagReverse), Filter: filter}, nil
}

func v3Filter(decoded input, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	raw := decoded.text(flagFilter)
	if raw == "" {
		return nil, nil
	}
	filter, err := ledgerconfig.DecodeFilter([]byte(raw), target)
	if err != nil {
		return nil, invalidArgument("flag %q contains an invalid filter expression", flagFilter)
	}
	return filter, nil
}

func drainV3Stream[T proto.Message](stream *v3Stream[T]) ([]proto.Message, string, error) {
	items := make([]proto.Message, 0)
	for {
		item := stream.newItem()
		err := stream.stream.RecvInto(item)
		if errors.Is(err, io.EOF) {
			cursor, ok := stream.stream.Continuation()
			if !ok {
				return items, "", nil
			}
			return items, cursor, nil
		}
		if err != nil {
			return nil, "", v3Failure("receive stream: %v", err)
		}
		items = append(items, item)
	}
}

func collectV3Pages(control sdk.ContinuationControl, initialCursor string, fetch func(string) ([]proto.Message, string, error)) ([]proto.Message, *sdk.PageInfo, error) {
	allPages := control.Mode == sdk.ContinuationAllPages
	maxPages := uint32(1)
	if allPages {
		maxPages = control.MaxPages
	}
	items := make([]proto.Message, 0)
	cursor := initialCursor
	seen := make(map[string]struct{})
	if cursor != "" {
		seen[cursor] = struct{}{}
	}
	for page := uint32(0); page < maxPages; page++ {
		pageItems, next, err := fetch(cursor)
		if err != nil {
			return nil, nil, err
		}
		if !allPages {
			return pageItems, &sdk.PageInfo{NextCursor: next, HasMore: next != ""}, nil
		}
		items = append(items, pageItems...)
		if err := enforceV3CollectionBudget(items, control); err != nil {
			return nil, nil, err
		}
		if next == "" {
			return items, nil, nil
		}
		if _, exists := seen[next]; exists {
			return nil, nil, v3BudgetExceeded("paginated response returned a repeated cursor")
		}
		seen[next] = struct{}{}
		cursor = next
	}
	return nil, nil, v3BudgetExceeded("collection exceeded the page limit")
}

func enforceV3CollectionBudget(items []proto.Message, control sdk.ContinuationControl) error {
	if uint32(len(items)) > control.MaxItems {
		return v3BudgetExceeded("collection exceeded the item limit")
	}
	size := uint64(2)
	for index, item := range items {
		encoded, err := marshalProductProto(item)
		if err != nil {
			return v3Failure("encode collection budget: %v", err)
		}
		size += uint64(len(encoded))
		if index != 0 {
			size++
		}
		if size > control.MaxBytes {
			return v3BudgetExceeded("collection exceeded the byte limit")
		}
	}
	return nil
}

func v3BudgetExceeded(message string) error {
	return sdk.Failure{Code: string(sdk.FailureBudgetExhausted), Message: "ledger v3: " + message}
}

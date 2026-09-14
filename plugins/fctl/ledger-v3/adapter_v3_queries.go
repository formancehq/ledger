package ledgerv3

import (
	"context"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func executeV3Queries(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch command.ID {
	case "ledger.v3.queries.create", "ledger.v3.queries.update":
		target := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
		if command.ID == "ledger.v3.queries.create" {
			var err error
			target, err = parseQueryTarget(decoded.text(flagQueryTarget))
			if err != nil {
				return true, err
			}
		}
		filter, err := parseQueryFilter(decoded.text(flagFilter), target)
		if err != nil || filter == nil {
			return true, invalidArgument("filter must contain at least one condition")
		}
		if command.ID == "ledger.v3.queries.create" {
			operation := opApplyCreatePreparedQuery.id
			_, err := applyV3(ctx, host, command, operation, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: decoded.text(argLedger), Query: &commonpb.PreparedQuery{Name: decoded.text(argName), Filter: filter, Target: target}}}})
			if err != nil {
				return true, v3Failure("create prepared query: %v", err)
			}
			return true, emitEmpty(host, operation)
		}
		operation := opApplyUpdatePreparedQuery.id
		_, err = applyV3(ctx, host, command, operation, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: decoded.text(argLedger), Name: decoded.text(argName), Filter: filter}}})
		if err != nil {
			return true, v3Failure("update prepared query: %v", err)
		}
		return true, emitEmpty(host, operation)
	case "ledger.v3.queries.delete":
		_, err := applyV3(ctx, host, command, opApplyDeletePreparedQuery.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: decoded.text(argLedger), Name: decoded.text(argName)}}})
		if err != nil {
			return true, v3Failure("delete prepared query: %v", err)
		}
		return true, emitEmpty(host, opApplyDeletePreparedQuery.id)
	case "ledger.v3.queries.list":
		response, err := unaryV3(ctx, host, command, opListPreparedQueries.id, &servicepb.ListPreparedQueriesRequest{Ledger: decoded.text(argLedger)}, &servicepb.ListPreparedQueriesResponse{})
		if err != nil {
			return true, v3Failure("list prepared queries: %v", err)
		}
		items := make([]proto.Message, 0, len(response.GetQueries()))
		for _, item := range response.GetQueries() {
			items = append(items, item)
		}
		return true, emitProtoList(host, opListPreparedQueries.id, items, nil)
	case "ledger.v3.queries.execute":
		parameters, err := parseQueryParameters(decoded.list(flagParameter))
		if err != nil {
			return true, err
		}
		mode := commonpb.QueryMode_QUERY_MODE_LIST
		if decoded.text(flagQueryMode) == "aggregate-volumes" {
			mode = commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES
		}
		items, page, err := collectV3Pages(request.Continuation, decoded.text(flagCursor), func(cursorValue string) ([]proto.Message, string, error) {
			response, err := unaryV3(ctx, host, command, opExecutePreparedQuery.id, &servicepb.ExecutePreparedQueryRequest{Ledger: decoded.text(argLedger), QueryName: decoded.text(argName), Parameters: parameters, PageSize: uint32(decoded.int32(flagPageSize)), Cursor: cursorValue, Mode: mode}, &servicepb.ExecutePreparedQueryResponse{})
			if err != nil {
				return nil, "", v3Failure("execute prepared query: %v", err)
			}
			if aggregate := response.GetAggregate(); aggregate != nil {
				return []proto.Message{aggregate}, "", nil
			}
			resultCursor := response.GetCursor()
			if resultCursor == nil {
				return nil, "", nil
			}
			pageItems := make([]proto.Message, 0, len(resultCursor.GetAccountData())+len(resultCursor.GetTransactionData())+len(resultCursor.GetLogData()))
			for _, item := range resultCursor.GetAccountData() {
				pageItems = append(pageItems, item)
			}
			for _, item := range resultCursor.GetTransactionData() {
				pageItems = append(pageItems, item)
			}
			for _, item := range resultCursor.GetLogData() {
				pageItems = append(pageItems, item)
			}
			if !resultCursor.GetHasMore() {
				return pageItems, "", nil
			}
			return pageItems, resultCursor.GetNext(), nil
		})
		if err != nil {
			return true, err
		}
		if mode == commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES {
			page = nil
		}
		return true, emitProtoList(host, opExecutePreparedQuery.id, items, page)
	default:
		return false, nil
	}
}

func parseQueryTarget(value string) (commonpb.QueryTarget, error) {
	switch value {
	case "accounts":
		return commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil
	case "transactions":
		return commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil
	case "logs":
		return commonpb.QueryTarget_QUERY_TARGET_LOGS, nil
	default:
		return 0, invalidArgument("invalid query target")
	}
}

func parseQueryParameters(values []string) (map[string]*commonpb.ParameterValue, error) {
	result := make(map[string]*commonpb.ParameterValue, len(values))
	for _, value := range values {
		key, raw, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return nil, invalidArgument("parameter expects name=value")
		}
		if _, duplicate := result[key]; duplicate {
			return nil, invalidArgument("parameter %q is repeated", key)
		}
		result[key] = &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: raw}}
	}
	return result, nil
}

package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/fctl/pkg/pluginsdk"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	cliindexes "github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	ledgerjson "github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

func (e *Executor) read(ctx context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, bool, error) {
	path := strings.Join(req.CommandPath, "/")
	switch path {
	case "ledger/list", "ledger/show", "ledger/metadata/show", "ledger/stats", "ledger/balances", "ledger/info",
		"ledger/accounts/list", "ledger/accounts/show", "ledger/accounts/balances", "ledger/accounts/metadata/show",
		"ledger/transactions/list", "ledger/transactions/show", "ledger/transactions/metadata/show", "ledger/logs/list",
		"ledger/indexes/list", "ledger/indexes/show", "ledger/indexes/status", "ledger/indexes/inspect":
	default:
		return pluginsdk.ExecuteResponse{}, false, nil
	}

	e.Result, e.NextCursor, e.PreviousCursor, e.Trailer = nil, "", "", nil
	if err := ctx.Err(); err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	if e.Client == nil {
		return pluginsdk.ExecuteResponse{}, true, errors.New("ledger reads require a gRPC client")
	}
	checkpoint := e.CheckpointID
	if raw := req.Context["checkpoint"]; raw != "" {
		var err error
		checkpoint, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return pluginsdk.ExecuteResponse{}, true, fmt.Errorf("invalid checkpoint: %w", err)
		}
	}
	consistency := req.Flags["consistency"]
	if consistency == "" {
		consistency = e.Consistency
	}
	consistency = strings.ToLower(strings.TrimSpace(consistency))
	if consistency != "" {
		if consistency != "stale" && consistency != "linearizable" {
			return pluginsdk.ExecuteResponse{}, true, errors.New("consistency must be linearizable or stale")
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Set("x-consistency", consistency)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	callOptions := []grpc.CallOption{grpc.Trailer(&e.Trailer)}
	ledger := req.Flags["ledger"]
	if path == "ledger/show" && len(req.Args) > 0 {
		ledger = req.Args[0]
	}
	var data any
	var err error
	switch path {
	case "ledger/list":
		options, parseErr := readListOptions(req, checkpoint, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, parseErr
		}
		stream, callErr := e.Client.ListLedgers(ctx, &servicepb.ListLedgersRequest{Options: options})

		return finishReadPage(e, stream, callErr)
	case "ledger/show", "ledger/metadata/show":
		result, callErr := e.Client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger, Read: readOptions(checkpoint)}, callOptions...)
		retainReadResult(e, result)
		data, err = result, callErr
	case "ledger/stats":
		result, callErr := e.Client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: ledger, CheckpointId: checkpoint}, callOptions...)
		retainReadResult(e, result)
		err = callErr
		if result != nil {
			data = map[string]uint64{
				"transactionCount": result.GetTransactionCount(), "volumeCount": result.GetVolumeCount(),
				"referenceCount": result.GetReferenceCount(), "postingCount": result.GetPostingCount(),
				"logCount": result.GetLogCount(), "ephemeralEvictedCount": result.GetEphemeralEvictedCount(),
				"transientUsedCount": result.GetTransientUsedCount(), "revertCount": result.GetRevertCount(),
				"numscriptExecutionCount": result.GetNumscriptExecutionCount(),
			}
		}
	case "ledger/info":
		result, callErr := e.Client.Discovery(ctx, &servicepb.DiscoveryRequest{}, callOptions...)
		err = callErr
		if result != nil {
			e.Result = result.GetServerInfo()
			info := result.GetServerInfo()

			return finishRead(map[string]string{"version": info.GetVersion(), "commit": info.GetCommit(),
				"buildDate": info.GetBuildDate(), "goVersion": info.GetGoVersion(), "protocolVersion": info.GetProtocolVersion()}, err)
		}
	case "ledger/balances":
		filter, parseErr := readFilter(req, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, parseErr
		}
		var prefixes []string
		if raw := req.Flags["group-by-prefixes"]; raw != "" {
			prefixes = strings.Split(raw, ",")
		}
		result, callErr := e.Client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
			Ledger: ledger, Filter: filter, CheckpointId: checkpoint, GroupByPrefixes: prefixes,
			UseMaxPrecision: req.Flags["use-max-precision"] == "true", CollapseColors: req.Flags["collapse-colors"] == "true",
		}, callOptions...)
		retainReadResult(e, result)
		err = callErr
		if result != nil {
			data = readAggregateJSON(result)
		}
	case "ledger/accounts/list":
		options, parseErr := readListOptions(req, checkpoint, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, parseErr
		}
		stream, callErr := e.Client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger, Options: options})

		return finishReadPage(e, stream, callErr)
	case "ledger/accounts/show", "ledger/accounts/balances", "ledger/accounts/metadata/show":
		result, callErr := e.Client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledger, Address: req.Args[0],
			CheckpointId: checkpoint, CollapseColors: req.Flags["collapse-colors"] == "true"}, callOptions...)
		retainReadResult(e, result)
		data, err = result, callErr
	case "ledger/transactions/list":
		options, parseErr := readListOptions(req, checkpoint, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, parseErr
		}
		stream, callErr := e.Client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{Ledger: ledger, Options: options})

		return finishReadPage(e, stream, callErr)
	case "ledger/transactions/show", "ledger/transactions/metadata/show":
		id, parseErr := strconv.ParseUint(req.Args[0], 10, 64)
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, fmt.Errorf("invalid transaction ID: %w", parseErr)
		}
		result, callErr := e.Client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: id, CheckpointId: checkpoint}, callOptions...)
		retainReadResult(e, result)
		err = callErr
		if result != nil {
			data = map[string]any{"transaction": result.GetTransaction()}
		}
	case "ledger/logs/list":
		options, parseErr := readListOptions(req, checkpoint, commonpb.QueryTarget_QUERY_TARGET_LOGS)
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, parseErr
		}
		stream, callErr := e.Client.ListLogs(ctx, &servicepb.ListLogsRequest{Ledger: ledger, Options: options})

		return finishReadPage(e, stream, callErr)
	case "ledger/indexes/list":
		return e.readIndexes(ctx, req, ledger)
	case "ledger/indexes/show", "ledger/indexes/status", "ledger/indexes/inspect":
		id, parseErr := indexes.ParseCanonical(req.Args[0])
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, parseErr
		}
		switch path {
		case "ledger/indexes/show":
			result, callErr := e.Client.GetIndex(ctx, &servicepb.GetIndexRequest{Ledger: ledger, Id: id}, callOptions...)
			retainReadResult(e, result)
			err = callErr
			if result != nil {
				data, parseErr = protojson.Marshal(result)
			}
		case "ledger/indexes/status":
			result, callErr := e.Client.GetIndexEntryStatus(ctx, &servicepb.GetIndexEntryStatusRequest{Ledger: ledger, Id: id}, callOptions...)
			retainReadResult(e, result)
			err = callErr
			if result != nil {
				data, parseErr = protojson.Marshal(result)
			}
		case "ledger/indexes/inspect":
			return e.readInspect(ctx, req, ledger, checkpoint, id)
		}
		if parseErr != nil {
			return pluginsdk.ExecuteResponse{}, true, errors.Join(err, parseErr)
		}
		if raw, ok := data.([]byte); ok {
			data = json.RawMessage(raw)
		}
	}
	if err != nil && e.Result == nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}

	return finishReadEnvelope(data, err)
}

func retainReadResult[T any](e *Executor, result *T) {
	if result != nil {
		e.Result = result
	}
}

func readOptions(checkpoint uint64) *commonpb.ReadOptions {
	if checkpoint == 0 {
		return nil
	}

	return &commonpb.ReadOptions{CheckpointId: checkpoint}
}

func readListOptions(req pluginsdk.ExecuteRequest, checkpoint uint64, target commonpb.QueryTarget) (*commonpb.ListOptions, error) {
	size := uint64(0)
	if raw := req.Flags["page-size"]; raw != "" {
		var err error
		size, err = strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid page size: %w", err)
		}
	}
	cursor := req.Context["cursor"]
	if cursor == "" {
		cursor = req.Flags["cursor"]
	}
	// The product validates mutually exclusive user flags before dispatch. A
	// normalized request can retain --after alongside its canonical cursor;
	// the host cursor also takes precedence when it advances to another page.
	if after := req.Flags["after"]; cursor == "" && after != "" {
		cursor = (pagecursor.Cursor{Key: after}).Encode()
	}
	if _, err := pagecursor.Decode(cursor); err != nil {
		return nil, err
	}
	filter, err := readFilter(req, target)
	if err != nil {
		return nil, err
	}

	return cmdutil.BuildListOptions(cmdutil.PaginationFlags{PageSize: uint32(size), Cursor: cursor, Reverse: req.Flags["reverse"] == "true"},
		cmdutil.ConsistencyFlags{CheckpointID: checkpoint}, filter), nil
}

func readFilter(req pluginsdk.ExecuteRequest, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	filter, err := cmdutil.BuildQueryFilter(req.Flags["filter"], req.Context["prefix"], target)
	if err != nil {
		return nil, err
	}
	if req.Flags["start-date"] == "" && req.Flags["end-date"] == "" {
		return filter, nil
	}
	condition := &commonpb.UintCondition{}
	for _, bound := range []struct {
		flag string
		dest **uint64
	}{{"start-date", &condition.Min}, {"end-date", &condition.Max}} {
		raw := req.Flags[bound.flag]
		if raw == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339, raw); err != nil {
			return nil, fmt.Errorf("invalid %s, expected RFC3339: %w", bound.flag, err)
		}
		micros, err := commonpb.CoerceDatetimeMicros(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", bound.flag, err)
		}
		*bound.dest = &micros
	}
	condition.MaxExclusive = condition.Max != nil
	var dateFilter *commonpb.QueryFilter
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		dateFilter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, Cond: condition}}}
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		dateFilter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
			Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, Cond: condition}}}
	default:
		return nil, errors.New("date ranges are supported only for transactions and logs")
	}
	if filter == nil {
		return dateFilter, nil
	}

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{dateFilter, filter}}}}, nil
}

// collectRead retains already received records on a terminal streaming error.
// The original RPC error remains available to the host; no retry occurs.
func collectRead[T any](stream grpc.ServerStreamingClient[T]) ([]*T, error) {
	items := make([]*T, 0)
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return items, nil
		}
		if err != nil {
			return items, err
		}
		items = append(items, item)
	}
}

func finishReadPage[T any](e *Executor, stream grpc.ServerStreamingClient[T], err error) (pluginsdk.ExecuteResponse, bool, error) {
	if err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	rows, err := collectRead(stream)
	e.Result = rows
	e.Trailer = stream.Trailer()
	e.NextCursor = cmdutil.NextCursorFromTrailer(e.Trailer)
	e.PreviousCursor = cmdutil.PreviousCursorFromTrailer(e.Trailer)
	page := map[string]any{"data": rows, "hasMore": e.NextCursor != ""}
	if e.NextCursor != "" {
		page["next"] = e.NextCursor
	}
	if e.PreviousCursor != "" {
		page["previous"] = e.PreviousCursor
	}

	return finishRead(page, err)
}

func marshalRead(data any) (pluginsdk.ExecuteResponse, error) {
	raw, err := ledgerjson.MarshalWithOptions(data, commonpb.MonetaryAmountsAsNumbers())
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	// Indent raw JSON without decoding its arbitrary-precision number tokens.
	raw, err = json.MarshalIndent(json.RawMessage(raw), "", "  ")

	return pluginsdk.ExecuteResponse{Data: raw}, err
}

func finishRead(data any, rpcErr error) (pluginsdk.ExecuteResponse, bool, error) {
	response, err := marshalRead(data)

	return response, true, errors.Join(rpcErr, err)
}

func finishReadEnvelope(data any, rpcErr error) (pluginsdk.ExecuteResponse, bool, error) {
	if data == nil && rpcErr != nil {
		return pluginsdk.ExecuteResponse{}, true, rpcErr
	}

	return finishRead(map[string]any{"data": data}, rpcErr)
}

func (e *Executor) readIndexes(ctx context.Context, req pluginsdk.ExecuteRequest, ledger string) (pluginsdk.ExecuteResponse, bool, error) {
	stream, err := e.Client.ListIndexes(ctx, &servicepb.ListIndexesRequest{Ledger: ledger, Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER})
	if err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	rows, readErr := collectRead(stream)
	if prefix := req.Context["creation-key-prefix"]; prefix != "" {
		rows, err = cliindexes.FilterIndexesByCreationKey(ctx, e.Client, ledger, prefix, rows)
		if err != nil {
			return pluginsdk.ExecuteResponse{}, true, errors.Join(readErr, err)
		}
	}
	e.Result = rows
	e.Trailer = stream.Trailer()
	values := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		raw, err := protojson.Marshal(row)
		if err != nil {
			return pluginsdk.ExecuteResponse{}, true, errors.Join(readErr, err)
		}
		values = append(values, raw)
	}

	return finishReadEnvelope(values, readErr)
}

func (e *Executor) readInspect(ctx context.Context, req pluginsdk.ExecuteRequest, ledger string, checkpoint uint64, id *commonpb.IndexID) (pluginsdk.ExecuteResponse, bool, error) {
	meta := id.GetMetadata()
	if meta == nil {
		return pluginsdk.ExecuteResponse{}, true, errors.New("index inspection requires a metadata index")
	}
	mode := servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY
	switch req.Flags["mode"] {
	case "distinctValues", "distinct-values":
		mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES
	case "facets":
		mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS
	case "", "summary":
	default:
		return pluginsdk.ExecuteResponse{}, true, errors.New("invalid index inspection mode")
	}
	size, err := strconv.ParseUint(req.Flags["page-size"], 10, 32)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, true, fmt.Errorf("invalid inspection page size: %w", err)
	}
	cursor := req.Context["cursor"]
	if cursor == "" {
		cursor = req.Flags["cursor"]
	}
	result, err := e.Client.InspectIndex(ctx, &servicepb.InspectIndexRequest{Ledger: ledger,
		TargetType: meta.GetTarget(), MetadataKey: meta.GetKey(), Mode: mode, PageSize: uint32(size), Cursor: cursor, CheckpointId: checkpoint}, grpc.Trailer(&e.Trailer))
	retainReadResult(e, result)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	// Match HTTP's render hint for datetime indexes. A failed schema lookup
	// falls back to the raw value; it is not a retry of the inspection RPC.
	declared := commonpb.MetadataType_METADATA_TYPE_STRING
	info, schemaErr := e.Client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger, Read: readOptions(checkpoint)})
	if schemaErr == nil {
		_, field := commonpb.SchemaFieldForTarget(info.GetMetadataSchema(), meta.GetTarget(), meta.GetKey())
		declared = field.GetType()
	}
	if ctx.Err() != nil {
		return pluginsdk.ExecuteResponse{}, true, ctx.Err()
	}
	e.InspectMetadataType = declared
	var data any
	switch value := result.GetResult().(type) {
	case *servicepb.InspectIndexResponse_DistinctValues:
		page := value.DistinctValues
		e.NextCursor, e.PreviousCursor = page.GetNextCursor(), page.GetPreviousCursor()
		values := make([]any, len(page.GetValues()))
		for i, v := range page.GetValues() {
			values[i] = readMetadataValue(v, declared)
		}
		data = struct {
			Values   []any  `json:"values"`
			HasMore  bool   `json:"hasMore"`
			Next     string `json:"nextCursor,omitempty"`
			Previous string `json:"previousCursor,omitempty"`
		}{values, page.GetHasMore(), e.NextCursor, e.PreviousCursor}
	case *servicepb.InspectIndexResponse_Facets:
		page := value.Facets
		e.NextCursor, e.PreviousCursor = page.GetNextCursor(), page.GetPreviousCursor()
		facets := make([]readFacetJSON, len(page.GetFacets()))
		for i, facet := range page.GetFacets() {
			facets[i] = readFacetJSON{Value: readMetadataValue(facet.GetValue(), declared), Count: facet.GetCount()}
		}
		data = struct {
			Facets   []readFacetJSON `json:"facets"`
			HasMore  bool            `json:"hasMore"`
			Next     string          `json:"nextCursor,omitempty"`
			Previous string          `json:"previousCursor,omitempty"`
		}{facets, page.GetHasMore(), e.NextCursor, e.PreviousCursor}
	case *servicepb.InspectIndexResponse_Summary:
		summary := value.Summary
		data = struct {
			Cardinality      uint64 `json:"cardinality"`
			Min              any    `json:"min"`
			Max              any    `json:"max"`
			EntitiesWithKey  uint64 `json:"entitiesWithKey"`
			EntitiesWithNull uint64 `json:"entitiesWithNull"`
		}{summary.GetCardinality(), readMetadataValue(summary.GetMin(), declared), readMetadataValue(summary.GetMax(), declared), summary.GetEntitiesWithKey(), summary.GetEntitiesWithNull()}
	default:
		return pluginsdk.ExecuteResponse{}, true, errors.New("index inspection returned no result")
	}

	return finishReadEnvelope(data, nil)
}

type readFacetJSON struct {
	Value any    `json:"value"`
	Count uint64 `json:"count"`
}

func readMetadataValue(value *commonpb.MetadataValue, declared commonpb.MetadataType) any {
	if value == nil {
		return nil
	}
	switch kind := value.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return kind.StringValue
	case *commonpb.MetadataValue_IntValue:
		if commonpb.IsDatetimeType(declared) {
			return time.UnixMicro(kind.IntValue).UTC().Format(time.RFC3339Nano)
		}

		return kind.IntValue
	case *commonpb.MetadataValue_UintValue:
		return kind.UintValue
	case *commonpb.MetadataValue_DatetimeValue:
		return time.UnixMicro(kind.DatetimeValue).UTC().Format(time.RFC3339Nano)
	case *commonpb.MetadataValue_BoolValue:
		return kind.BoolValue
	default:
		return nil
	}
}

func readAggregateJSON(result *commonpb.AggregateResult) any {
	data := map[string]any{"volumes": readVolumeJSON(result.GetVolumes())}
	if len(result.GetGroups()) > 0 {
		groups := make([]any, len(result.GetGroups()))
		for i, group := range result.GetGroups() {
			groups[i] = map[string]any{"prefix": group.GetPrefix(), "volumes": readVolumeJSON(group.GetVolumes())}
		}
		data["groups"] = groups
	}

	return data
}

func readVolumeJSON(volumes []*commonpb.AggregatedVolume) []any {
	data := make([]any, len(volumes))
	for i, volume := range volumes {
		input, output := volume.GetInput().ToBigInt(), volume.GetOutput().ToBigInt()
		inputAmount, _ := commonpb.NewBigUint(input) // Unsigned Uint256 inputs cannot fail conversion.
		outputAmount, _ := commonpb.NewBigUint(output)
		data[i] = map[string]any{"asset": volume.GetAsset(), "color": volume.GetColor(), "input": inputAmount,
			"output": outputAmount, "balance": commonpb.NewSignedBigInt(new(big.Int).Sub(input, output))}
	}

	return data
}

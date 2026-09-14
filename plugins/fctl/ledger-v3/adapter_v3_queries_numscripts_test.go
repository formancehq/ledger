package ledgerv3

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

type queryNumscriptInputHost struct {
	*sdk.MemoryHost
	chunks []sdk.InputArtifactChunk
	err    error
	reads  int
}

func (h *queryNumscriptInputHost) ReadInput(_ context.Context, handle string) (sdk.InputArtifactChunk, error) {
	if handle != "script-handle" {
		return sdk.InputArtifactChunk{}, errors.New("unexpected artifact handle")
	}
	if h.err != nil {
		return sdk.InputArtifactChunk{}, h.err
	}
	if h.reads >= len(h.chunks) {
		return sdk.InputArtifactChunk{}, errors.New("unexpected artifact read")
	}
	chunk := h.chunks[h.reads]
	h.reads++
	return chunk, nil
}

type queryNumscriptErrorResponses struct{ err error }

func (r queryNumscriptErrorResponses) Recv() (sdk.Response, error) { return sdk.Response{}, r.err }

func TestExecuteV3QueriesMapsCRUDRequestsAndResults(t *testing.T) {
	tests := []struct {
		name       string
		commandID  string
		arguments  []string
		flags      []sdk.FlagOccurrence
		operation  string
		response   proto.Message
		wantShape  sdk.ResultShape
		wantData   string
		newRequest func() proto.Message
		want       proto.Message
		mutation   *servicepb.Request
	}{
		{
			name: "create", commandID: "ledger.v3.queries.create", arguments: []string{"main", "active-users"},
			flags:     []sdk.FlagOccurrence{{Name: flagQueryTarget, Value: "accounts"}, {Name: flagFilter, Value: `address ^= "users:"`}, {Name: flagIdempotencyKey, Value: "query-write-1"}},
			operation: opApplyCreatePreparedQuery.id, response: &servicepb.ApplyResponse{}, wantShape: sdk.ResultEmpty, wantData: `{}`,
			mutation: &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: "main", Query: &commonpb.PreparedQuery{Name: "active-users", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: addressPrefixFilter("users:")}}}},
		},
		{
			name: "update", commandID: "ledger.v3.queries.update", arguments: []string{"main", "active-users"},
			flags:     []sdk.FlagOccurrence{{Name: flagFilter, Value: `address ^= "customers:"`}, {Name: flagIdempotencyKey, Value: "query-write-1"}},
			operation: opApplyUpdatePreparedQuery.id, response: &servicepb.ApplyResponse{}, wantShape: sdk.ResultEmpty, wantData: `{}`,
			mutation: &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: "main", Name: "active-users", Filter: addressPrefixFilter("customers:")}}},
		},
		{
			name: "delete", commandID: "ledger.v3.queries.delete", arguments: []string{"main", "active-users"},
			flags:     []sdk.FlagOccurrence{{Name: flagIdempotencyKey, Value: "query-write-1"}},
			operation: opApplyDeletePreparedQuery.id, response: &servicepb.ApplyResponse{}, wantShape: sdk.ResultEmpty, wantData: `{}`,
			mutation: &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "main", Name: "active-users"}}},
		},
		{
			name: "list", commandID: "ledger.v3.queries.list", arguments: []string{"main"},
			operation: opListPreparedQueries.id,
			response:  &servicepb.ListPreparedQueriesResponse{Queries: []*commonpb.PreparedQuery{{Name: "active-users", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS}}},
			wantShape: sdk.ResultCollection, wantData: `[{"name":"active-users","target":"ACCOUNTS"}]`,
			newRequest: func() proto.Message { return &servicepb.ListPreparedQueriesRequest{} },
			want:       &servicepb.ListPreparedQueriesRequest{Ledger: "main"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, decoded, request := decodedQueryNumscript(t, test.commandID, test.arguments, test.flags, sdk.SinglePageContinuationControl())
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				if got.Operation != test.operation {
					t.Fatalf("operation = %q, want %q", got.Operation, test.operation)
				}
				if test.mutation != nil {
					actual := &servicepb.ApplyRequest{}
					if got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, actual) != nil {
						t.Fatalf("apply request = %v", actual)
					}
					batch := actual.GetUnsigned()
					if batch.GetIdempotencyKey() != "query-write-1" || len(batch.GetRequests()) != 1 || !proto.Equal(batch.GetRequests()[0], test.mutation) {
						t.Fatalf("apply batch = %v, want mutation %v", batch, test.mutation)
					}
				} else {
					actual := test.newRequest()
					if got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, actual) != nil || !proto.Equal(actual, test.want) {
						t.Fatalf("request = %v, want %v", actual, test.want)
					}
				}
				return sdk.NewResponseStream(protoResponse(t, test.response)), nil
			})
			handled, err := executeV3Queries(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Queries() = %v, %v", handled, err)
			}
			result := host.Events()[0].Result
			if result.OperationID != test.operation || result.Shape != test.wantShape || !equalJSON(result.Data, test.wantData) {
				t.Fatalf("result = %#v, data = %s", result, result.Data)
			}
		})
	}
}

func TestExecuteV3QueriesCreateDefaultsToAccountsAndAcceptsNoFilter(t *testing.T) {
	t.Parallel()

	command, decoded, request := decodedQueryNumscript(t, "ledger.v3.queries.create", []string{"main", "all-accounts"}, []sdk.FlagOccurrence{{Name: flagIdempotencyKey, Value: "query-write-1"}}, sdk.SinglePageContinuationControl())
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		var actual servicepb.ApplyRequest
		if got.Operation != opApplyCreatePreparedQuery.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &actual) != nil {
			t.Fatalf("host request = %#v", got)
		}
		query := actual.GetUnsigned().GetRequests()[0].GetCreatePreparedQuery().GetQuery()
		if query.GetTarget() != commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS || query.GetFilter() != nil {
			t.Fatalf("prepared query = %#v, want accounts target and nil filter", query)
		}
		return sdk.NewResponseStream(protoResponse(t, &servicepb.ApplyResponse{})), nil
	})
	if handled, err := executeV3Queries(context.Background(), request, decoded, command, host); err != nil || !handled {
		t.Fatalf("executeV3Queries() = (%v, %v)", handled, err)
	}
}

func TestExecuteV3QueriesExecuteMapsListParametersAndCursor(t *testing.T) {
	flags := []sdk.FlagOccurrence{
		{Name: flagParameter, Value: "threshold=1000"}, {Name: flagParameter, Value: "label="},
		{Name: flagPageSize, Value: "3"}, {Name: flagCursor, Value: "opaque-start"},
		{Name: flagQueryMode, Value: "list"},
	}
	command, decoded, request := decodedQueryNumscript(t, "ledger.v3.queries.execute", []string{"main", "activity"}, flags, sdk.SinglePageContinuationControl())
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		actual := &servicepb.ExecutePreparedQueryRequest{}
		if got.Operation != opExecutePreparedQuery.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, actual) != nil {
			t.Fatalf("host request = %#v", got)
		}
		want := &servicepb.ExecutePreparedQueryRequest{
			Ledger: "main", QueryName: "activity", PageSize: 3, Cursor: "opaque-start", Mode: commonpb.QueryMode_QUERY_MODE_LIST,
			Parameters: map[string]*commonpb.ParameterValue{
				"threshold": {Value: &commonpb.ParameterValue_StringValue{StringValue: "1000"}},
				"label":     {Value: &commonpb.ParameterValue_StringValue{StringValue: ""}},
			},
		}
		if !proto.Equal(actual, want) {
			t.Fatalf("request = %v, want %v", actual, want)
		}
		response := &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: &commonpb.PreparedQueryCursor{
			HasMore: true, Next: "opaque-next", AccountData: []*commonpb.Account{{Address: "users:42"}}, TransactionData: []*commonpb.Transaction{{Id: 7}}, LogData: []*commonpb.Log{{Sequence: 9}},
		}}}
		return sdk.NewResponseStream(protoResponse(t, response)), nil
	})

	handled, err := executeV3Queries(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Queries() = %v, %v", handled, err)
	}
	result := host.Events()[0].Result
	if result.Shape != sdk.ResultCollection || result.Page == nil || result.Page.NextCursor != "opaque-next" || !result.Page.HasMore || !equalJSON(result.Data, `[{"address":"users:42","volumes":[]},{"postings":[],"metadata":{},"id":7,"reverted":false},{"sequence":9,"responseSignature":{}}]`) {
		t.Fatalf("result = %#v, data = %s", result, result.Data)
	}
}

func TestExecuteV3QueriesExecuteHonoursAllPagesContinuation(t *testing.T) {
	command, decoded, request := decodedQueryNumscript(t, "ledger.v3.queries.execute", []string{"main", "activity"}, nil, sdk.ContinuationControl{
		Mode: sdk.ContinuationAllPages, MaxPages: 2, MaxItems: 2, MaxBytes: 1024,
	})
	if !command.Pagination.Supported {
		t.Fatal("queries execute does not advertise cursor continuation")
	}
	calls := 0
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		calls++
		wire := &servicepb.ExecutePreparedQueryRequest{}
		if got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, wire) != nil {
			t.Fatalf("request = %#v", got)
		}
		if calls == 1 {
			if wire.GetCursor() != "" {
				t.Fatalf("first cursor = %q", wire.GetCursor())
			}
			return sdk.NewResponseStream(protoResponse(t, &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: &commonpb.PreparedQueryCursor{HasMore: true, Next: "next", AccountData: []*commonpb.Account{{Address: "users:1"}}}}})), nil
		}
		if wire.GetCursor() != "next" {
			t.Fatalf("second cursor = %q", wire.GetCursor())
		}
		return sdk.NewResponseStream(protoResponse(t, &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: &commonpb.PreparedQueryCursor{AccountData: []*commonpb.Account{{Address: "users:2"}}}}})), nil
	})
	if handled, err := executeV3Queries(context.Background(), request, decoded, command, host); err != nil || !handled {
		t.Fatalf("executeV3Queries() = (%v, %v)", handled, err)
	}
	result := host.Events()[0].Result
	if calls != 2 || result.Page != nil || !equalJSON(result.Data, `[{"address":"users:1","volumes":[]},{"address":"users:2","volumes":[]}]`) {
		t.Fatalf("calls = %d, result = %#v, data = %s", calls, result, result.Data)
	}
}

func TestExecuteV3QueriesExecuteEmitsAggregateAndEmptyCursor(t *testing.T) {
	tests := []struct {
		name     string
		response *servicepb.ExecutePreparedQueryResponse
		wantData string
	}{
		{
			name:     "aggregate",
			response: &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{Aggregate: &commonpb.AggregateResult{Groups: []*commonpb.GroupedAggregateResult{{Prefix: "users:"}}}}},
			wantData: `[{"groups":[{"prefix":"users:"}]}]`,
		},
		{name: "empty cursor", response: &servicepb.ExecutePreparedQueryResponse{}, wantData: `[]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags := []sdk.FlagOccurrence{{Name: flagQueryMode, Value: "aggregate-volumes"}}
			command, decoded, request := decodedQueryNumscript(t, "ledger.v3.queries.execute", []string{"main", "balances"}, flags, sdk.SinglePageContinuationControl())
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				actual := &servicepb.ExecutePreparedQueryRequest{}
				if proto.Unmarshal(got.GRPC.Message, actual) != nil || actual.GetMode() != commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES {
					t.Fatalf("request = %v", actual)
				}
				return sdk.NewResponseStream(protoResponse(t, test.response)), nil
			})
			handled, err := executeV3Queries(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Queries() = %v, %v", handled, err)
			}
			result := host.Events()[0].Result
			if result.Page != nil || !equalJSON(result.Data, test.wantData) {
				t.Fatalf("result = %#v, data = %s", result, result.Data)
			}
		})
	}
}

func TestParseQueryTargetAndParameters(t *testing.T) {
	targets := map[string]commonpb.QueryTarget{
		"accounts":     commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		"transactions": commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		"logs":         commonpb.QueryTarget_QUERY_TARGET_LOGS,
	}
	for input, want := range targets {
		got, err := parseQueryTarget(input)
		if err != nil || got != want {
			t.Fatalf("parseQueryTarget(%q) = %v, %v", input, got, err)
		}
	}
	if _, err := parseQueryTarget("payments"); err == nil {
		t.Fatal("parseQueryTarget(invalid) error = nil")
	}

	got, err := parseQueryParameters([]string{"threshold=1000", "expression=a=b"})
	if err != nil || got["threshold"].GetStringValue() != "1000" || got["expression"].GetStringValue() != "a=b" {
		t.Fatalf("parseQueryParameters() = %#v, %v", got, err)
	}
	for _, values := range [][]string{{"missing-separator"}, {"=empty-key"}, {"key=one", "key=two"}} {
		if _, err := parseQueryParameters(values); err == nil {
			t.Fatalf("parseQueryParameters(%q) error = nil", values)
		}
	}
}

func TestExecuteV3QueriesRejectsInvalidInputsAndPropagatesHostFailure(t *testing.T) {
	tests := []struct {
		name      string
		commandID string
		arguments []string
		flags     []sdk.FlagOccurrence
		host      sdk.Host
	}{
		{name: "empty update filter", commandID: "ledger.v3.queries.update", arguments: []string{"main", "q"}, flags: []sdk.FlagOccurrence{{Name: flagFilter, Value: ""}}, host: sdk.NewMemoryHost(nil)},
		{name: "invalid filter", commandID: "ledger.v3.queries.update", arguments: []string{"main", "q"}, flags: []sdk.FlagOccurrence{{Name: flagFilter, Value: "("}}, host: sdk.NewMemoryHost(nil)},
		{name: "invalid parameter", commandID: "ledger.v3.queries.execute", arguments: []string{"main", "q"}, flags: []sdk.FlagOccurrence{{Name: flagParameter, Value: "invalid"}}, host: sdk.NewMemoryHost(nil)},
		{name: "host failure", commandID: "ledger.v3.queries.delete", arguments: []string{"main", "q"}, host: sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) { return nil, errors.New("host unavailable") })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, decoded, request := decodedQueryNumscript(t, test.commandID, test.arguments, test.flags, sdk.SinglePageContinuationControl())
			handled, err := executeV3Queries(context.Background(), request, decoded, command, test.host)
			if !handled || err == nil {
				t.Fatalf("executeV3Queries() = %v, %v", handled, err)
			}
		})
	}
	if handled, err := executeV3Queries(context.Background(), sdk.ExecuteRequest{}, input{}, sdk.Command{ID: "ledger.v3.numscripts.get"}, sdk.NewMemoryHost(nil)); handled || err != nil {
		t.Fatalf("unhandled command = %v, %v", handled, err)
	}
}

func TestExecuteV3NumscriptsMapsGetAndVersions(t *testing.T) {
	tests := []struct {
		name       string
		commandID  string
		flags      []sdk.FlagOccurrence
		operation  string
		newRequest func() proto.Message
		want       proto.Message
		response   proto.Message
		wantData   string
	}{
		{
			name: "get", commandID: "ledger.v3.numscripts.get", operation: opGetNumscript.id,
			flags:      []sdk.FlagOccurrence{{Name: flagVersion, Value: "1.2"}, {Name: flagCheckpointID, Value: "41"}},
			newRequest: func() proto.Message { return &servicepb.GetNumscriptRequest{} },
			want:       &servicepb.GetNumscriptRequest{Ledger: "main", Name: "payout", Version: "1.2", Read: &commonpb.ReadOptions{CheckpointId: 41}},
			response:   &commonpb.NumscriptInfo{Name: "payout", Version: "1.2.3", Content: "send [USD 1]"},
			wantData:   `{"name":"payout","content":"send [USD 1]","version":"1.2.3"}`,
		},
		{
			name: "versions", commandID: "ledger.v3.numscripts.versions", operation: opListNumscriptVersions.id,
			flags:      []sdk.FlagOccurrence{{Name: flagCheckpointID, Value: "41"}},
			newRequest: func() proto.Message { return &servicepb.ListNumscriptVersionsRequest{} },
			want:       &servicepb.ListNumscriptVersionsRequest{Ledger: "main", Name: "payout", Read: &commonpb.ReadOptions{CheckpointId: 41}},
			response:   &servicepb.ListNumscriptVersionsResponse{LatestVersion: "2.0.0", Versions: []*commonpb.NumscriptVersionEntry{{Version: "1.0.0"}, {Version: "2.0.0"}}},
			wantData:   `{"latestVersion":"2.0.0","versions":[{"version":"1.0.0"},{"version":"2.0.0"}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, decoded, request := decodedQueryNumscript(t, test.commandID, []string{"main", "payout"}, test.flags, sdk.SinglePageContinuationControl())
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				actual := test.newRequest()
				if got.Operation != test.operation || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, actual) != nil || !proto.Equal(actual, test.want) {
					t.Fatalf("request = %v, want %v", actual, test.want)
				}
				return sdk.NewResponseStream(protoResponse(t, test.response)), nil
			})
			handled, err := executeV3Numscripts(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Numscripts() = %v, %v", handled, err)
			}
			result := host.Events()[0].Result
			if result.OperationID != test.operation || result.Shape != sdk.ResultObject || !equalJSON(result.Data, test.wantData) {
				t.Fatalf("result = %#v, data = %s", result, result.Data)
			}
		})
	}
}

func TestExecuteV3NumscriptsSaveReadsArtifactAndMapsApply(t *testing.T) {
	flags := []sdk.FlagOccurrence{{Name: flagVersion, Value: "1.2.3"}, {Name: flagScript, Value: "script-handle"}, {Name: flagIdempotencyKey, Value: "save-1"}}
	command, decoded, request := decodedQueryNumscript(t, "ledger.v3.numscripts.save", []string{"main", "payout"}, flags, sdk.SinglePageContinuationControl())
	memory := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		actual := &servicepb.ApplyRequest{}
		if got.Operation != opApplySaveNumscript.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, actual) != nil {
			t.Fatalf("host request = %#v", got)
		}
		batch := actual.GetUnsigned()
		if batch.GetIdempotencyKey() != "save-1" || len(batch.GetRequests()) != 1 {
			t.Fatalf("apply batch = %#v", batch)
		}
		save := batch.GetRequests()[0].GetSaveNumscript()
		if save.GetLedger() != "main" || save.GetName() != "payout" || save.GetVersion() != "1.2.3" || save.GetContent() != "send [USD 10] (\n  source = @world\n  destination = @users:42\n)" {
			t.Fatalf("save request = %#v", save)
		}
		return sdk.NewResponseStream(protoResponse(t, &servicepb.ApplyResponse{})), nil
	})
	host := &queryNumscriptInputHost{MemoryHost: memory, chunks: []sdk.InputArtifactChunk{
		{Bytes: []byte("send [USD 10] (\n  source = @world\n"), Final: false},
		{Bytes: []byte("  destination = @users:42\n)"), Final: true},
	}}

	handled, err := executeV3Numscripts(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Numscripts() = %v, %v", handled, err)
	}
	result := host.Events()[0].Result
	if host.reads != 2 || result.OperationID != opApplySaveNumscript.id || result.Shape != sdk.ResultObject || !equalJSON(result.Data, `{}`) {
		t.Fatalf("reads = %d, result = %#v, data = %s", host.reads, result, result.Data)
	}
}

func TestExecuteV3NumscriptsListPreservesAndTraversesOpaqueCursors(t *testing.T) {
	tests := []struct {
		name         string
		continuation sdk.ContinuationControl
		wantCursors  []string
		wantPage     *sdk.PageInfo
		wantData     string
	}{
		{name: "single page", continuation: sdk.SinglePageContinuationControl(), wantCursors: []string{"opaque-start"}, wantPage: &sdk.PageInfo{NextCursor: "opaque-next", HasMore: true}, wantData: `[{"name":"first","version":"1.0.0"}]`},
		{name: "all pages", continuation: sdk.AllPagesContinuationControl(), wantCursors: []string{"opaque-start", "opaque-next"}, wantData: `[{"name":"first","version":"1.0.0"},{"name":"second","version":"2.0.0"}]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags := []sdk.FlagOccurrence{{Name: flagPageSize, Value: "2"}, {Name: flagCursor, Value: "opaque-start"}, {Name: flagReverse, Value: "true"}, {Name: flagCheckpointID, Value: "41"}}
			command, decoded, request := decodedQueryNumscript(t, "ledger.v3.numscripts.list", []string{"main"}, flags, test.continuation)
			var cursors []string
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				actual := &servicepb.ListNumscriptsRequest{}
				if got.Operation != opListNumscripts.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, actual) != nil {
					t.Fatalf("host request = %#v", got)
				}
				options := actual.GetOptions()
				if actual.GetLedger() != "main" || options.GetPageSize() != 2 || !options.GetReverse() || options.GetRead().GetCheckpointId() != 41 {
					t.Fatalf("request = %#v", actual)
				}
				cursors = append(cursors, options.GetCursor())
				if len(cursors) == 1 {
					return withContinuation(sdk.NewResponseStream(protoResponse(t, &commonpb.NumscriptInfo{Name: "first", Version: "1.0.0"})), "opaque-next"), nil
				}
				return sdk.NewResponseStream(protoResponse(t, &commonpb.NumscriptInfo{Name: "second", Version: "2.0.0"})), nil
			})

			handled, err := executeV3Numscripts(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Numscripts() = %v, %v", handled, err)
			}
			result := host.Events()[0].Result
			if !reflect.DeepEqual(cursors, test.wantCursors) || !reflect.DeepEqual(result.Page, test.wantPage) || !equalJSON(result.Data, test.wantData) {
				t.Fatalf("cursors = %q, result = %#v, data = %s", cursors, result, result.Data)
			}
		})
	}
}

func TestReadArtifactRejectsUnavailableAndUnboundedInputs(t *testing.T) {
	tests := []struct {
		name   string
		handle string
		host   sdk.Host
		limit  int64
	}{
		{name: "empty handle", host: sdk.NewMemoryHost(nil), limit: 10},
		{name: "host lacks input capability", handle: "script-handle", host: sdk.NewMemoryHost(nil), limit: 10},
		{name: "host read failure", handle: "script-handle", host: &queryNumscriptInputHost{MemoryHost: sdk.NewMemoryHost(nil), err: errors.New("read failed")}, limit: 10},
		{name: "byte limit", handle: "script-handle", host: &queryNumscriptInputHost{MemoryHost: sdk.NewMemoryHost(nil), chunks: []sdk.InputArtifactChunk{{Bytes: []byte("eleven bytes"), Final: true}}}, limit: 10},
		{name: "chunk limit", handle: "script-handle", host: &queryNumscriptInputHost{MemoryHost: sdk.NewMemoryHost(nil), chunks: []sdk.InputArtifactChunk{{Bytes: []byte("a")}, {Bytes: []byte("b")}, {Bytes: []byte("c")}, {Bytes: []byte("d")}}}, limit: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := readArtifact(context.Background(), test.host, test.handle, test.limit); err == nil {
				t.Fatal("readArtifact() error = nil")
			}
		})
	}
}

func TestExecuteV3NumscriptsReturnsErrorsForReadStreamApplyAndCursorCycle(t *testing.T) {
	tests := []struct {
		name         string
		commandID    string
		arguments    []string
		flags        []sdk.FlagOccurrence
		continuation sdk.ContinuationControl
		host         sdk.Host
	}{
		{
			name: "unary request", commandID: "ledger.v3.numscripts.get", arguments: []string{"main", "payout"}, continuation: sdk.SinglePageContinuationControl(),
			host: sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) { return nil, errors.New("unavailable") }),
		},
		{
			name: "list receive", commandID: "ledger.v3.numscripts.list", arguments: []string{"main"}, continuation: sdk.SinglePageContinuationControl(),
			host: sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				return queryNumscriptErrorResponses{err: errors.New("stream failed")}, nil
			}),
		},
		{
			name: "cursor cycle", commandID: "ledger.v3.numscripts.list", arguments: []string{"main"}, flags: []sdk.FlagOccurrence{{Name: flagCursor, Value: "cycle"}}, continuation: sdk.AllPagesContinuationControl(),
			host: sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				return withContinuation(sdk.NewResponseStream(), "cycle"), nil
			}),
		},
		{
			name: "save apply", commandID: "ledger.v3.numscripts.save", arguments: []string{"main", "payout"}, flags: []sdk.FlagOccurrence{{Name: flagVersion, Value: "1.0.0"}, {Name: flagScript, Value: "script-handle"}}, continuation: sdk.SinglePageContinuationControl(),
			host: &queryNumscriptInputHost{MemoryHost: sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) { return nil, errors.New("apply failed") }), chunks: []sdk.InputArtifactChunk{{Bytes: []byte("send [USD 1]"), Final: true}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, decoded, request := decodedQueryNumscript(t, test.commandID, test.arguments, test.flags, test.continuation)
			handled, err := executeV3Numscripts(context.Background(), request, decoded, command, test.host)
			if !handled || err == nil {
				t.Fatalf("executeV3Numscripts() = %v, %v", handled, err)
			}
			if test.name == "cursor cycle" {
				var failure sdk.Failure
				if !errors.As(err, &failure) || failure.Code != string(sdk.FailureBudgetExhausted) {
					t.Fatalf("cycle error = %#v", err)
				}
			}
		})
	}

	handled, err := executeV3Numscripts(context.Background(), sdk.ExecuteRequest{}, input{}, sdk.Command{ID: "ledger.v3.queries.list"}, sdk.NewMemoryHost(nil))
	if handled || err != nil {
		t.Fatalf("unhandled command = %v, %v", handled, err)
	}
}

func decodedQueryNumscript(t *testing.T, commandID string, arguments []string, flags []sdk.FlagOccurrence, continuation sdk.ContinuationControl) (sdk.Command, input, sdk.ExecuteRequest) {
	t.Helper()
	command, ok := commandByID(commandID)
	if !ok {
		t.Fatalf("missing command %q", commandID)
	}
	request := sdk.ExecuteRequest{CommandID: commandID, Arguments: arguments, Flags: flags, Continuation: continuation}
	decoded, err := decode(command, request)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return command, decoded, request
}

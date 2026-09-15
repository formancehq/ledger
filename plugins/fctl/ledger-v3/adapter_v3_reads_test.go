package ledgerv3

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func TestExecuteV3ReadsEncodesUnaryRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, commandID, operation string
		arguments                  []string
		flags                      []sdk.FlagOccurrence
		want, response             proto.Message
	}{
		{"ledger", "ledger.v3.ledgers.get", opGetLedger.id, []string{"main"}, readFlagValues(), &servicepb.GetLedgerRequest{Ledger: "main", Read: &commonpb.ReadOptions{CheckpointId: 41}}, &commonpb.LedgerInfo{Name: "main"}},
		{"schema", "ledger.v3.ledgers.get-schema", opGetMetadataSchema.id, []string{"main"}, nil, &servicepb.GetMetadataSchemaStatusRequest{Ledger: "main"}, &servicepb.GetMetadataSchemaStatusResponse{}},
		{"stats", "ledger.v3.ledgers.stats", opGetLedgerStats.id, []string{"main"}, []sdk.FlagOccurrence{{Name: flagCheckpointID, Value: "41"}}, &servicepb.GetLedgerStatsRequest{Ledger: "main", CheckpointId: 41}, &commonpb.LedgerStats{}},
		{"account", "ledger.v3.accounts.get", opGetAccount.id, []string{"main", "users:42"}, []sdk.FlagOccurrence{{Name: flagCheckpointID, Value: "41"}, {Name: flagCollapseColors, Value: "true"}}, &servicepb.GetAccountRequest{Ledger: "main", Address: "users:42", CheckpointId: 41, CollapseColors: true}, &commonpb.Account{Address: "users:42"}},
		{"aggregate", "ledger.v3.accounts.aggregate-volumes", opAggregateVolumes.id, []string{"main"}, []sdk.FlagOccurrence{{Name: flagFilter, Value: `address ^= "users:"`}, {Name: flagGroupBy, Value: "users:"}, {Name: flagMaxPrecision, Value: "true"}, {Name: flagCollapseColors, Value: "true"}, {Name: flagCheckpointID, Value: "41"}}, &servicepb.AggregateVolumesRequest{Ledger: "main", Filter: addressPrefixFilter("users:"), GroupByPrefixes: []string{"users:"}, UseMaxPrecision: true, CollapseColors: true, CheckpointId: 41}, &commonpb.AggregateResult{}},
		{"audit", "ledger.v3.audit.get", opGetAuditEntry.id, []string{"18446744073709551615"}, nil, &servicepb.GetAuditEntryRequest{Sequence: ^uint64(0)}, &auditpb.AuditEntry{Sequence: ^uint64(0)}},
		{"log", "ledger.v3.logs.get", opGetLog.id, []string{"9007199254740993"}, []sdk.FlagOccurrence{{Name: flagCheckpointID, Value: "41"}}, &servicepb.GetLogRequest{Sequence: 9007199254740993, CheckpointId: 41}, &commonpb.Log{Sequence: 9007199254740993}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command, decoded, request := decodedRead(t, test.commandID, test.arguments, test.flags, sdk.SinglePageContinuationControl())
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				if got.Operation != test.operation {
					t.Fatalf("operation = %q, want %q", got.Operation, test.operation)
				}
				actual := test.want.ProtoReflect().Type().New().Interface()
				if err := proto.Unmarshal(got.GRPC.Message, actual); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(actual, test.want) {
					t.Fatalf("request = %v, want %v", actual, test.want)
				}
				return sdk.NewResponseStream(protoResponse(t, test.response)), nil
			})
			handled, err := executeV3Reads(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("execute = (%v, %v), want (true, nil)", handled, err)
			}
			result := host.Events()[0].Result
			if result.OperationID != test.operation || result.Shape != sdk.ResultObject {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestExecuteV3ReadsSinglePagePreservesOpaqueContinuation(t *testing.T) {
	t.Parallel()
	flags := []sdk.FlagOccurrence{{Name: flagPageSize, Value: "2"}, {Name: flagCursor, Value: "opaque-start"}, {Name: flagReverse, Value: "true"}, {Name: flagFilter, Value: `address ^= "users:"`}, {Name: flagCheckpointID, Value: "41"}}
	command, decoded, request := decodedRead(t, "ledger.v3.accounts.list", []string{"main"}, flags, sdk.SinglePageContinuationControl())
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		actual := &servicepb.ListAccountsRequest{}
		if err := proto.Unmarshal(got.GRPC.Message, actual); err != nil {
			t.Fatal(err)
		}
		want := &servicepb.ListAccountsRequest{Ledger: "main", Options: &commonpb.ListOptions{PageSize: 2, Cursor: "opaque-start", Reverse: true, Filter: addressPrefixFilter("users:"), Read: &commonpb.ReadOptions{CheckpointId: 41}}}
		if !proto.Equal(actual, want) {
			t.Fatalf("request = %v, want %v", actual, want)
		}
		return withContinuation(sdk.NewResponseStream(protoResponse(t, &commonpb.Account{Address: "users:42"})), "opaque-next"), nil
	})
	handled, err := executeV3Reads(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("execute = (%v, %v)", handled, err)
	}
	result := host.Events()[0].Result
	if result.Shape != sdk.ResultCollection || result.Page == nil || result.Page.NextCursor != "opaque-next" || !result.Page.HasMore {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecuteV3ReadsAllPagesFollowsCursorAndAggregates(t *testing.T) {
	t.Parallel()
	command, decoded, request := decodedRead(t, "ledger.v3.logs.list", []string{"main"}, nil, sdk.AllPagesContinuationControl())
	var cursors []string
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		actual := &servicepb.ListLogsRequest{}
		if err := proto.Unmarshal(got.GRPC.Message, actual); err != nil {
			t.Fatal(err)
		}
		cursors = append(cursors, actual.GetOptions().GetCursor())
		if len(cursors) == 1 {
			return withContinuation(sdk.NewResponseStream(protoResponse(t, &commonpb.Log{Sequence: 1})), "server-token"), nil
		}
		return sdk.NewResponseStream(protoResponse(t, &commonpb.Log{Sequence: 2})), nil
	})
	handled, err := executeV3Reads(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("execute = (%v, %v)", handled, err)
	}
	if !reflect.DeepEqual(cursors, []string{"", "server-token"}) {
		t.Fatalf("cursors = %q", cursors)
	}
	result := host.Events()[0].Result
	if result.Page != nil || string(result.Data) != `[{"sequence":1,"responseSignature":{}},{"sequence":2,"responseSignature":{}}]` {
		t.Fatalf("result = %#v data=%s", result, result.Data)
	}
}

func TestExecuteV3ReadsAllPagesRejectsCursorCycle(t *testing.T) {
	t.Parallel()
	command, decoded, request := decodedRead(t, "ledger.v3.audit.list", nil, []sdk.FlagOccurrence{{Name: flagCursor, Value: "cycle"}}, sdk.AllPagesContinuationControl())
	host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
		return withContinuation(sdk.NewResponseStream(protoResponse(t, &auditpb.AuditEntry{Sequence: 1})), "cycle"), nil
	})
	handled, err := executeV3Reads(context.Background(), request, decoded, command, host)
	var failure sdk.Failure
	if !handled || !errors.As(err, &failure) || failure.Code != string(sdk.FailureBudgetExhausted) {
		t.Fatalf("execute = (%v, %#v)", handled, err)
	}
}

func TestCollectV3PagesEnforcesEveryHostCeiling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		control sdk.ContinuationControl
		fetch   func(string) ([]proto.Message, string, error)
	}{
		{
			name:    "pages",
			control: sdk.ContinuationControl{Mode: sdk.ContinuationAllPages, MaxPages: 1, MaxItems: 10, MaxBytes: 1024},
			fetch: func(string) ([]proto.Message, string, error) {
				return []proto.Message{&commonpb.Log{Sequence: 1}}, "more", nil
			},
		},
		{
			name:    "items",
			control: sdk.ContinuationControl{Mode: sdk.ContinuationAllPages, MaxPages: 2, MaxItems: 1, MaxBytes: 1024},
			fetch: func(string) ([]proto.Message, string, error) {
				return []proto.Message{&commonpb.Log{Sequence: 1}, &commonpb.Log{Sequence: 2}}, "", nil
			},
		},
		{
			name:    "bytes",
			control: sdk.ContinuationControl{Mode: sdk.ContinuationAllPages, MaxPages: 2, MaxItems: 10, MaxBytes: 2},
			fetch: func(string) ([]proto.Message, string, error) {
				return []proto.Message{&commonpb.Log{Sequence: 1}}, "", nil
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := collectV3Pages(test.control, "", test.fetch)
			var failure sdk.Failure
			if !errors.As(err, &failure) || failure.Code != string(sdk.FailureBudgetExhausted) {
				t.Fatalf("error = %#v, want budget-exhausted failure", err)
			}
		})
	}
}

func TestCollectV3PagesMeasuresTheProductJSONThatWillBeEmitted(t *testing.T) {
	t.Parallel()

	_, _, err := collectV3Pages(sdk.ContinuationControl{Mode: sdk.ContinuationAllPages, MaxPages: 1, MaxItems: 1, MaxBytes: 20}, "", func(string) ([]proto.Message, string, error) {
		return []proto.Message{&commonpb.Log{Sequence: 1}}, "", nil
	})
	var failure sdk.Failure
	if !errors.As(err, &failure) || failure.Code != string(sdk.FailureBudgetExhausted) {
		t.Fatalf("error = %#v, want custom JSON budget failure", err)
	}
}

func TestExecuteV3ReadsEmitsOnlyTheFinalAccountAnalysisResult(t *testing.T) {
	t.Parallel()
	command, decoded, request := decodedRead(t, "ledger.v3.accounts.analyze", []string{"main"}, []sdk.FlagOccurrence{{Name: flagVariableThreshold, Value: "8"}}, sdk.SinglePageContinuationControl())
	wantSchema := outputSchemaFor(false, []sdk.TableColumn{{Header: "TOTAL ACCOUNTS", Field: "totalAccounts"}})
	if !reflect.DeepEqual(command.RawOutputSchema, wantSchema) || !reflect.DeepEqual(command.PublicOutputSchema, wantSchema) {
		t.Fatalf("accounts analyze output schema = %s, want render-aware object %s", command.RawOutputSchema, wantSchema)
	}
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		actual := &servicepb.AnalyzeAccountsRequest{}
		if err := proto.Unmarshal(got.GRPC.Message, actual); err != nil {
			t.Fatal(err)
		}
		if want := (&servicepb.AnalyzeAccountsRequest{Ledger: "main", VariableThreshold: 8}); !proto.Equal(actual, want) {
			t.Fatalf("request = %v", actual)
		}
		return sdk.NewResponseStream(
			protoResponse(t, &servicepb.AnalyzeAccountsEvent{Type: &servicepb.AnalyzeAccountsEvent_Progress{Progress: &servicepb.AnalyzeProgress{Processed: 4, Total: 10}}}),
			protoResponse(t, &servicepb.AnalyzeAccountsEvent{Type: &servicepb.AnalyzeAccountsEvent_Result{Result: &servicepb.AnalyzeAccountsResponse{
				TotalAccounts: 10,
				Patterns: []*servicepb.AccountPattern{{
					Pattern: "users:{id}", AccountCount: 9,
					Segments: []*servicepb.PatternSegment{{Position: 1, Type: servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE, VariableName: "id", UniqueValues: 9}},
				}},
			}}}),
		), nil
	})
	handled, err := executeV3Reads(context.Background(), request, decoded, command, host)
	if err != nil || !handled || len(host.Events()) != 2 || host.Events()[0].Kind != sdk.EventProgress || host.Events()[1].Result == nil || host.Events()[1].Result.Shape != sdk.ResultObject {
		t.Fatalf("execute = (%v, %v)", handled, err)
	}
	if got := host.Events()[0].Payload; !equalJSON(got, `{"processed":4,"total":10,"phase":""}`) {
		t.Fatalf("progress = %s", got)
	}
	if got := host.Events()[1].Result.Data; !equalJSON(got, `{"patterns":[{"pattern":"users:{id}","accountCount":9,"assets":[],"metadataKeys":[],"segments":[{"position":1,"type":"variable","variableName":"id","uniqueValues":9,"examples":[]}]}],"totalAccounts":10}`) {
		t.Fatalf("result = %s", got)
	}

	command, decoded, request = decodedRead(t, "ledger.v3.accounts.analyze", []string{"main"}, nil, sdk.SinglePageContinuationControl())
	host = sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
		return sdk.NewResponseStream(protoResponse(t, &servicepb.AnalyzeAccountsEvent{Type: &servicepb.AnalyzeAccountsEvent_Progress{Progress: &servicepb.AnalyzeProgress{Processed: 1}}})), nil
	})
	handled, err = executeV3Reads(context.Background(), request, decoded, command, host)
	if !handled || err == nil {
		t.Fatalf("missing result = (%v, %v), want failure", handled, err)
	}

	// A different command family is still declined.
	command, decoded, request = decodedRead(t, "ledger.v3.transactions.get", []string{"main", "1"}, nil, sdk.SinglePageContinuationControl())
	handled, err = executeV3Reads(context.Background(), request, decoded, command, sdk.NewMemoryHost(nil))
	if err != nil || handled {
		t.Fatalf("outside family = (%v, %v)", handled, err)
	}
}

func decodedRead(t *testing.T, commandID string, arguments []string, flags []sdk.FlagOccurrence, continuation sdk.ContinuationControl) (sdk.Command, input, sdk.ExecuteRequest) {
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

func readFlagValues() []sdk.FlagOccurrence {
	return []sdk.FlagOccurrence{{Name: flagCheckpointID, Value: "41"}}
}
func addressPrefixFilter(prefix string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix}}}}
}
func protoResponse(t *testing.T, message proto.Message) sdk.Response {
	t.Helper()
	encoded, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return sdk.Response{Body: encoded}
}

type continuationResponses struct {
	sdk.Responses
	continuation string
}

func (r continuationResponses) ResponseStreamMetadata() sdk.ResponseStreamMetadata {
	return sdk.ResponseStreamMetadata{Continuation: r.continuation}
}
func withContinuation(responses sdk.Responses, continuation string) sdk.Responses {
	return continuationResponses{Responses: responses, continuation: continuation}
}

package ledgerv3

import (
	"context"
	"encoding/json"
	"math/big"
	"reflect"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

type transactionMetadataResponses struct {
	sdk.Responses
	continuation string
}

func (r transactionMetadataResponses) ResponseStreamMetadata() sdk.ResponseStreamMetadata {
	return sdk.ResponseStreamMetadata{Continuation: r.continuation}
}

type transactionInputHost struct {
	*sdk.MemoryHost
	chunks []sdk.InputArtifactChunk
	reads  int
}

func (h *transactionInputHost) ReadInput(_ context.Context, handle string) (sdk.InputArtifactChunk, error) {
	if handle != "script-handle" || h.reads >= len(h.chunks) {
		return sdk.InputArtifactChunk{}, v3Failure("unexpected test input read")
	}
	chunk := h.chunks[h.reads]
	h.reads++
	return chunk, nil
}

func transactionProtoResponse(t *testing.T, message proto.Message) sdk.Response {
	t.Helper()
	encoded, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	return sdk.Response{Body: encoded}
}

func TestExecuteV3TransactionsGetMapsTheGeneratedRequestAndResponse(t *testing.T) {
	command, ok := commandByID("ledger.v3.transactions.get")
	if !ok {
		t.Fatal("transactions get command is absent")
	}
	request := sdk.ExecuteRequest{
		CommandID: command.ID,
		Arguments: []string{"main", "18446744073709551615"},
		Flags:     []sdk.FlagOccurrence{{Name: flagCheckpointID, Value: "17"}},
	}
	decoded, err := decode(command, request)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		if got.Operation != opGetTransaction.id || got.GRPC == nil || got.GRPC.FullMethod != bucketFullMethod("GetTransaction") {
			t.Fatalf("host request = %#v", got)
		}
		var wire servicepb.GetTransactionRequest
		if err := proto.Unmarshal(got.GRPC.Message, &wire); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if wire.GetLedger() != "main" || wire.GetTransactionId() != ^uint64(0) || wire.GetCheckpointId() != 17 {
			t.Fatalf("generated request = %#v", &wire)
		}
		body, err := proto.Marshal(&servicepb.GetTransactionResponse{Transaction: &commonpb.Transaction{
			Id:        42,
			Timestamp: &commonpb.Timestamp{Data: 1_776_864_120_966_130},
			Postings:  []*commonpb.Posting{{Source: "world", Destination: "users:001", Asset: "USD", Amount: commonpb.NewUint256FromUint64(5_000_000_000)}},
		}})
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return sdk.NewResponseStream(sdk.Response{Body: body}), nil
	})

	handled, err := executeV3Transactions(context.Background(), request, decoded, command, host)
	if err != nil {
		t.Fatalf("executeV3Transactions() error = %v", err)
	}
	if !handled {
		t.Fatal("executeV3Transactions() handled = false")
	}
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != opGetTransaction.id || events[0].Result.Shape != sdk.ResultObject {
		t.Fatalf("events = %#v", events)
	}
	var result map[string]any
	if err := json.Unmarshal(events[0].Result.Data, &result); err != nil {
		t.Fatalf("result JSON = %s, error = %v", events[0].Result.Data, err)
	}
	transaction, ok := result["transaction"].(map[string]any)
	if !ok || transaction["id"] != float64(42) || transaction["timestamp"] != "2026-04-22T13:22:00.96613Z" {
		t.Fatalf("result = %#v", result)
	}
	posting := transaction["postings"].([]any)[0].(map[string]any)
	if posting["amount"] != float64(5_000_000_000) {
		t.Fatalf("posting = %#v", posting)
	}
}

func TestExecuteV3TransactionsAnalyzeConsumesProgressAndEmitsTheFinalResult(t *testing.T) {
	command, _ := commandByID("ledger.v3.transactions.analyze")
	request := sdk.ExecuteRequest{CommandID: command.ID, Arguments: []string{"main"}, Flags: []sdk.FlagOccurrence{{Name: flagVariableThreshold, Value: "12"}}}
	decoded, err := decode(command, request)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		var wire servicepb.AnalyzeTransactionsRequest
		if got.Operation != opAnalyzeTransactions.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		if wire.GetLedger() != "main" || wire.GetVariableThreshold() != 12 {
			t.Fatalf("generated request = %#v", &wire)
		}
		return sdk.NewResponseStream(
			transactionProtoResponse(t, &servicepb.AnalyzeTransactionsEvent{Type: &servicepb.AnalyzeTransactionsEvent_Progress{Progress: &servicepb.AnalyzeProgress{Processed: 3, Total: 10}}}),
			transactionProtoResponse(t, &servicepb.AnalyzeTransactionsEvent{Type: &servicepb.AnalyzeTransactionsEvent_Result{Result: &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 9, TotalReverted: 2,
				FlowPatterns: []*servicepb.FlowPattern{{
					Signature: "world->users:{id}[USD]", Structure: servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE, TransactionCount: 7,
					Postings: []*servicepb.NormalizedPosting{{SourcePattern: "world", DestinationPattern: "users:{id}", Asset: "USD"}},
					Temporal: &servicepb.TemporalStats{FirstSeen: &commonpb.Timestamp{Data: 1_000_000}, LastSeen: &commonpb.Timestamp{Data: 2_000_000}, TransactionsPerDay: 3.5},
				}},
			}}}),
		), nil
	})

	handled, err := executeV3Transactions(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Transactions() = %v, %v", handled, err)
	}
	events := host.Events()
	if len(events) != 2 || events[0].Kind != sdk.EventProgress || events[1].Result == nil || events[1].Result.OperationID != opAnalyzeTransactions.id {
		t.Fatalf("events = %#v", events)
	}
	if !equalJSON(events[0].Payload, `{"processed":3,"total":10,"phase":""}`) {
		t.Fatalf("progress = %s", events[0].Payload)
	}
	if !equalJSON(events[1].Result.Data, `{"flowPatterns":[{"signature":"world->users:{id}[USD]","structure":"simple","transactionCount":7,"postings":[{"sourcePattern":"world","destinationPattern":"users:{id}","asset":"USD","color":""}],"temporal":{"firstSeen":"1970-01-01T00:00:01Z","lastSeen":"1970-01-01T00:00:02Z","transactionsPerDay":3.5},"volumeStats":[],"metadataKeys":[]}],"totalTransactions":9,"totalReverted":2}`) {
		t.Fatalf("result = %s", events[1].Result.Data)
	}
}

func TestExecuteV3TransactionsListTraversesOpaqueCursorsWithinHostBounds(t *testing.T) {
	command, _ := commandByID("ledger.v3.transactions.list")
	request := sdk.ExecuteRequest{
		CommandID: command.ID,
		Arguments: []string{"main"},
		Flags: []sdk.FlagOccurrence{
			{Name: flagPageSize, Value: "2"}, {Name: flagFilter, Value: `address ^= "users:"`},
			{Name: flagCheckpointID, Value: "11"}, {Name: flagReverse, Value: "true"},
		},
		Continuation: sdk.AllPagesContinuationControl(),
	}
	decoded, err := decode(command, request)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	calls := 0
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		calls++
		var wire servicepb.ListTransactionsRequest
		if got.Operation != opListTransactions.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		options := wire.GetOptions()
		if options.GetPageSize() != 2 || !options.GetReverse() || options.GetFilter() == nil || options.GetRead().GetCheckpointId() != 11 {
			t.Fatalf("list options = %#v", options)
		}
		switch calls {
		case 1:
			if options.GetCursor() != "" {
				t.Fatalf("first cursor = %q", options.GetCursor())
			}
			return transactionMetadataResponses{Responses: sdk.NewResponseStream(transactionProtoResponse(t, &commonpb.Transaction{Id: 1})), continuation: "opaque-next"}, nil
		case 2:
			if options.GetCursor() != "opaque-next" {
				t.Fatalf("second cursor = %q", options.GetCursor())
			}
			return transactionMetadataResponses{Responses: sdk.NewResponseStream(transactionProtoResponse(t, &commonpb.Transaction{Id: 2}))}, nil
		default:
			t.Fatalf("unexpected request %d", calls)
			return nil, nil
		}
	})

	handled, err := executeV3Transactions(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Transactions() = %v, %v", handled, err)
	}
	if calls != 2 {
		t.Fatalf("host calls = %d, want 2", calls)
	}
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.Page != nil || !equalJSON(events[0].Result.Data, `[{"postings":[],"metadata":{},"id":1,"reverted":false},{"postings":[],"metadata":{},"id":2,"reverted":false}]`) {
		t.Fatalf("events = %#v", events)
	}
}

func TestExecuteV3TransactionsCreateReadsTheDeclaredArtifactAndMapsTheApplyBatch(t *testing.T) {
	command, _ := commandByID("ledger.v3.transactions.create")
	request := sdk.ExecuteRequest{
		CommandID: command.ID,
		Arguments: []string{"main"},
		Flags: []sdk.FlagOccurrence{
			{Name: flagScript, Value: "script-handle"}, {Name: flagScriptVar, Value: "account=users:42"},
			{Name: flagReference, Value: "invoice-1"}, {Name: flagTimestamp, Value: "2026-09-11T12:34:56.123456Z"},
			{Name: flagMetadata, Value: "source=checkout"}, {Name: flagForce, Value: "true"}, {Name: flagIdempotencyKey, Value: "apply-1"},
		},
	}
	decoded, err := decode(command, request)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	memory := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		var wire servicepb.ApplyRequest
		if got.Operation != opApplyCreateTransaction.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		batch := wire.GetUnsigned()
		if batch.GetIdempotencyKey() != "apply-1" || len(batch.GetRequests()) != 1 {
			t.Fatalf("apply batch = %#v", batch)
		}
		apply := batch.GetRequests()[0].GetApply()
		payload := apply.GetAction().GetCreateTransaction()
		if apply.GetLedger() != "main" || payload.GetScript().GetPlain() != "send [USD/2 10] (\n  source = @world\n  destination = $account\n)" || payload.GetScript().GetVars()["account"] != "users:42" {
			t.Fatalf("create payload = %#v", payload)
		}
		if payload.GetReference() != "invoice-1" || !payload.GetForce() || payload.GetTimestamp().GetData() != 1789130096123456 || payload.GetMetadata()["source"].GetStringValue() != "checkout" {
			t.Fatalf("create payload = %#v", payload)
		}
		created := &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 27}}
		return sdk.NewResponseStream(transactionProtoResponse(t, transactionCreatedResponse(created))), nil
	})
	host := &transactionInputHost{MemoryHost: memory, chunks: []sdk.InputArtifactChunk{
		{Bytes: []byte("send [USD/2 10] (\n  source = @world\n"), Final: false},
		{Bytes: []byte("  destination = $account\n)"), Final: true},
	}}

	handled, err := executeV3Transactions(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Transactions() = %v, %v", handled, err)
	}
	if host.reads != 2 {
		t.Fatalf("artifact reads = %d, want 2", host.reads)
	}
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != opApplyCreateTransaction.id || !equalJSON(events[0].Result.Data, `{"transaction":{"postings":[],"metadata":{},"id":27,"reverted":false}}`) {
		t.Fatalf("events = %#v, data = %s", events, events[0].Result.Data)
	}
}

func transactionCreatedResponse(created *commonpb.CreatedTransaction) *servicepb.ApplyResponse {
	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: created}}},
	}}}}}}
}

func transactionRevertedResponse(reverted *commonpb.RevertedTransaction) *servicepb.ApplyResponse {
	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: reverted}}},
	}}}}}}
}

func TestExecuteV3TransactionMetadataAndRevertCommandsMapTypedApplyActions(t *testing.T) {
	tests := []struct {
		name        string
		commandID   string
		operationID string
		arguments   []string
		flags       []sdk.FlagOccurrence
		check       func(*testing.T, *servicepb.ApplyBatch)
		response    *servicepb.ApplyResponse
		wantShape   sdk.ResultShape
		wantData    string
	}{
		{
			name: "delete metadata", commandID: "ledger.v3.transactions.delete-metadata", operationID: opApplyDeleteMetadata.id,
			arguments: []string{"main", "42", "category"}, flags: []sdk.FlagOccurrence{{Name: flagIdempotencyKey, Value: "delete-1"}},
			check: func(t *testing.T, batch *servicepb.ApplyBatch) {
				t.Helper()
				payload := batch.GetRequests()[0].GetApply().GetAction().GetDeleteMetadata()
				if payload.GetTarget().GetTransactionId() != 42 || payload.GetKey() != "category" {
					t.Fatalf("delete metadata payload = %#v", payload)
				}
			},
			response: &servicepb.ApplyResponse{}, wantShape: sdk.ResultEmpty, wantData: `{}`,
		},
		{
			name: "set metadata", commandID: "ledger.v3.transactions.set-metadata", operationID: opApplyAddMetadata.id,
			arguments: []string{"main", "43"}, flags: []sdk.FlagOccurrence{{Name: flagMetadata, Value: "category=payout"}, {Name: flagMetadata, Value: "owner=alice"}},
			check: func(t *testing.T, batch *servicepb.ApplyBatch) {
				t.Helper()
				payload := batch.GetRequests()[0].GetApply().GetAction().GetAddMetadata()
				if payload.GetTarget().GetTransactionId() != 43 || payload.GetMetadata()["category"].GetStringValue() != "payout" || payload.GetMetadata()["owner"].GetStringValue() != "alice" {
					t.Fatalf("set metadata payload = %#v", payload)
				}
			},
			response: &servicepb.ApplyResponse{}, wantShape: sdk.ResultEmpty, wantData: `{}`,
		},
		{
			name: "revert", commandID: "ledger.v3.transactions.revert", operationID: opApplyRevertTransaction.id,
			arguments: []string{"main", "44"}, flags: []sdk.FlagOccurrence{
				{Name: flagForce, Value: "true"}, {Name: flagAtEffectiveDate, Value: "true"},
				{Name: flagMetadata, Value: "reason=refund"}, {Name: flagIdempotencyKey, Value: "revert-1"},
			},
			check: func(t *testing.T, batch *servicepb.ApplyBatch) {
				t.Helper()
				payload := batch.GetRequests()[0].GetApply().GetAction().GetRevertTransaction()
				if payload.GetTransactionId() != 44 || !payload.GetForce() || !payload.GetAtEffectiveDate() || payload.GetMetadata()["reason"].GetStringValue() != "refund" {
					t.Fatalf("revert payload = %#v", payload)
				}
			},
			response:  transactionRevertedResponse(&commonpb.RevertedTransaction{RevertedTransactionId: 44, RevertTransaction: &commonpb.Transaction{Id: 45}}),
			wantShape: sdk.ResultObject, wantData: `{"revertedTransactionId":44,"revertTransaction":{"postings":[],"metadata":{},"id":45,"reverted":false}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, _ := commandByID(test.commandID)
			request := sdk.ExecuteRequest{CommandID: command.ID, Arguments: test.arguments, Flags: test.flags}
			decoded, err := decode(command, request)
			if err != nil {
				t.Fatalf("decode() error = %v", err)
			}
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				var wire servicepb.ApplyRequest
				if got.Operation != test.operationID || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
					t.Fatalf("host request = %#v", got)
				}
				batch := wire.GetUnsigned()
				if batch == nil || len(batch.GetRequests()) != 1 {
					t.Fatalf("apply batch = %#v", batch)
				}
				test.check(t, batch)
				return sdk.NewResponseStream(transactionProtoResponse(t, test.response)), nil
			})

			handled, err := executeV3Transactions(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Transactions() = %v, %v", handled, err)
			}
			events := host.Events()
			if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != test.operationID || events[0].Result.Shape != test.wantShape || !equalJSON(events[0].Result.Data, test.wantData) {
				t.Fatalf("events = %#v, data = %s, want = %s", events, events[0].Result.Data, test.wantData)
			}
		})
	}
}

func TestParseV3PostingAcceptsLedgerctlAndDocumentedFormats(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  *commonpb.Posting
	}{
		{name: "ledgerctl comma format", value: "world,users:42,100,USD/2,BLUE", want: commonpb.NewColoredPosting("world", "users:42", "USD/2", "BLUE", bigInt(100))},
		{name: "descriptor colon format", value: "world:USD/2:100:users:42", want: commonpb.NewPosting("world", "users:42", "USD/2", bigInt(100))},
		{name: "colon format with segmented source", value: "merchants:shop:USD:50:users:42", want: commonpb.NewPosting("merchants:shop", "users:42", "USD", bigInt(50))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseV3Posting(test.value)
			if err != nil {
				t.Fatalf("parseV3Posting() error = %v", err)
			}
			if !proto.Equal(got, test.want) {
				t.Fatalf("parseV3Posting() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestTransactionSourceRejectsAmbiguousOrUnboundedInputs(t *testing.T) {
	command, _ := commandByID("ledger.v3.transactions.create")
	tests := []struct {
		name  string
		flags []sdk.FlagOccurrence
		host  sdk.Host
	}{
		{
			name: "script and posting", flags: []sdk.FlagOccurrence{{Name: flagScript, Value: "script-handle"}, {Name: flagPosting, Value: "world,users:42,1,USD"}},
			host: &transactionInputHost{MemoryHost: sdk.NewMemoryHost(nil)},
		},
		{
			name: "script variable without script", flags: []sdk.FlagOccurrence{{Name: flagScriptVar, Value: "account=users:42"}},
			host: &transactionInputHost{MemoryHost: sdk.NewMemoryHost(nil)},
		},
		{
			name: "empty non-final artifact chunk", flags: []sdk.FlagOccurrence{{Name: flagScript, Value: "script-handle"}},
			host: &transactionInputHost{MemoryHost: sdk.NewMemoryHost(nil), chunks: []sdk.InputArtifactChunk{{Final: false}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := sdk.ExecuteRequest{CommandID: command.ID, Arguments: []string{"main"}, Flags: test.flags}
			decoded, err := decode(command, request)
			if err != nil {
				t.Fatalf("decode() error = %v", err)
			}
			if _, _, err := transactionSource(context.Background(), decoded, test.host); err == nil {
				t.Fatal("transactionSource() error = nil")
			}
		})
	}
}

func TestExecuteV3TransactionsReturnsUnhandledForAnotherFamily(t *testing.T) {
	handled, err := executeV3Transactions(context.Background(), sdk.ExecuteRequest{CommandID: "ledger.v3.accounts.get"}, input{}, sdk.Command{}, sdk.NewMemoryHost(nil))
	if err != nil || handled {
		t.Fatalf("executeV3Transactions() = %v, %v", handled, err)
	}
}

func bigInt(value int64) *big.Int { return new(big.Int).SetInt64(value) }

func equalJSON(actual []byte, expected string) bool {
	var actualValue any
	var expectedValue any
	if json.Unmarshal(actual, &actualValue) != nil || json.Unmarshal([]byte(expected), &expectedValue) != nil {
		return false
	}
	return reflect.DeepEqual(actualValue, expectedValue)
}

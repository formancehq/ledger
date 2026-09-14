package ledgerv3

import (
	"context"
	"errors"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func TestExecuteV3IndexMutationsMapTypedApplyRequests(t *testing.T) {
	metadataID := &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
		Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
		Key:    "category",
	}}}
	tests := []struct {
		name      string
		commandID string
		operation string
		want      *servicepb.Request
	}{
		{
			name:      "create",
			commandID: "ledger.v3.indexes.create",
			operation: opApplyCreateIndex.id,
			want: &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: "main",
				Id:     metadataID,
			}}},
		},
		{
			name:      "drop",
			commandID: "ledger.v3.indexes.drop",
			operation: opApplyDropIndex.id,
			want: &servicepb.Request{Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{
				Ledger: "main",
				Id:     metadataID,
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags := []sdk.FlagOccurrence{
				{Name: flagIndexKind, Value: "metadata"},
				{Name: flagTargetType, Value: "transaction"},
				{Name: flagMetadataKey, Value: "category"},
				{Name: flagIdempotencyKey, Value: "index-write-1"},
			}
			command, decoded, request := decodedRead(t, test.commandID, []string{"main"}, flags, sdk.ContinuationControl{})
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				wire := &servicepb.ApplyRequest{}
				if got.Operation != test.operation || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, wire) != nil {
					t.Fatalf("host request = %#v", got)
				}
				batch := wire.GetUnsigned()
				if batch == nil || batch.GetIdempotencyKey() != "index-write-1" || len(batch.GetRequests()) != 1 || !proto.Equal(batch.GetRequests()[0], test.want) {
					t.Fatalf("apply batch = %v, want request %v", batch, test.want)
				}
				return sdk.NewResponseStream(protoResponse(t, &servicepb.ApplyResponse{})), nil
			})

			handled, err := executeV3Indexes(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Indexes() = (%v, %v), want (true, nil)", handled, err)
			}
			assertResult(t, host, test.operation, sdk.ResultEmpty, `{}`, nil)
		})
	}
}

func TestExecuteV3IndexesInspectMapsEveryMode(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want servicepb.InspectIndexMode
	}{
		{name: "distinct values", mode: "distinct-values", want: servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES},
		{name: "facets", mode: "facets", want: servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS},
		{name: "summary", mode: "summary", want: servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags := []sdk.FlagOccurrence{
				{Name: flagTargetType, Value: "ledger"},
				{Name: flagMetadataKey, Value: "category"},
				{Name: flagInspectMode, Value: test.mode},
				{Name: flagPageSize, Value: "25"},
				{Name: flagCursor, Value: "inspect-start"},
				{Name: flagCheckpointID, Value: "51"},
			}
			command, decoded, request := decodedRead(t, "ledger.v3.indexes.inspect", []string{"main"}, flags, sdk.ContinuationControl{})
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				wire := &servicepb.InspectIndexRequest{}
				if got.Operation != opInspectIndex.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, wire) != nil {
					t.Fatalf("host request = %#v", got)
				}
				want := &servicepb.InspectIndexRequest{Ledger: "main", TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER, MetadataKey: "category", Mode: test.want, PageSize: 25, Cursor: "inspect-start", CheckpointId: 51}
				if !proto.Equal(wire, want) {
					t.Fatalf("request = %v, want %v", wire, want)
				}
				return sdk.NewResponseStream(protoResponse(t, &servicepb.InspectIndexResponse{})), nil
			})

			handled, err := executeV3Indexes(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Indexes() = (%v, %v), want (true, nil)", handled, err)
			}
			assertResult(t, host, opInspectIndex.id, sdk.ResultObject, `{}`, nil)
		})
	}
}

func TestExecuteV3IndexesListMapsLedgerScopeAndCollectionResult(t *testing.T) {
	command, decoded, request := decodedRead(t, "ledger.v3.indexes.list", []string{"main"}, nil, sdk.ContinuationControl{})
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		wire := &servicepb.ListIndexesRequest{}
		if got.Operation != opListIndexes.id || got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		want := &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: "main"}
		if !proto.Equal(wire, want) {
			t.Fatalf("request = %v, want %v", wire, want)
		}
		return sdk.NewResponseStream(protoResponse(t, &commonpb.Index{Ledger: "main", ForwardEncodingVersion: 2})), nil
	})

	handled, err := executeV3Indexes(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Indexes() = (%v, %v), want (true, nil)", handled, err)
	}
	assertResult(t, host, opListIndexes.id, sdk.ResultCollection, `[{"ledger":"main","forwardEncodingVersion":2}]`, nil)
}

func TestParseIndexIDAcceptsEveryDeclaredKind(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   *commonpb.IndexID
	}{
		{
			name:   "metadata",
			values: map[string]string{flagIndexKind: "metadata", flagTargetType: "account", flagMetadataKey: "category"},
			want: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
				Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:    "category",
			}}},
		},
		{
			name:   "transaction builtin",
			values: map[string]string{flagIndexKind: "tx-builtin", flagBuiltin: "destination-address"},
			want:   &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS}},
		},
		{
			name:   "account builtin",
			values: map[string]string{flagIndexKind: "account-builtin", flagBuiltin: "asset"},
			want:   &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET}},
		},
		{
			name:   "log builtin",
			values: map[string]string{flagIndexKind: "log-builtin", flagBuiltin: "date"},
			want:   &commonpb.IndexID{Kind: &commonpb.IndexID_LogBuiltin{LogBuiltin: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseIndexID(input{scalars: test.values})
			if err != nil || !proto.Equal(got, test.want) {
				t.Fatalf("parseIndexID() = (%v, %v), want (%v, nil)", got, err, test.want)
			}
		})
	}
}

func TestParseIndexIDRejectsInvalidCombinations(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "unknown kind", values: map[string]string{flagIndexKind: "other"}},
		{name: "metadata without key", values: map[string]string{flagIndexKind: "metadata", flagTargetType: "account"}},
		{name: "metadata with builtin", values: map[string]string{flagIndexKind: "metadata", flagTargetType: "account", flagMetadataKey: "category", flagBuiltin: "asset"}},
		{name: "metadata with invalid target", values: map[string]string{flagIndexKind: "metadata", flagTargetType: "invalid", flagMetadataKey: "category"}},
		{name: "unknown transaction builtin", values: map[string]string{flagIndexKind: "tx-builtin", flagBuiltin: "asset"}},
		{name: "wrong account builtin", values: map[string]string{flagIndexKind: "account-builtin", flagBuiltin: "date"}},
		{name: "wrong log builtin", values: map[string]string{flagIndexKind: "log-builtin", flagBuiltin: "asset"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseIndexID(input{scalars: test.values})
			var failure sdk.Failure
			if got != nil || !errors.As(err, &failure) || failure.Code != string(sdk.FailureInvalidArgument) {
				t.Fatalf("parseIndexID() = (%v, %#v), want invalid-argument failure", got, err)
			}
		})
	}
}

func assertResult(t *testing.T, host *sdk.MemoryHost, operation string, shape sdk.ResultShape, data string, page *sdk.PageInfo) {
	t.Helper()
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil {
		t.Fatalf("events = %#v, want one result", events)
	}
	result := events[0].Result
	if result.OperationID != operation || result.Shape != shape || !equalJSON(result.Data, data) {
		t.Fatalf("result = %#v, data = %s", result, result.Data)
	}
	if page == nil {
		if result.Page != nil {
			t.Fatalf("page = %#v, want nil", result.Page)
		}
		return
	}
	if result.Page == nil || *result.Page != *page {
		t.Fatalf("page = %#v, want %#v", result.Page, page)
	}
}

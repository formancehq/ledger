package ledgerv3

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func TestExecuteV3AccountMutationsMapsApplyRequestsAndEmitsEmptyResults(t *testing.T) {
	tests := []struct {
		name      string
		commandID string
		arguments []string
		flags     []sdk.FlagOccurrence
		operation string
		check     func(*testing.T, *servicepb.LedgerApplyRequest)
	}{
		{
			name:      "set metadata",
			commandID: "ledger.v3.accounts.set-metadata",
			arguments: []string{"main", "users:42"},
			flags: []sdk.FlagOccurrence{
				{Name: flagMetadata, Value: "category=premium"},
				{Name: flagMetadata, Value: "enabled=true"},
				{Name: flagIdempotencyKey, Value: "metadata-1"},
			},
			operation: opApplyAddMetadata.id,
			check: func(t *testing.T, apply *servicepb.LedgerApplyRequest) {
				t.Helper()
				payload := apply.GetAction().GetAddMetadata()
				if apply.GetLedger() != "main" || payload.GetTarget().GetAccount().GetAddr() != "users:42" {
					t.Fatalf("set metadata request = %#v", apply)
				}
				if payload.GetMetadata()["category"].GetStringValue() != "premium" || payload.GetMetadata()["enabled"].GetStringValue() != "true" {
					t.Fatalf("set metadata payload = %#v", payload)
				}
			},
		},
		{
			name:      "delete metadata",
			commandID: "ledger.v3.accounts.delete-metadata",
			arguments: []string{"main", "users:42", "category"},
			flags:     []sdk.FlagOccurrence{{Name: flagIdempotencyKey, Value: "metadata-2"}},
			operation: opApplyDeleteMetadata.id,
			check: func(t *testing.T, apply *servicepb.LedgerApplyRequest) {
				t.Helper()
				payload := apply.GetAction().GetDeleteMetadata()
				if apply.GetLedger() != "main" || payload.GetTarget().GetAccount().GetAddr() != "users:42" || payload.GetKey() != "category" {
					t.Fatalf("delete metadata request = %#v", apply)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, request, decoded := decodedMutation(t, test.commandID, test.arguments, test.flags)
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				batch := decodeApplyBatch(t, got, test.operation)
				if batch.GetIdempotencyKey() != test.flags[len(test.flags)-1].Value || len(batch.GetRequests()) != 1 {
					t.Fatalf("apply batch = %#v", batch)
				}
				test.check(t, batch.GetRequests()[0].GetApply())
				return sdk.NewResponseStream(mutationProtoResponse(t, &servicepb.ApplyResponse{})), nil
			})

			handled, err := executeV3AccountMutations(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3AccountMutations() = %v, %v", handled, err)
			}
			assertEmptyMutationResult(t, host, test.operation)
		})
	}
}

func TestExecuteV3AccountMutationsRejectsMalformedMetadataBeforeRequest(t *testing.T) {
	command, request, decoded := decodedMutation(t, "ledger.v3.accounts.set-metadata", []string{"main", "users:42"}, []sdk.FlagOccurrence{{Name: flagMetadata, Value: "missing-separator"}})
	host := sdk.NewMemoryHost(nil)

	handled, err := executeV3AccountMutations(context.Background(), request, decoded, command, host)
	if !handled || err == nil || !strings.Contains(err.Error(), "metadata expects key=value") {
		t.Fatalf("executeV3AccountMutations() = %v, %v", handled, err)
	}
	if len(host.Requests()) != 0 || len(host.Events()) != 0 {
		t.Fatalf("host side effects = requests %#v, events %#v", host.Requests(), host.Events())
	}
}

func TestExecuteV3AccountTypesAddMapsParsedFieldsAndEmitsEmptyResult(t *testing.T) {
	flags := []sdk.FlagOccurrence{
		{Name: flagPattern, Value: "users:{id}:{sequence}:{blob}:{code}"},
		{Name: flagPersistence, Value: "transient"},
		{Name: flagSegmentType, Value: "id=uuid"},
		{Name: flagSegmentType, Value: "sequence=uint64"},
		{Name: flagSegmentType, Value: "blob=bytes"},
		{Name: flagSegmentType, Value: "code=regex:[A-Z]+"},
		{Name: flagIdempotencyKey, Value: "type-1"},
	}
	command, request, decoded := decodedMutation(t, "ledger.v3.account-types.add", []string{"main", "user"}, flags)
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		batch := decodeApplyBatch(t, got, opApplyAddAccountType.id)
		if batch.GetIdempotencyKey() != "type-1" || len(batch.GetRequests()) != 1 {
			t.Fatalf("apply batch = %#v", batch)
		}
		payload := batch.GetRequests()[0].GetAddAccountType()
		accountType := payload.GetAccountType()
		if payload.GetLedger() != "main" || accountType.GetName() != "user" || accountType.GetPattern() != "users:{id}:{sequence}:{blob}:{code}" || accountType.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			t.Fatalf("add account type request = %#v", payload)
		}
		segments := accountType.GetSegmentTypes()
		if segments["id"].GetUuid() == nil || segments["sequence"].GetUint64() == nil || segments["blob"].GetBytes() == nil || segments["code"].GetRegex() != "[A-Z]+" {
			t.Fatalf("segment types = %#v", segments)
		}
		return sdk.NewResponseStream(mutationProtoResponse(t, &servicepb.ApplyResponse{})), nil
	})

	handled, err := executeV3AccountTypes(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3AccountTypes() = %v, %v", handled, err)
	}
	assertEmptyMutationResult(t, host, opApplyAddAccountType.id)
}

func TestExecuteV3AccountTypesMapsRemoveAndEnforcementRequests(t *testing.T) {
	tests := []struct {
		name      string
		commandID string
		arguments []string
		flags     []sdk.FlagOccurrence
		operation string
		check     func(*testing.T, *servicepb.Request)
	}{
		{
			name: "remove", commandID: "ledger.v3.account-types.remove", arguments: []string{"main", "user"},
			flags: []sdk.FlagOccurrence{{Name: flagIdempotencyKey, Value: "remove-1"}}, operation: opApplyRemoveAccountType.id,
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetRemoveAccountType()
				if payload.GetLedger() != "main" || payload.GetName() != "user" {
					t.Fatalf("remove account type request = %#v", payload)
				}
			},
		},
		{
			name: "strict enforcement", commandID: "ledger.v3.account-types.set-default-enforcement", arguments: []string{"main"},
			flags: []sdk.FlagOccurrence{{Name: flagEnforcementMode, Value: "strict"}, {Name: flagIdempotencyKey, Value: "enforcement-1"}}, operation: opApplyDefaultEnforcement.id,
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetSetDefaultEnforcementMode()
				if payload.GetLedger() != "main" || payload.GetEnforcementMode() != commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
					t.Fatalf("set enforcement request = %#v", payload)
				}
			},
		},
		{
			name: "audit enforcement", commandID: "ledger.v3.account-types.set-default-enforcement", arguments: []string{"main"},
			flags: []sdk.FlagOccurrence{{Name: flagEnforcementMode, Value: "audit"}, {Name: flagIdempotencyKey, Value: "enforcement-2"}}, operation: opApplyDefaultEnforcement.id,
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetSetDefaultEnforcementMode()
				if payload.GetLedger() != "main" || payload.GetEnforcementMode() != commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT {
					t.Fatalf("set enforcement request = %#v", payload)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, request, decoded := decodedMutation(t, test.commandID, test.arguments, test.flags)
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				batch := decodeApplyBatch(t, got, test.operation)
				if len(batch.GetRequests()) != 1 {
					t.Fatalf("apply batch = %#v", batch)
				}
				test.check(t, batch.GetRequests()[0])
				return sdk.NewResponseStream(mutationProtoResponse(t, &servicepb.ApplyResponse{})), nil
			})

			handled, err := executeV3AccountTypes(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3AccountTypes() = %v, %v", handled, err)
			}
			assertEmptyMutationResult(t, host, test.operation)
		})
	}
}

func TestExecuteV3AccountTypesGetMapsReadOptionsAndEmitsObject(t *testing.T) {
	command, request, decoded := decodedMutation(t, "ledger.v3.account-types.get", []string{"main", "user"}, []sdk.FlagOccurrence{
		{Name: flagCheckpointID, Value: "41"},
	})
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		var wire servicepb.GetLedgerRequest
		if got.Operation != opGetLedger.id || got.GRPC == nil || got.GRPC.FullMethod != bucketFullMethod("GetLedger") || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		if wire.GetLedger() != "main" || wire.GetRead().GetCheckpointId() != 41 {
			t.Fatalf("get ledger request = %#v", &wire)
		}
		ledger := &commonpb.LedgerInfo{AccountTypes: map[string]*commonpb.AccountType{
			"user": {Name: "user", Pattern: "users:{id}"},
		}}
		return sdk.NewResponseStream(mutationProtoResponse(t, ledger)), nil
	})

	handled, err := executeV3AccountTypes(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3AccountTypes() = %v, %v", handled, err)
	}
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != opGetLedger.id || events[0].Result.Shape != sdk.ResultObject || !equalJSON(events[0].Result.Data, `{"name":"user","pattern":"users:{id}"}`) {
		t.Fatalf("events = %#v", events)
	}
}

func TestExecuteV3AccountTypesListEmitsDeterministicNameOrder(t *testing.T) {
	command, request, decoded := decodedMutation(t, "ledger.v3.account-types.list", []string{"main"}, nil)
	host := sdk.NewMemoryHost(func(_ context.Context, _ sdk.Request) (sdk.Responses, error) {
		ledger := &commonpb.LedgerInfo{AccountTypes: map[string]*commonpb.AccountType{
			"z-last":   {Name: "z-last"},
			"a-first":  {Name: "a-first"},
			"m-middle": {Name: "m-middle"},
		}}
		return sdk.NewResponseStream(mutationProtoResponse(t, ledger)), nil
	})

	handled, err := executeV3AccountTypes(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3AccountTypes() = %v, %v", handled, err)
	}
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != opGetLedger.id || events[0].Result.Shape != sdk.ResultCollection {
		t.Fatalf("events = %#v", events)
	}
	var items []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(events[0].Result.Data, &items); err != nil {
		t.Fatalf("result JSON = %s, error = %v", events[0].Result.Data, err)
	}
	if len(items) != 3 || items[0].Name != "a-first" || items[1].Name != "m-middle" || items[2].Name != "z-last" {
		t.Fatalf("ordered account types = %#v", items)
	}
}

func TestExecuteV3AccountTypesGetReportsMissingType(t *testing.T) {
	command, request, decoded := decodedMutation(t, "ledger.v3.account-types.get", []string{"main", "absent"}, nil)
	host := sdk.NewMemoryHost(func(_ context.Context, _ sdk.Request) (sdk.Responses, error) {
		return sdk.NewResponseStream(mutationProtoResponse(t, &commonpb.LedgerInfo{})), nil
	})

	handled, err := executeV3AccountTypes(context.Background(), request, decoded, command, host)
	if !handled || err == nil || !strings.Contains(err.Error(), "account type not found") {
		t.Fatalf("executeV3AccountTypes() = %v, %v", handled, err)
	}
	if len(host.Events()) != 0 {
		t.Fatalf("events = %#v", host.Events())
	}
}

func TestParseSegmentTypesAcceptsEveryConstraint(t *testing.T) {
	segments, err := parseSegmentTypes([]string{"code=regex:[A-Z]+", "id=uuid", "sequence=uint64", "payload=bytes"})
	if err != nil {
		t.Fatalf("parseSegmentTypes() error = %v", err)
	}
	if len(segments) != 4 || segments["code"].GetRegex() != "[A-Z]+" || segments["id"].GetUuid() == nil || segments["sequence"].GetUint64() == nil || segments["payload"].GetBytes() == nil {
		t.Fatalf("parseSegmentTypes() = %#v", segments)
	}
}

func TestParseSegmentTypesRejectsMalformedConstraints(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{name: "missing separator", values: []string{"id"}, want: "expects variable=constraint"},
		{name: "empty variable", values: []string{"=uuid"}, want: "expects variable=constraint"},
		{name: "empty constraint", values: []string{"id="}, want: "expects variable=constraint"},
		{name: "repeated variable", values: []string{"id=uuid", "id=bytes"}, want: "is repeated"},
		{name: "empty regex", values: []string{"id=regex:"}, want: "invalid segment type"},
		{name: "unknown constraint", values: []string{"id=integer"}, want: "invalid segment type"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			segments, err := parseSegmentTypes(test.values)
			if err == nil || !strings.Contains(err.Error(), test.want) || segments != nil {
				t.Fatalf("parseSegmentTypes() = %#v, %v", segments, err)
			}
		})
	}
}

func decodedMutation(t *testing.T, commandID string, arguments []string, flags []sdk.FlagOccurrence) (sdk.Command, sdk.ExecuteRequest, input) {
	t.Helper()
	command, ok := commandByID(commandID)
	if !ok {
		t.Fatalf("missing command %q", commandID)
	}
	request := sdk.ExecuteRequest{CommandID: commandID, Arguments: arguments, Flags: flags}
	decoded, err := decode(command, request)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	return command, request, decoded
}

func decodeApplyBatch(t *testing.T, request sdk.Request, operation string) *servicepb.ApplyBatch {
	t.Helper()
	var wire servicepb.ApplyRequest
	if request.Operation != operation || request.GRPC == nil || request.GRPC.FullMethod != bucketFullMethod("Apply") || proto.Unmarshal(request.GRPC.Message, &wire) != nil {
		t.Fatalf("host request = %#v", request)
	}
	if wire.GetUnsigned() == nil {
		t.Fatalf("apply request = %#v", &wire)
	}
	return wire.GetUnsigned()
}

func mutationProtoResponse(t *testing.T, message proto.Message) sdk.Response {
	t.Helper()
	body, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	return sdk.Response{Body: body}
}

func assertEmptyMutationResult(t *testing.T, host *sdk.MemoryHost, operation string) {
	t.Helper()
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != operation || events[0].Result.Shape != sdk.ResultEmpty || !equalJSON(events[0].Result.Data, `{}`) {
		t.Fatalf("events = %#v", events)
	}
}

package ledgerv3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	ledgerconfig "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3/internal/ledgerconfig"
	"google.golang.org/protobuf/proto"
)

type ledgerInputHost struct {
	*sdk.MemoryHost
	content []byte
	reads   int
}

type mirrorInputHost struct {
	*sdk.MemoryHost
	content []byte
	reads   int
}

func (h *mirrorInputHost) ReadInput(_ context.Context, handle string) (sdk.InputArtifactChunk, error) {
	if handle != "rules-handle" || h.reads != 0 {
		return sdk.InputArtifactChunk{}, errors.New("unexpected mirror rewrite input read")
	}
	h.reads++
	return sdk.InputArtifactChunk{Bytes: append([]byte(nil), h.content...), Final: true}, nil
}

func (h *ledgerInputHost) ReadInput(_ context.Context, handle string) (sdk.InputArtifactChunk, error) {
	if handle != "configuration-handle" || h.reads != 0 {
		return sdk.InputArtifactChunk{}, errors.New("unexpected test input read")
	}
	h.reads++
	return sdk.InputArtifactChunk{Bytes: append([]byte(nil), h.content...), Final: true}, nil
}

func TestExecuteV3LedgersCreateAndDeleteEmitTheAppliedLedger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		commandID   string
		operationID string
		arguments   []string
		flags       []sdk.FlagOccurrence
		response    *servicepb.ApplyResponse
		check       func(*testing.T, *servicepb.ApplyBatch)
		want        string
	}{
		{
			name:        "create",
			commandID:   "ledger.v3.ledgers.create",
			operationID: opApplyCreateLedger.id,
			arguments:   []string{"main"},
			flags: []sdk.FlagOccurrence{
				{Name: flagMetadataType, Value: "account:category:string"},
				{Name: flagMetadataType, Value: "transaction:priority:int64"},
				{Name: flagEnforcementMode, Value: "audit"},
				{Name: flagIdempotencyKey, Value: "create-1"},
			},
			response: ledgerApplyResponse(&commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "main", Id: 7, DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			}}}),
			check: func(t *testing.T, batch *servicepb.ApplyBatch) {
				t.Helper()
				if batch.GetIdempotencyKey() != "create-1" || len(batch.GetRequests()) != 1 {
					t.Fatalf("apply batch = %#v", batch)
				}
				create := batch.GetRequests()[0].GetCreateLedger()
				wantSchema := []*commonpb.SetMetadataFieldTypeCommand{
					{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "category", Type: commonpb.MetadataType_METADATA_TYPE_STRING},
					{TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "priority", Type: commonpb.MetadataType_METADATA_TYPE_INT64},
				}
				if create.GetName() != "main" || create.GetDefaultEnforcementMode() != commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT || !reflect.DeepEqual(create.GetInitialSchema(), wantSchema) {
					t.Fatalf("create request = %#v", create)
				}
			},
			want: `{"name":"main","metadataSchema":{},"mirrorSource":{},"mirrorSyncProgress":{},"defaultEnforcementMode":"CHART_ENFORCEMENT_AUDIT"}`,
		},
		{
			name:        "delete",
			commandID:   "ledger.v3.ledgers.delete",
			operationID: opApplyDeleteLedger.id,
			arguments:   []string{"obsolete"},
			flags:       []sdk.FlagOccurrence{{Name: flagIdempotencyKey, Value: "delete-1"}},
			response: ledgerApplyResponse(&commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{
				Name: "obsolete",
			}}}),
			check: func(t *testing.T, batch *servicepb.ApplyBatch) {
				t.Helper()
				if batch.GetIdempotencyKey() != "delete-1" || len(batch.GetRequests()) != 1 || batch.GetRequests()[0].GetDeleteLedger().GetName() != "obsolete" {
					t.Fatalf("apply batch = %#v", batch)
				}
			},
			want: `{"name":"obsolete"}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command, decoded, request := decodedLedgerCommand(t, test.commandID, test.arguments, test.flags)
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				if got.Operation != test.operationID || got.GRPC == nil || got.GRPC.FullMethod != bucketFullMethod("Apply") {
					t.Fatalf("host request = %#v", got)
				}
				var batch servicepb.ApplyRequest
				if err := proto.Unmarshal(got.GRPC.Message, &batch); err != nil {
					t.Fatal(err)
				}
				test.check(t, batch.GetUnsigned())
				return sdk.NewResponseStream(protoResponse(t, test.response)), nil
			})

			handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
			}
			assertLedgerResult(t, host, test.operationID, sdk.ResultObject, test.want)
		})
	}
}

func TestExecuteV3LedgersCreateMapsHTTPMirrorSource(t *testing.T) {
	command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.create", []string{"mirror"}, []sdk.FlagOccurrence{
		{Name: flagMode, Value: "mirror"},
		{Name: flagMirrorSourceType, Value: "http"},
		{Name: flagMirrorLedgerName, Value: "legacy"},
		{Name: flagMirrorBaseURL, Value: "https://ledger-v2.example"},
		{Name: flagMirrorOAuth2ClientID, Value: "client"},
		{Name: flagMirrorOAuth2ClientSecret, Value: "secret"},
		{Name: flagMirrorOAuth2TokenEndpoint, Value: "https://issuer.example/token"},
		{Name: flagMirrorOAuth2Scopes, Value: "ledger:read"},
		{Name: flagMirrorBatchSize, Value: "4294967295"},
		{Name: flagMirrorRewriteRule, Value: `{"anyVariant":{"actions":[{"drop":{}}]}}`},
	})
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		var wire servicepb.ApplyRequest
		if got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		create := wire.GetUnsigned().GetRequests()[0].GetCreateLedger()
		wantSource := &commonpb.MirrorSourceConfig{
			LedgerName: "legacy", BatchSize: ^uint32(0),
			Type:         &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://ledger-v2.example", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientId: "client", ClientSecret: "secret", TokenEndpoint: "https://issuer.example/token", Scopes: []string{"ledger:read"}}}},
			RewriteRules: []*commonpb.MirrorRewriteRule{{Scope: &commonpb.MirrorRewriteRule_AnyVariant{AnyVariant: &commonpb.AnyVariantRule{Actions: []*commonpb.AnyVariantAction{{Action: &commonpb.AnyVariantAction_Drop{Drop: &commonpb.DropAction{}}}}}}}},
		}
		if create.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR || !proto.Equal(create.GetMirrorSource(), wantSource) {
			t.Fatalf("create mirror request = %#v", create)
		}
		return sdk.NewResponseStream(protoResponse(t, ledgerApplyResponse(&commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "mirror"}}}))), nil
	})
	if handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host); err != nil || !handled {
		t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
	}
}

func TestExecuteV3LedgersCreateReadsMirrorRewriteFile(t *testing.T) {
	command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.create", []string{"mirror"}, []sdk.FlagOccurrence{
		{Name: flagMode, Value: "mirror"},
		{Name: flagMirrorBaseURL, Value: "https://ledger-v2.example"},
		{Name: flagMirrorRewriteFile, Value: "rules-handle"},
	})
	memory := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		var wire servicepb.ApplyRequest
		if got.GRPC == nil || proto.Unmarshal(got.GRPC.Message, &wire) != nil {
			t.Fatalf("host request = %#v", got)
		}
		rules := wire.GetUnsigned().GetRequests()[0].GetCreateLedger().GetMirrorSource().GetRewriteRules()
		if len(rules) != 1 || rules[0].GetAnyVariant() == nil || len(rules[0].GetAnyVariant().GetActions()) != 1 || rules[0].GetAnyVariant().GetActions()[0].GetDrop() == nil {
			t.Fatalf("rewrite rules = %#v", rules)
		}
		return sdk.NewResponseStream(protoResponse(t, ledgerApplyResponse(&commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "mirror"}}}))), nil
	})
	host := &mirrorInputHost{MemoryHost: memory, content: []byte(`[{"anyVariant":{"actions":[{"drop":{}}]}}]`)}
	if handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host); err != nil || !handled {
		t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
	}
	if host.reads != 1 {
		t.Fatalf("rewrite artifact reads = %d", host.reads)
	}
}

func TestExecuteV3LedgersCreateRejectsExplicitEmptyAWSIAMFlags(t *testing.T) {
	t.Parallel()

	for _, flagName := range []string{flagMirrorAWSRegion, flagMirrorAWSRoleARN} {
		flagName := flagName
		t.Run(flagName, func(t *testing.T) {
			t.Parallel()
			command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.create", []string{"mirror"}, []sdk.FlagOccurrence{
				{Name: flagMode, Value: "mirror"},
				{Name: flagMirrorSourceType, Value: "postgres"},
				{Name: flagMirrorDSN, Value: "postgres://ledger.example/db"},
				{Name: flagName, Value: ""},
			})
			host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				t.Fatal("explicit empty IAM input must fail before a host request")
				return nil, nil
			})
			handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host)
			if !handled || err == nil {
				t.Fatalf("executeV3Ledgers() = (%v, %v), want handled invalid argument", handled, err)
			}
			if failureCode(t, err) != sdk.FailureInvalidArgument || !strings.Contains(err.Error(), flagName) {
				t.Fatalf("error = %v, want invalid_argument naming %q", err, flagName)
			}
		})
	}
}

func TestExecuteV3LedgerMetadataAndSchemaMutationsMapTypedApplyRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, commandID, operationID string
		arguments                    []string
		flags                        []sdk.FlagOccurrence
		check                        func(*testing.T, *servicepb.Request)
	}{
		{
			name: "delete metadata", commandID: "ledger.v3.ledgers.delete-metadata", operationID: opApplyDeleteLedgerMetadata.id,
			arguments: []string{"main", "owner"},
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetDeleteLedgerMetadata()
				if payload.GetLedger() != "main" || payload.GetKey() != "owner" {
					t.Fatalf("delete metadata request = %#v", payload)
				}
			},
		},
		{
			name: "set metadata", commandID: "ledger.v3.ledgers.set-metadata", operationID: opApplySaveLedgerMetadata.id,
			arguments: []string{"main"}, flags: []sdk.FlagOccurrence{{Name: flagMetadata, Value: "owner=treasury"}, {Name: flagMetadata, Value: "region=eu"}},
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetSaveLedgerMetadata()
				if payload.GetLedger() != "main" || payload.GetMetadata()["owner"].GetStringValue() != "treasury" || payload.GetMetadata()["region"].GetStringValue() != "eu" {
					t.Fatalf("save metadata request = %#v", payload)
				}
			},
		},
		{
			name: "remove metadata type", commandID: "ledger.v3.ledgers.remove-metadata-type", operationID: opApplyRemoveMetadataType.id,
			arguments: []string{"main", "category"}, flags: []sdk.FlagOccurrence{{Name: flagTargetType, Value: "transaction"}},
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetRemoveMetadataFieldType()
				if payload.GetLedger() != "main" || payload.GetTargetType() != commonpb.TargetType_TARGET_TYPE_TRANSACTION || payload.GetKey() != "category" {
					t.Fatalf("remove metadata type request = %#v", payload)
				}
			},
		},
		{
			name: "set metadata type", commandID: "ledger.v3.ledgers.set-metadata-type", operationID: opApplySetMetadataFieldType.id,
			arguments: []string{"main", "verified"}, flags: []sdk.FlagOccurrence{{Name: flagTargetType, Value: "ledger"}, {Name: flagMetadataType, Value: "bool"}},
			check: func(t *testing.T, request *servicepb.Request) {
				t.Helper()
				payload := request.GetSetMetadataFieldType()
				if payload.GetLedger() != "main" || payload.GetTargetType() != commonpb.TargetType_TARGET_TYPE_LEDGER || payload.GetKey() != "verified" || payload.GetType() != commonpb.MetadataType_METADATA_TYPE_BOOL {
					t.Fatalf("set metadata type request = %#v", payload)
				}
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			flags := append(append([]sdk.FlagOccurrence(nil), test.flags...), sdk.FlagOccurrence{Name: flagIdempotencyKey, Value: "mutation-1"})
			command, decoded, executeRequest := decodedLedgerCommand(t, test.commandID, test.arguments, flags)
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				if got.Operation != test.operationID {
					t.Fatalf("operation = %q, want %q", got.Operation, test.operationID)
				}
				var wire servicepb.ApplyRequest
				if err := proto.Unmarshal(got.GRPC.Message, &wire); err != nil {
					t.Fatal(err)
				}
				batch := wire.GetUnsigned()
				if batch.GetIdempotencyKey() != "mutation-1" || len(batch.GetRequests()) != 1 {
					t.Fatalf("apply batch = %#v", batch)
				}
				test.check(t, batch.GetRequests()[0])
				return sdk.NewResponseStream(protoResponse(t, &servicepb.ApplyResponse{})), nil
			})

			handled, err := executeV3Ledgers(context.Background(), executeRequest, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
			}
			assertLedgerResult(t, host, test.operationID, sdk.ResultEmpty, `{}`)
		})
	}
}

func TestParseInitialSchemaMapsEntriesAndRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	got, err := parseInitialSchema([]string{"account:segment:string", "transaction:attempts:uint32", "ledger:opened-at:datetime"})
	if err != nil {
		t.Fatalf("parseInitialSchema() error = %v", err)
	}
	want := []*commonpb.SetMetadataFieldTypeCommand{
		{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "segment", Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		{TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "attempts", Type: commonpb.MetadataType_METADATA_TYPE_UINT32},
		{TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER, Key: "opened-at", Type: commonpb.MetadataType_METADATA_TYPE_DATETIME},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseInitialSchema() = %#v, want %#v", got, want)
	}

	for _, value := range []string{"account::string", "account:key", "unknown:key:string", "account:key:unknown"} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			if _, err := parseInitialSchema([]string{value}); err == nil {
				t.Fatalf("parseInitialSchema(%q) error = nil", value)
			}
		})
	}
}

func TestExecuteV3LedgerConfigurationAggregatesAllReadSurfaces(t *testing.T) {
	t.Parallel()

	command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.configuration", []string{"main"}, nil)
	numscriptCursors := make([]string, 0, 2)
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		switch got.Operation {
		case opGetLedger.id:
			assertProtoRequest(t, got, &servicepb.GetLedgerRequest{Ledger: "main"})
			return sdk.NewResponseStream(protoResponse(t, &commonpb.LedgerInfo{
				Name: "main", DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
				MetadataSchema: &commonpb.MetadataSchema{TransactionFields: map[string]*commonpb.MetadataFieldSchema{
					"category": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
				}},
			})), nil
		case opListIndexes.id:
			assertProtoRequest(t, got, &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: "main"})
			return sdk.NewResponseStream(protoResponse(t, &commonpb.Index{Ledger: "main", Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "category"}}}})), nil
		case opListPreparedQueries.id:
			assertProtoRequest(t, got, &servicepb.ListPreparedQueriesRequest{Ledger: "main"})
			return sdk.NewResponseStream(protoResponse(t, &servicepb.ListPreparedQueriesResponse{Queries: []*commonpb.PreparedQuery{{Name: "recent", Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS}}})), nil
		case opListNumscripts.id:
			var wire servicepb.ListNumscriptsRequest
			if err := proto.Unmarshal(got.GRPC.Message, &wire); err != nil {
				t.Fatal(err)
			}
			if wire.GetLedger() != "main" || wire.GetOptions().GetPageSize() != 100 {
				t.Fatalf("list numscripts request = %#v", &wire)
			}
			numscriptCursors = append(numscriptCursors, wire.GetOptions().GetCursor())
			if len(numscriptCursors) == 1 {
				return withContinuation(sdk.NewResponseStream(protoResponse(t, &commonpb.NumscriptInfo{Name: "pay", Content: "send ...", Version: "1.0.0"})), "next-page"), nil
			}
			return sdk.NewResponseStream(protoResponse(t, &commonpb.NumscriptInfo{Name: "refund", Content: "send refund ...", Version: "2.0.0"})), nil
		default:
			t.Fatalf("unexpected operation %q", got.Operation)
			return nil, nil
		}
	})

	handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
	}
	if !reflect.DeepEqual(numscriptCursors, []string{"", "next-page"}) {
		t.Fatalf("numscript cursors = %q", numscriptCursors)
	}
	assertLedgerResult(t, host, opGetLedger.id, sdk.ResultObject, `{
		"defaultEnforcementMode":"chart_enforcement_audit",
		"metadataSchema":{"transaction":{"category":{"type":"string","indexed":true}}},
		"indexes":{"reference":false,"timestamp":false,"address":false,"sourceAddress":false,"destinationAddress":false,"insertedAt":false,"revertedAt":false},
		"preparedQueries":{"recent":{"target":"transactions"}},
		"numscripts":{"pay":{"content":"send ...","version":"1.0.0"},"refund":{"content":"send refund ...","version":"2.0.0"}}
	}`)
}

func TestExecuteV3LedgerConfigurationEnforcesTheExactJSONBudget(t *testing.T) {
	_, _, request := decodedLedgerCommand(t, "ledger.v3.ledgers.configuration", []string{"main"}, nil)
	request.Target = sdk.TargetSelection{OrganizationID: "org", StackID: "stack"}
	request.ServiceVersions = []sdk.ServiceVersion{{Service: sdk.ServiceLedger, Version: "3.0.0", Major: 3}}

	for _, test := range []struct {
		name       string
		size       uint64
		wantBudget bool
	}{
		{name: "exact limit", size: sdk.DefaultAllPagesMaxBytes},
		{name: "one byte over", size: sdk.DefaultAllPagesMaxBytes + 1, wantBudget: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger := &commonpb.LedgerInfo{Name: "main"}
			numscripts := numscriptsForConfigurationJSONSize(t, ledger, test.size)
			host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				switch got.Operation {
				case opGetLedger.id:
					return sdk.NewResponseStream(protoResponse(t, ledger)), nil
				case opListIndexes.id:
					return sdk.NewResponseStream(), nil
				case opListPreparedQueries.id:
					return sdk.NewResponseStream(protoResponse(t, &servicepb.ListPreparedQueriesResponse{})), nil
				case opListNumscripts.id:
					responses := make([]sdk.Response, 0, len(numscripts))
					for _, numscript := range numscripts {
						responses = append(responses, protoResponse(t, numscript))
					}
					return sdk.NewResponseStream(responses...), nil
				default:
					t.Fatalf("unexpected operation %q", got.Operation)
					return nil, nil
				}
			})

			err := (Plugin{}).Execute(context.Background(), request, host)
			if test.wantBudget {
				var failure sdk.Failure
				if !errors.As(err, &failure) || failure.Code != string(sdk.FailureBudgetExhausted) {
					t.Fatalf("Plugin.Execute() error = %v, want %s", err, sdk.FailureBudgetExhausted)
				}
				if events := host.Events(); len(events) != 0 {
					t.Fatalf("Plugin.Execute() emitted %d partial events", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("Plugin.Execute() error = %v", err)
			}
			events := host.Events()
			if len(events) != 1 || events[0].Result == nil || uint64(len(events[0].Result.Data)) != test.size {
				t.Fatalf("result = %#v, want one %d-byte JSON document", events, test.size)
			}
		})
	}
}

func TestV3NumscriptJSONBudgetAcceptsTheLimitAndRejectsTheNextByte(t *testing.T) {
	base := newV3NumscriptJSONBudget()
	if err := base.add(&commonpb.NumscriptInfo{Name: "script"}); err != nil {
		t.Fatal(err)
	}
	contentBytes := sdk.DefaultAllPagesMaxBytes - base.bytes

	exact := newV3NumscriptJSONBudget()
	if err := exact.add(&commonpb.NumscriptInfo{Name: "script", Content: strings.Repeat("x", int(contentBytes))}); err != nil {
		t.Fatalf("exact limit: %v", err)
	}
	if exact.bytes != sdk.DefaultAllPagesMaxBytes {
		t.Fatalf("budget bytes = %d, want %d", exact.bytes, sdk.DefaultAllPagesMaxBytes)
	}

	over := newV3NumscriptJSONBudget()
	err := over.add(&commonpb.NumscriptInfo{Name: "script", Content: strings.Repeat("x", int(contentBytes+1))})
	var failure sdk.Failure
	if !errors.As(err, &failure) || failure.Code != string(sdk.FailureBudgetExhausted) {
		t.Fatalf("next byte error = %v, want %s", err, sdk.FailureBudgetExhausted)
	}
}

func numscriptsForConfigurationJSONSize(t *testing.T, ledger *commonpb.LedgerInfo, target uint64) []*commonpb.NumscriptInfo {
	t.Helper()

	const count = 20
	numscripts := make([]*commonpb.NumscriptInfo, count)
	for index := range numscripts {
		numscripts[index] = &commonpb.NumscriptInfo{Name: fmt.Sprintf("script-%02d", index)}
	}
	base, err := json.Marshal(ledgerconfig.ConfigFromProto(ledger, nil, nil, numscripts))
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(base)) > target {
		t.Fatalf("empty configuration is %d bytes, target is %d", len(base), target)
	}
	remaining := target - uint64(len(base))
	perItem, remainder := remaining/count, remaining%count
	for index := range numscripts {
		size := perItem
		if uint64(index) < remainder {
			size++
		}
		numscripts[index].Content = strings.Repeat("x", int(size))
	}
	encoded, err := json.Marshal(ledgerconfig.ConfigFromProto(ledger, nil, nil, numscripts))
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(encoded)) != target {
		t.Fatalf("configuration JSON is %d bytes, want %d", len(encoded), target)
	}
	return numscripts
}

func TestExecuteV3LedgerConfigurationApplyDistinguishesNoOpFromChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		content    string
		wantApply  bool
		wantShape  sdk.ResultShape
		wantResult string
	}{
		{name: "no-op", content: `{"defaultEnforcementMode":"chart_enforcement_strict","indexes":{"reference":false,"timestamp":false,"address":false,"sourceAddress":false,"destinationAddress":false,"insertedAt":false,"revertedAt":false}}`, wantShape: sdk.ResultEmpty, wantResult: `{}`},
		{name: "apply", content: "defaultEnforcementMode: audit\nindexes:\n  reference: false\n  timestamp: false\n  address: false\n  sourceAddress: false\n  destinationAddress: false\n  insertedAt: false\n  revertedAt: false\n", wantApply: true, wantShape: sdk.ResultObject, wantResult: `{}`},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.configuration.apply", []string{"main"}, []sdk.FlagOccurrence{{Name: flagConfiguration, Value: "configuration-handle"}, {Name: flagIdempotencyKey, Value: "config-1"}})
			applyCalls := 0
			memory := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
				switch got.Operation {
				case opGetLedger.id:
					return sdk.NewResponseStream(protoResponse(t, &commonpb.LedgerInfo{Name: "main", DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT})), nil
				case opListIndexes.id:
					return sdk.NewResponseStream(), nil
				case opListPreparedQueries.id:
					return sdk.NewResponseStream(protoResponse(t, &servicepb.ListPreparedQueriesResponse{})), nil
				case opListNumscripts.id:
					return sdk.NewResponseStream(), nil
				case opApplyConfiguration.id:
					applyCalls++
					var wire servicepb.ApplyRequest
					if err := proto.Unmarshal(got.GRPC.Message, &wire); err != nil {
						t.Fatal(err)
					}
					batch := wire.GetUnsigned()
					if batch.GetIdempotencyKey() != "config-1" || len(batch.GetRequests()) != 1 {
						t.Fatalf("configuration apply batch = %#v", batch)
					}
					mode := batch.GetRequests()[0].GetSetDefaultEnforcementMode()
					if mode.GetLedger() != "main" || mode.GetEnforcementMode() != commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT {
						t.Fatalf("configuration apply request = %#v", mode)
					}
					return sdk.NewResponseStream(protoResponse(t, &servicepb.ApplyResponse{})), nil
				default:
					t.Fatalf("unexpected operation %q", got.Operation)
					return nil, nil
				}
			})
			host := &ledgerInputHost{MemoryHost: memory, content: []byte(test.content)}

			handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host)
			if err != nil || !handled {
				t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
			}
			if host.reads != 1 || (applyCalls == 1) != test.wantApply {
				t.Fatalf("artifact reads = %d, apply calls = %d", host.reads, applyCalls)
			}
			assertLedgerResult(t, memory, opApplyConfiguration.id, test.wantShape, test.wantResult)
		})
	}
}

func TestExecuteV3LedgerConfigurationDryRunEmitsPlanWithoutMutation(t *testing.T) {
	command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.configuration.apply", []string{"main"}, []sdk.FlagOccurrence{
		{Name: flagConfiguration, Value: "configuration-handle"},
		{Name: flagDryRun, Value: "true"},
	})
	applyCalls := 0
	memory := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		switch got.Operation {
		case opGetLedger.id:
			return sdk.NewResponseStream(protoResponse(t, &commonpb.LedgerInfo{Name: "main", DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT})), nil
		case opListIndexes.id, opListNumscripts.id:
			return sdk.NewResponseStream(), nil
		case opListPreparedQueries.id:
			return sdk.NewResponseStream(protoResponse(t, &servicepb.ListPreparedQueriesResponse{})), nil
		case opApplyConfiguration.id:
			applyCalls++
			return sdk.NewResponseStream(protoResponse(t, &servicepb.ApplyResponse{})), nil
		default:
			t.Fatalf("unexpected operation %q", got.Operation)
			return nil, nil
		}
	})
	host := &ledgerInputHost{MemoryHost: memory, content: []byte("defaultEnforcementMode: audit\nindexes: {}\n")}
	if handled, err := executeV3Ledgers(context.Background(), request, decoded, command, host); err != nil || !handled {
		t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
	}
	if applyCalls != 0 {
		t.Fatalf("dry-run made %d Apply calls", applyCalls)
	}
	assertLedgerResult(t, memory, opApplyConfiguration.id, sdk.ResultObject, `{"actions":[{"section":"defaultEnforcementMode","operation":"update"}]}`)
}

func TestExecuteV3LedgersDeclinesAnotherCommandFamily(t *testing.T) {
	t.Parallel()
	handled, err := executeV3Ledgers(context.Background(), sdk.ExecuteRequest{CommandID: "ledger.v3.accounts.get"}, input{}, sdk.Command{}, sdk.NewMemoryHost(nil))
	if err != nil || handled {
		t.Fatalf("executeV3Ledgers() = (%v, %v)", handled, err)
	}
}

func decodedLedgerCommand(t *testing.T, commandID string, arguments []string, flags []sdk.FlagOccurrence) (sdk.Command, input, sdk.ExecuteRequest) {
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
	return command, decoded, request
}

func ledgerApplyResponse(payload *commonpb.LogPayload) *servicepb.ApplyResponse {
	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: payload}}}
}

func assertProtoRequest(t *testing.T, request sdk.Request, want proto.Message) {
	t.Helper()
	if request.GRPC == nil {
		t.Fatalf("request = %#v", request)
	}
	got := want.ProtoReflect().Type().New().Interface()
	if err := proto.Unmarshal(request.GRPC.Message, got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("request = %v, want %v", got, want)
	}
}

func assertLedgerResult(t *testing.T, host *sdk.MemoryHost, operationID string, shape sdk.ResultShape, want string) {
	t.Helper()
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != operationID || events[0].Result.Shape != shape || !equalJSON(events[0].Result.Data, want) {
		var data []byte
		if len(events) == 1 && events[0].Result != nil {
			data = events[0].Result.Data
		}
		t.Fatalf("events = %#v, data = %s", events, data)
	}
}

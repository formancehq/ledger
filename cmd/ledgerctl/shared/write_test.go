package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	ledgerplugin "github.com/formancehq/ledger/misc/fctl-plugin"

	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestWriteTransactionPreservesCanonicalPayloadAndNumbers(t *testing.T) {
	t.Parallel()
	const amount = "340282366920938463463374607431768211455"
	const metadataNumber = uint64(18446744073709551615)
	var applied *servicepb.ApplyResponse
	calls := 0
	executor := &Executor{Apply: func(ctx context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		require.NoError(t, ctx.Err())
		require.Equal(t, "durable-transaction-key", key)
		require.Len(t, requests, 1)
		request := requests[0].GetApply()
		require.Equal(t, "bank", request.GetLedger())
		payload := request.GetAction().GetCreateTransaction()
		require.Len(t, payload.GetPostings(), 1)
		require.Equal(t, amount, payload.GetPostings()[0].GetAmount().Dec())
		require.Equal(t, metadataNumber, payload.GetMetadata()["number"].GetUintValue())
		require.Equal(t, uint64(9007199254740993), payload.GetAccountMetadata()["users:1"].GetValues()["number"].GetUintValue())
		created := &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 9007199254740993, Postings: payload.GetPostings(), Metadata: payload.GetMetadata()}, AccountMetadata: payload.GetAccountMetadata()}
		applied = writeTestLedgerLog(1, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: created}})

		return applied, nil
	}}
	req := writeTestRequest("transactions/create", nil, `{"postings":[{"source":"world","destination":"users:1","amount":`+amount+`,"asset":"USD/2"}],"metadata":{"number":18446744073709551615},"accountMetadata":{"users:1":{"number":9007199254740993}}}`)
	req.Flags["idempotency-key"] = "durable-transaction-key"
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Same(t, applied, executor.LastApplyResponse)
	require.IsType(t, &commonpb.CreatedTransaction{}, executor.Result)
	require.Contains(t, string(response.Data), amount)
	require.Contains(t, string(response.Data), "18446744073709551615")
	require.Contains(t, string(response.Data), "9007199254740993")
	var envelope struct {
		Data struct {
			Transaction json.RawMessage `json:"transaction"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &envelope))
	require.NotEmpty(t, envelope.Data.Transaction)
}

func TestWriteTransactionScriptReferenceUsesCanonicalStringVersion(t *testing.T) {
	t.Parallel()
	calls := 0
	executor := &Executor{Apply: func(_ context.Context, _ string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		require.Len(t, requests, 1)
		payload := requests[0].GetApply().GetAction().GetCreateTransaction()
		require.Empty(t, payload.GetPostings())
		require.Nil(t, payload.GetScript())
		require.Equal(t, "saved-transfer", payload.GetScriptReference().GetName())
		require.Equal(t, "1.2.3", payload.GetScriptReference().GetVersion())
		require.Equal(t, "USD/2 100", payload.GetScriptReference().GetVars()["amount"])

		return writeTestLedgerLog(1, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}}}}), nil
	}}
	req := writeTestRequest("transactions/create", nil, `{"scriptReference":{"name":"saved-transfer","version":"1.2.3","vars":{"amount":"USD/2 100"}}}`)
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.True(t, json.Valid(response.Data))
}

func TestWriteLedgerCreationUsesHTTPDTO(t *testing.T) {
	t.Parallel()
	calls := 0
	executor := &Executor{Apply: func(_ context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		require.Empty(t, key)
		require.Len(t, requests, 1)
		create := requests[0].GetCreateLedger()
		require.Equal(t, "new-ledger", create.GetName())
		require.Equal(t, uint64(9007199254740993), create.GetMetadata()["number"].GetUintValue())
		require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, create.GetDefaultEnforcementMode())
		require.Len(t, create.GetInitialSchema(), 1)
		require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, create.GetInitialSchema()[0].GetTargetType())
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_UINT64, create.GetInitialSchema()[0].GetType())
		require.Equal(t, "number", create.GetInitialSchema()[0].GetKey())
		accountType := create.GetAccountTypes()["users"]
		require.Equal(t, "users", accountType.GetName())
		require.Equal(t, "users:$id", accountType.GetPattern())
		require.Equal(t, commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, accountType.GetPersistence())
		require.NotNil(t, accountType.GetSegmentTypes()["id"].GetUuid())
		created := &commonpb.CreatedLedgerLog{Name: create.GetName(), Metadata: create.GetMetadata(), AccountTypes: create.GetAccountTypes(), DefaultEnforcementMode: create.GetDefaultEnforcementMode()}

		return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: created}}}}}, nil
	}}
	req := writeTestRequest("create", []string{"new-ledger"}, `{"metadata":{"number":9007199254740993},"initialSchema":[{"targetType":"account","key":"number","type":"uint64"}],"defaultEnforcementMode":"AUDIT","accountTypes":{"users":{"name":"users","pattern":"users:$id","persistence":"EPHEMERAL","segmentTypes":{"id":{"type":"uuid"}}}}}`)
	delete(req.Flags, "ledger")
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	info := executor.Result.(*commonpb.LedgerInfo)
	require.Equal(t, uint64(9007199254740993), info.GetMetadata()["number"].GetUintValue())
	require.Contains(t, string(response.Data), "9007199254740993")
}

func TestWriteMirrorCreationUsesNativeDecoders(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		body  string
		check func(*testing.T, *commonpb.MirrorSourceConfig)
	}{
		{"http", `{"ledgerName":"source","baseUrl":"https://example.invalid","oauth2ClientId":"client","oauth2ClientSecret":"fixture-secret","oauth2TokenEndpoint":"https://example.invalid/token","oauth2Scopes":["read"],"batchSize":19}`, func(t *testing.T, cfg *commonpb.MirrorSourceConfig) {
			require.Equal(t, "https://example.invalid", cfg.GetHttp().GetBaseUrl())
			require.Equal(t, "fixture-secret", cfg.GetHttp().GetOauth2ClientCredentials().GetClientSecret())
			require.Equal(t, []string{"read"}, cfg.GetHttp().GetOauth2ClientCredentials().GetScopes())
			require.Equal(t, uint32(19), cfg.GetBatchSize())
		}},
		{"postgres", `{"ledgerName":"source","type":"postgres","dsn":"postgres://example.invalid"}`, func(t *testing.T, cfg *commonpb.MirrorSourceConfig) {
			require.Equal(t, "postgres://example.invalid", cfg.GetPostgres().GetDsn())
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request, err := decodeWriteLedger("mirror", json.RawMessage(`{"mode":"MIRROR","mirrorSource":`+test.body+`}`))
			require.NoError(t, err)
			require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, request.GetMode())
			require.Equal(t, "source", request.GetMirrorSource().GetLedgerName())
			test.check(t, request.GetMirrorSource())
		})
	}
}

func TestWriteNativeLedgerAndIndexesRemainOneProposal(t *testing.T) {
	t.Parallel()
	index, err := indexes.ParseCanonical("log_builtin:LOG_BUILTIN_INDEX_DATE")
	require.NoError(t, err)
	create := &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "bank", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source"}}}}
	addIndex := &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: "bank", Id: index}}}
	calls := 0
	executor := &Executor{CreateLedgerRequests: []*servicepb.Request{create, addIndex}, Apply: func(_ context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		require.Equal(t, "atomic-key", key)
		require.Len(t, requests, 2)
		require.Same(t, create, requests[0])
		require.Same(t, addIndex, requests[1])
		created := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "bank", Mode: requests[0].GetCreateLedger().GetMode(), MirrorSource: requests[0].GetCreateLedger().GetMirrorSource()}}}}
		indexed := writeTestLedgerLog(2, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: index}}}).GetLogs()[0]

		return &servicepb.ApplyResponse{Logs: []*commonpb.Log{created, indexed}}, nil
	}}
	req := writeTestRequest("create", nil, `{}`)
	req.Flags["idempotency-key"] = "atomic-key"
	_, err = ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Len(t, executor.LastApplyResponse.GetLogs(), 2)
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, executor.Result.(*commonpb.LedgerInfo).GetMode())

	executor.CreateLedgerRequests[0] = &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "other-ledger"}}}
	_, err = ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.ErrorContains(t, err, "different ledger")
	require.Equal(t, 1, calls)
}

func TestWriteMetadataTargetsAndRevertDecode(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		path  string
		args  []string
		body  string
		check func(*testing.T, *servicepb.Request)
	}{
		{"metadata/set", nil, `{"number":9007199254740993,"truth":true}`, func(t *testing.T, req *servicepb.Request) {
			require.Equal(t, "bank", req.GetSaveLedgerMetadata().GetLedger())
			require.Equal(t, uint64(9007199254740993), req.GetSaveLedgerMetadata().GetMetadata()["number"].GetUintValue())
			require.True(t, req.GetSaveLedgerMetadata().GetMetadata()["truth"].GetBoolValue())
		}},
		{"metadata/delete", []string{"raw/key"}, "", func(t *testing.T, req *servicepb.Request) {
			require.Equal(t, "raw/key", req.GetDeleteLedgerMetadata().GetKey())
			require.Equal(t, "bank", req.GetDeleteLedgerMetadata().GetLedger())
		}},
		{"accounts/metadata/set", []string{"users:123"}, `{"number":9007199254740993}`, func(t *testing.T, req *servicepb.Request) {
			metadata := req.GetApply().GetAction().GetAddMetadata()
			require.Equal(t, "users:123", metadata.GetTarget().GetAccount().GetAddr())
			require.Equal(t, uint64(9007199254740993), metadata.GetMetadata()["number"].GetUintValue())
		}},
		{"accounts/metadata/delete", []string{"users:123", "raw/key"}, "", func(t *testing.T, req *servicepb.Request) {
			metadata := req.GetApply().GetAction().GetDeleteMetadata()
			require.Equal(t, "users:123", metadata.GetTarget().GetAccount().GetAddr())
			require.Equal(t, "raw/key", metadata.GetKey())
		}},
		{"transactions/metadata/set", []string{"9007199254740993"}, `{"number":9007199254740993}`, func(t *testing.T, req *servicepb.Request) {
			metadata := req.GetApply().GetAction().GetAddMetadata()
			require.Equal(t, uint64(9007199254740993), metadata.GetTarget().GetTransactionId())
			require.Equal(t, uint64(9007199254740993), metadata.GetMetadata()["number"].GetUintValue())
		}},
		{"transactions/metadata/delete", []string{"9007199254740993", "raw/key"}, "", func(t *testing.T, req *servicepb.Request) {
			metadata := req.GetApply().GetAction().GetDeleteMetadata()
			require.Equal(t, uint64(9007199254740993), metadata.GetTarget().GetTransactionId())
			require.Equal(t, "raw/key", metadata.GetKey())
		}},
		{"transactions/revert", []string{"9007199254740993"}, `{"id":999,"force":true,"atEffectiveDate":true,"metadata":{"number":9007199254740993}}`, func(t *testing.T, req *servicepb.Request) {
			payload := req.GetApply().GetAction().GetRevertTransaction()
			require.Equal(t, uint64(9007199254740993), payload.GetTransactionId())
			require.True(t, payload.GetForce())
			require.True(t, payload.GetAtEffectiveDate())
			require.Equal(t, uint64(9007199254740993), payload.GetMetadata()["number"].GetUintValue())
		}},
		{"transactions/revert", []string{"1"}, "", func(t *testing.T, req *servicepb.Request) {
			require.Equal(t, uint64(1), req.GetApply().GetAction().GetRevertTransaction().GetTransactionId())
		}},
		{"delete", []string{"bank"}, "", func(t *testing.T, req *servicepb.Request) { require.Equal(t, "bank", req.GetDeleteLedger().GetName()) }},
		{"indexes/create", nil, `{"id":"log_builtin:LOG_BUILTIN_INDEX_DATE"}`, func(t *testing.T, req *servicepb.Request) {
			require.Equal(t, "log_builtin:LOG_BUILTIN_INDEX_DATE", indexes.Canonical(req.GetCreateIndex().GetId()))
			require.Equal(t, "bank", req.GetCreateIndex().GetLedger())
		}},
		{"indexes/delete", []string{"log_builtin:LOG_BUILTIN_INDEX_DATE"}, "", func(t *testing.T, req *servicepb.Request) {
			require.Equal(t, "log_builtin:LOG_BUILTIN_INDEX_DATE", indexes.Canonical(req.GetDropIndex().GetId()))
			require.Equal(t, "bank", req.GetDropIndex().GetLedger())
		}},
	} {
		t.Run(test.path+strings.Join(test.args, "-"), func(t *testing.T) {
			t.Parallel()
			intentionalError := errors.New("assertion stop before commit")
			calls := 0
			executor := &Executor{Apply: func(_ context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
				calls++
				require.Equal(t, "mutation-key", key)
				require.Len(t, requests, 1)
				test.check(t, requests[0])

				return nil, intentionalError
			}}
			req := writeTestRequest(test.path, test.args, test.body)
			req.Flags["idempotency-key"] = "mutation-key"
			if test.path == "delete" || strings.HasSuffix(test.path, "/delete") {
				req.Flags["confirm"] = "true"
			}
			_, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
			require.ErrorIs(t, err, intentionalError)
			require.Equal(t, 1, calls)
		})
	}
}

func TestWriteRejectsInvalidPayloadBeforeApply(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		path    string
		args    []string
		body    string
		message string
	}{
		{"create", nil, `{"initialSchema":[{"targetType":"invalid","key":"x","type":"uint64"}]}`, "initialSchema[0]"},
		{"create", nil, `{"accountTypes":{"a":{"persistence":"invalid"}}}`, `accountTypes["a"]`},
		{"create", nil, `{"defaultEnforcementMode":"invalid"}`, "invalid enforcement mode"},
		{"create", nil, `{"mode":"MIRROR","mirrorSource":{"type":"unsupported"}}`, "unsupported mirror source"},
		{"create", nil, `{"mode":"MIRROR","mirrorSource":{"rewriteRules":[null]}}`, "rewriteRules[0]"},
		{"metadata/set", nil, `{"nested":{"bad":1}}`, "invalid metadata"},
		{"transactions/revert", []string{"1"}, `{"metadata":{"number":1.5}}`, "invalid metadata"},
		{"indexes/create", nil, `{"id":"invalid"}`, "invalid"},
		{"indexes/create", nil, `{}`, "id is required"},
		{"bulk", nil, `[{"action":"CREATE_TRANSACTION","data":{}},null]`, "bulk element 1"},
		{"bulk", nil, `[{"action":"CREATE_TRANSACTION","data":{}},{"action":"BAD","data":{}}]`, "unsupported action"},
	} {
		t.Run(test.path+test.message, func(t *testing.T) {
			t.Parallel()
			executor := &Executor{Apply: func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
				t.Fatal("invalid input reached Apply")

				return nil, nil
			}}
			_, handled, err := executor.write(t.Context(), writeTestRequest(test.path, test.args, test.body))
			require.True(t, handled)
			require.ErrorContains(t, err, test.message)
		})
	}
}

func TestWriteNativeResultTypesAndNoContentResponses(t *testing.T) {
	t.Parallel()
	index, err := indexes.ParseCanonical("log_builtin:LOG_BUILTIN_INDEX_DATE")
	require.NoError(t, err)
	ledgerLog := func(payload *commonpb.LedgerLogPayload) *commonpb.LogPayload {
		return writeTestLedgerLog(1, payload).GetLogs()[0].GetPayload()
	}
	for _, test := range []struct {
		path    string
		payload *commonpb.LogPayload
		native  proto.Message
		content bool
	}{
		{"ledger/delete", &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "bank"}}}, &commonpb.DeletedLedgerLog{}, false},
		{"ledger/metadata/set", &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{Ledger: "bank"}}}, &commonpb.SavedLedgerMetadataLog{}, false},
		{"ledger/metadata/delete", &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedLedgerMetadata{DeletedLedgerMetadata: &commonpb.DeletedLedgerMetadataLog{Ledger: "bank"}}}, &commonpb.DeletedLedgerMetadataLog{}, false},
		{"ledger/accounts/metadata/set", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{}}}), &commonpb.SavedMetadata{}, false},
		{"ledger/transactions/metadata/set", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{}}}), &commonpb.SavedMetadata{}, false},
		{"ledger/accounts/metadata/delete", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{}}}), &commonpb.DeletedMetadata{}, false},
		{"ledger/transactions/metadata/delete", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{}}}), &commonpb.DeletedMetadata{}, false},
		{"ledger/transactions/revert", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{RevertedTransactionId: 1}}}), &commonpb.RevertedTransaction{}, true},
		{"ledger/indexes/create", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: index}}}), &commonpb.CreatedIndexLog{}, true},
		{"ledger/indexes/delete", ledgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DropIndex{DropIndex: &commonpb.DroppedIndexLog{Id: index}}}), &commonpb.DroppedIndexLog{}, false},
	} {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			executor := &Executor{}
			requests := []*servicepb.Request{{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Id: index}}}}
			result, err := executor.writeResult(test.path, requests, &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: test.payload}}})
			require.NoError(t, err)
			require.IsType(t, test.native, executor.Result)
			if test.content {
				require.NotEmpty(t, result.Data)
				require.True(t, json.Valid(result.Data))
			} else {
				require.Empty(t, result.Data, "HTTP 204 responses retain their native result only for ledgerctl rendering")
			}

			_, err = executor.writeResult(test.path, requests, &servicepb.ApplyResponse{Logs: []*commonpb.Log{{}}})
			require.ErrorContains(t, err, "mutation may have committed", "a mismatched response payload must not be treated as a valid result")
			require.True(t, executor.Result == nil, "an invalid response must leave a nil interface, not a typed nil result")
		})
	}
}

func TestWriteClearsResultAndRetainsOnlyCurrentApplyResponse(t *testing.T) {
	t.Parallel()
	verificationErr := errors.New("response verification failed")
	current := &servicepb.ApplyResponse{Logs: []*commonpb.Log{{}}}
	executor := &Executor{
		Result:            &commonpb.LedgerInfo{Name: "previous"},
		LastApplyResponse: &servicepb.ApplyResponse{},
		Apply: func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
			return current, verificationErr
		},
	}
	req := writeTestRequest("metadata/set", nil, `{"number":9007199254740993}`)
	response, handled, err := executor.write(t.Context(), req)
	require.True(t, handled)
	require.ErrorIs(t, err, verificationErr)
	require.Empty(t, response.Data)
	require.True(t, executor.Result == nil)
	require.Same(t, current, executor.LastApplyResponse, "retain the current response when post-Apply verification fails")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	executor.Apply = func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		t.Fatal("a canceled invocation must not call Apply")

		return nil, nil
	}
	_, handled, err = executor.write(ctx, req)
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, executor.Result == nil)
	require.Nil(t, executor.LastApplyResponse, "a canceled invocation must not expose a previous Apply response")
}

func TestWriteBulkPreservesAtomicAndPerElementIdempotency(t *testing.T) {
	t.Parallel()
	body := `[{"action":"CREATE_TRANSACTION","ik":"first","skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"],"data":{"postings":[{"source":"world","destination":"users:1","amount":340282366920938463463374607431768211455,"asset":"USD/2"}]}},{"action":"REVERT_TRANSACTION","ik":"second","data":{"id":9007199254740993,"force":true,"metadata":{"n":9007199254740993}}},{"action":"ADD_METADATA","ik":"third","data":{"targetType":"ACCOUNT","targetId":"users:1","metadata":{"n":9007199254740993}}},{"action":"DELETE_METADATA","ik":"fourth","data":{"targetType":"TRANSACTION","targetId":9007199254740993,"key":"key"}}]`
	for _, atomic := range []bool{false, true} {
		t.Run(fmt.Sprintf("atomic=%t", atomic), func(t *testing.T) {
			t.Parallel()
			var keys []string
			var recorded []*servicepb.Request
			executor := &Executor{Apply: func(_ context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
				keys = append(keys, key)
				recorded = append(recorded, requests...)
				logs := make([]*commonpb.Log, len(requests))
				for i, request := range requests {
					payload := &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{RevertedTransactionId: 9007199254740993}}}
					if create := request.GetApply().GetAction().GetCreateTransaction(); create != nil {
						payload = &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: create.GetPostings()}}}}
					}
					logs[i] = writeTestLedgerLog(9007199254740993+uint64(len(recorded))+uint64(i), payload).GetLogs()[0]
				}

				return &servicepb.ApplyResponse{Logs: logs}, nil
			}}
			req := writeTestRequest("bulk", nil, body)
			req.Flags["atomic"], req.Flags["idempotency-key"] = strconv.FormatBool(atomic), "batch-key"
			response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
			require.NoError(t, err)
			if atomic {
				require.Equal(t, []string{"batch-key"}, keys)
			} else {
				require.Equal(t, []string{"first", "second", "third", "fourth"}, keys)
			}
			require.Len(t, recorded, 4)
			require.Equal(t, []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}, recorded[0].GetApply().GetSkippableReasons())
			require.Equal(t, uint64(9007199254740993), recorded[1].GetApply().GetAction().GetRevertTransaction().GetTransactionId())
			require.Equal(t, "users:1", recorded[2].GetApply().GetAction().GetAddMetadata().GetTarget().GetAccount().GetAddr())
			require.Equal(t, uint64(9007199254740993), recorded[3].GetApply().GetAction().GetDeleteMetadata().GetTarget().GetTransactionId())
			var envelope struct {
				Data []writeBulkResult `json:"data"`
			}
			require.NoError(t, json.Unmarshal(response.Data, &envelope))
			require.Len(t, envelope.Data, 4)
			require.Equal(t, "CREATE_TRANSACTION", envelope.Data[0].ResponseType)
			require.Contains(t, string(envelope.Data[0].Data), "340282366920938463463374607431768211455")
			require.Empty(t, envelope.Data[1].Data, "revert bulk entries have no data in the HTTP contract")
			require.NotZero(t, envelope.Data[0].LogID)
		})
	}
}

func TestWriteBulkReturnsPartialResultsAndStopsOrContinues(t *testing.T) {
	t.Parallel()
	businessErr := &apierr.Remote{KindValue: domain.KindConflict, ReasonValue: "TRANSACTION_REFERENCE_CONFLICT", Msg: "reference already exists"}
	body := `[{"action":"CREATE_TRANSACTION","ik":"first","data":{}},{"action":"CREATE_TRANSACTION","ik":"second","data":{}},{"action":"CREATE_TRANSACTION","ik":"third","data":{}}]`
	for _, continued := range []bool{false, true} {
		t.Run(fmt.Sprintf("continue=%t", continued), func(t *testing.T) {
			t.Parallel()
			var keys []string
			executor := &Executor{Apply: func(_ context.Context, key string, _ ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
				keys = append(keys, key)
				if key == "second" {
					return nil, businessErr
				}

				return writeTestLedgerLog(uint64(len(keys)), &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 9007199254740993}}}}), nil
			}}
			req := writeTestRequest("bulk", nil, body)
			req.Flags["continue-on-failure"] = strconv.FormatBool(continued)
			response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
			require.ErrorIs(t, err, businessErr)
			var envelope struct {
				Data []writeBulkResult `json:"data"`
			}
			require.NoError(t, json.Unmarshal(response.Data, &envelope))
			require.Len(t, envelope.Data, 3)
			require.Equal(t, "CREATE_TRANSACTION", envelope.Data[0].ResponseType)
			require.Contains(t, string(envelope.Data[0].Data), "9007199254740993")
			require.Equal(t, "TRANSACTION_REFERENCE_CONFLICT", envelope.Data[1].ErrorCode)
			require.Equal(t, "reference already exists", envelope.Data[1].ErrorDescription)
			if continued {
				require.Equal(t, []string{"first", "second", "third"}, keys)
				require.Equal(t, "CREATE_TRANSACTION", envelope.Data[2].ResponseType)
			} else {
				require.Equal(t, []string{"first", "second"}, keys)
				require.Equal(t, "ERROR", envelope.Data[2].ResponseType)
				require.Equal(t, "context canceled", envelope.Data[2].ErrorDescription)
			}
		})
	}
}

func TestWriteBulkSkippedOutcomeRetainsReasonAndLogID(t *testing.T) {
	t.Parallel()
	calls := 0
	executor := &Executor{Apply: func(_ context.Context, _ string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		require.Equal(t, []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}, requests[0].GetApply().GetSkippableReasons())

		return writeTestLedgerLog(9007199254740993, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, Context: map[string]string{"reference": "duplicate"}}}}), nil
	}}
	req := writeTestRequest("bulk", nil, `[{"action":"CREATE_TRANSACTION","skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"],"data":{"reference":"duplicate"}}]`)
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	var envelope struct {
		Data []writeBulkResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &envelope))
	require.Len(t, envelope.Data, 1)
	require.Equal(t, uint64(9007199254740993), envelope.Data[0].LogID)
	require.Equal(t, "CREATE_TRANSACTION", envelope.Data[0].ResponseType)
	require.Contains(t, string(envelope.Data[0].Data), `"skipped":true`)
	require.Contains(t, string(envelope.Data[0].Data), "TRANSACTION_REFERENCE_CONFLICT")
	require.Contains(t, string(envelope.Data[0].Data), "duplicate")
}

func TestWriteBulkResultFailureKeepsExecutionPolicy(t *testing.T) {
	t.Parallel()
	var keys []string
	executor := &Executor{Apply: func(_ context.Context, key string, _ ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		keys = append(keys, key)
		if key == "first" {
			return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{}}}, nil
		}

		return writeTestLedgerLog(2, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 2}}}}), nil
	}}
	req := writeTestRequest("bulk", nil, `[{"action":"CREATE_TRANSACTION","ik":"first","data":{}},{"action":"CREATE_TRANSACTION","ik":"second","data":{}}]`)
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.ErrorContains(t, err, "mutation may have committed")
	require.Equal(t, []string{"first", "second"}, keys)
	var envelope struct {
		Data []writeBulkResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &envelope))
	require.Equal(t, "ERROR", envelope.Data[0].ResponseType)
	require.Equal(t, "CREATE_TRANSACTION", envelope.Data[1].ResponseType)
}

func TestWriteCancellationNeverDispatchesOrRetries(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	executor := &Executor{Apply: func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		t.Fatal("already-canceled command reached Apply")

		return nil, nil
	}}
	_, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(ctx, writeTestRequest("transactions/create", nil, `{}`))
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	executor.Apply = func(ctx context.Context, _ string, _ ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		cancel()

		return nil, ctx.Err()
	}
	req := writeTestRequest("bulk", nil, `[{"action":"CREATE_TRANSACTION","data":{}},{"action":"CREATE_TRANSACTION","data":{}}]`)
	req.Flags["continue-on-failure"] = "true"
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	require.NotEmpty(t, response.Data)
}

func TestWriteResultFailureNeverReplaysApply(t *testing.T) {
	t.Parallel()
	calls := 0
	executor := &Executor{Apply: func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++

		return &servicepb.ApplyResponse{}, nil
	}}
	_, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), writeTestRequest("transactions/create", nil, `{}`))
	require.ErrorContains(t, err, "mutation may have committed")
	require.Equal(t, 1, calls)

	calls = 0
	executor.Apply = func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++

		return writeTestLedgerLog(1, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{}}}), nil
	}
	_, err = ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), writeTestRequest("transactions/create", nil, `{}`))
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestWriteBulkAtomicFailureDoesNotSplitOrReplay(t *testing.T) {
	t.Parallel()
	intentionalError := errors.New("lost acknowledgement")
	calls := 0
	executor := &Executor{Apply: func(_ context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		calls++
		require.Equal(t, "batch-key", key)
		require.Len(t, requests, 2)

		return nil, intentionalError
	}}
	req := writeTestRequest("bulk", nil, `[{"action":"CREATE_TRANSACTION","ik":"ignored-a","data":{}},{"action":"CREATE_TRANSACTION","ik":"ignored-b","data":{}}]`)
	req.Flags["atomic"], req.Flags["idempotency-key"] = "true", "batch-key"
	response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.ErrorIs(t, err, intentionalError)
	require.Equal(t, 1, calls)
	var envelope struct {
		Data []writeBulkResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &envelope))
	require.Len(t, envelope.Data, 2)
	require.Equal(t, "ERROR", envelope.Data[0].ResponseType)
	require.Equal(t, "ERROR", envelope.Data[1].ResponseType)
	require.NotContains(t, string(response.Data), intentionalError.Error())
}

func TestWriteBulkMapsWireErrorsWithoutTrustingInvalidPairs(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		reason  string
		code    codes.Code
		want    string
		message string
	}{
		{"future reason", "FUTURE_PRODUCT_REASON", codes.Aborted, "FUTURE_PRODUCT_REASON", "public future conflict"},
		{"invalid pair", "LEDGER_DELETED", codes.Canceled, "INTERNAL_ERROR", "peer-secret-details"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			wire, err := status.New(test.code, test.message).WithDetails(&errdetails.ErrorInfo{Domain: "ledger", Reason: test.reason})
			require.NoError(t, err)
			executor := &Executor{Apply: func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
				return nil, wire.Err()
			}}
			response, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), writeTestRequest("bulk", nil, `[{"action":"CREATE_TRANSACTION","data":{}}]`))
			require.Error(t, err)
			var envelope struct {
				Data []writeBulkResult `json:"data"`
			}
			require.NoError(t, json.Unmarshal(response.Data, &envelope))
			require.Len(t, envelope.Data, 1)
			require.Equal(t, test.want, envelope.Data[0].ErrorCode)
			if test.want == "INTERNAL_ERROR" {
				require.NotContains(t, string(response.Data), test.message)
				require.NotContains(t, string(response.Data), test.reason)
			} else {
				require.Equal(t, test.message, envelope.Data[0].ErrorDescription)
			}
		})
	}
}

func TestWriteMutationInputsRemainImmutable(t *testing.T) {
	t.Parallel()
	req := writeTestRequest("transactions/create", nil, `{"postings":[{"source":"world","destination":"users:1","amount":1,"asset":"USD/2"}]}`)
	var original, captured *servicepb.Request
	executor := &Executor{Apply: func(_ context.Context, _ string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		captured = requests[0]
		original = proto.Clone(requests[0]).(*servicepb.Request)

		return writeTestLedgerLog(1, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: requests[0].GetApply().GetAction().GetCreateTransaction().GetPostings()}}}}), nil
	}}
	_, err := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute).Execute(t.Context(), req)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, captured), "encoding a response must not modify its accepted request")
}

func writeTestRequest(path string, args []string, body string) pluginsdk.ExecuteRequest {
	var data json.RawMessage
	if body != "" {
		data = json.RawMessage(body)
	}

	return pluginsdk.ExecuteRequest{CommandPath: strings.Split("ledger/"+path, "/"), Args: args, Body: data, Flags: map[string]string{"ledger": "bank"}}
}

func writeTestLedgerLog(id uint64, payload *commonpb.LedgerLogPayload) *servicepb.ApplyResponse {
	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: id, Data: payload}}}}}}}
}

//go:build e2e

package business

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func parityHTTP(method, path, body string, want int) []byte {
	GinkgoHelper()
	ctx := GinkgoT().Context()
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://127.0.0.1:%d/v3%s", sharedHTTPPort, path), strings.NewReader(body))
	Expect(err).To(Succeed())
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	Expect(err).To(Succeed())
	defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).To(Succeed())
	Expect(resp.StatusCode).To(Equal(want), "%s %s: %s", method, path, raw)

	return raw
}

func parityDecode[T any](raw []byte) T {
	GinkgoHelper()
	var decoded T
	Expect(json.Unmarshal(raw, &decoded)).To(Succeed())

	return decoded
}

var _ = Describe("REST and gRPC business parity", func() {
	It("resolves an encoded ledger name exactly once on reads and writes", func() {
		ctx := GinkgoT().Context()
		const ledger = "parity:encoded"
		_, err := sharedClient.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		got := parityDecode[struct{ Data struct{ Name string } }](
			parityHTTP(http.MethodGet, "/parity%3Aencoded", "", http.StatusOK))
		Expect(got.Data.Name).To(Equal(ledger))

		parityHTTP(http.MethodPost, "/parity%3Aencoded/metadata", `{"source":"encoded-path"}`, http.StatusNoContent)
		info, err := sharedClient.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		Expect(err).To(Succeed())
		Expect(info.GetMetadata()["source"].GetStringValue()).To(Equal("encoded-path"))
		parityHTTP(http.MethodGet, "/parity%253Aencoded", "", http.StatusBadRequest)
	})

	It("rejects sink creation without a type through both transports", func() {
		ctx := GinkgoT().Context()
		const name = "parity-untyped"
		parityHTTP(http.MethodPost, "/_/events-sinks", `{"name":"parity-untyped"}`, http.StatusBadRequest)
		_, err := sharedClient.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{
				Config: &commonpb.SinkConfig{Name: name},
			}},
		}))
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument), "%v", err)
		Expect(err.Error()).To(ContainSubstring("sink name and one sink type are required"))

		sinks, err := sharedClient.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		for _, sink := range sinks.GetSinks() {
			Expect(sink.GetName()).NotTo(Equal(name))
		}
	})

	It("rejects a mirror source on a normal ledger through both transports", func() {
		ctx := GinkgoT().Context()
		const restName, grpcName = "parity-rest-normal", "parity-grpc-normal"
		raw := parityHTTP(http.MethodPost, "/"+restName,
			`{"mode":"NORMAL","mirrorSource":{"ledgerName":"source","baseUrl":"http://127.0.0.1:1"}}`,
			http.StatusBadRequest)
		Expect(parityDecode[struct{ ErrorMessage string }](raw).ErrorMessage).To(Equal("mirrorSource requires mode MIRROR"))
		_, err := sharedClient.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
				Name: grpcName,
				Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
				MirrorSource: &commonpb.MirrorSourceConfig{
					LedgerName: "source",
					Type:       &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://127.0.0.1:1"}},
				},
			}},
		}))
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument), "%v", err)
		Expect(err.Error()).To(ContainSubstring("mirrorSource requires mode MIRROR"))
		for _, name := range []string{restName, grpcName} {
			_, err := sharedClient.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
			Expect(status.Code(err)).To(Equal(codes.NotFound), "%v", err)
		}
	})

	It("rejects a mirror ledger without a source over REST", func() {
		const name = "parity-rest-mirror-no-source"
		raw := parityHTTP(http.MethodPost, "/"+name, `{"mode":"MIRROR"}`, http.StatusBadRequest)
		Expect(parityDecode[struct{ ErrorMessage string }](raw).ErrorMessage).To(Equal("mode MIRROR requires mirrorSource"))
		parityHTTP(http.MethodGet, "/"+name, "", http.StatusNotFound)
	})

	It("rejects a mirror ledger without a source over gRPC", func() {
		ctx := GinkgoT().Context()
		const name = "parity-grpc-mirror-no-source"
		_, err := sharedClient.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
				Name: name,
				Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			}},
		}))
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument), "%v", err)
		Expect(status.Convert(err).Message()).To(ContainSubstring("mode MIRROR requires mirrorSource"))
		_, err = sharedClient.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
		Expect(status.Code(err)).To(Equal(codes.NotFound), "%v", err)
	})

	It("uses exact prepared query integers and rejects a null filter argument", func() {
		ctx := GinkgoT().Context()
		const ledger = "parity-prepared-params"
		const exact = 9007199254740993
		const account = commonpb.TargetType_TARGET_TYPE_ACCOUNT
		parityApply(
			actions.CreateLedgerWithSchemaAction(ledger, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{TargetType: account, Key: "rank", Type: commonpb.MetadataType_METADATA_TYPE_INT64},
				{TargetType: account, Key: "tag", Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			}),
			actions.CreateAccountMetadataIndexAction(ledger, "rank"),
			actions.CreateAccountMetadataIndexAction(ledger, "tag"),
		)
		Expect(actions.WaitForMetadataIndexReady(ctx, sharedClient, ledger, account, "rank")).To(Succeed())
		Expect(actions.WaitForMetadataIndexReady(ctx, sharedClient, ledger, account, "tag")).To(Succeed())
		parityApply(
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{actions.NewPosting("world", "lower", big.NewInt(7), "USD/2")}, nil, nil),
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{actions.NewPosting("world", "exact", big.NewInt(19), "USD/2")}, nil, nil),
			actions.SaveAccountMetadataAction(ledger, "lower", map[string]string{"rank": "9007199254740992", "tag": ""}),
			actions.SaveAccountMetadataAction(ledger, "exact", map[string]string{"rank": "9007199254740993", "tag": "selected"}),
			actions.CreatePreparedQueryAction("rank", ledger, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, actions.ParamInt64RangeMetadataFilter("rank", "x", "x")),
			actions.CreatePreparedQueryAction("tag", ledger, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, actions.ParamStringMetadataFilter("tag", "x")),
		)

		result := parityDecode[struct {
			AggregateResult struct{ Volumes []struct{ Input json.Number } }
		}](parityHTTP(http.MethodPost, "/"+ledger+"/prepared-queries/rank/execute",
			`{"mode":"AGGREGATE_VOLUMES","parameters":{"x":9007199254740993}}`, http.StatusOK))
		Expect(result.AggregateResult.Volumes).To(HaveLen(1))
		Expect(result.AggregateResult.Volumes[0].Input.String()).To(Equal("19"))
		grpcResult, err := sharedClient.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger: ledger, QueryName: "rank", Mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
			Parameters: map[string]*commonpb.ParameterValue{
				"x": {Value: &commonpb.ParameterValue_Int64Value{Int64Value: exact}},
			},
		})
		Expect(err).To(Succeed())
		Expect(grpcResult.GetAggregate().GetVolumes()).To(HaveLen(1))
		Expect(grpcResult.GetAggregate().GetVolumes()[0].GetInput().DecimalString()).To(Equal("19"))

		nullError := parityHTTP(http.MethodPost, "/"+ledger+"/prepared-queries/tag/execute",
			`{"parameters":{"x":null}}`, http.StatusBadRequest)
		Expect(parityDecode[struct{ ErrorCode string }](nullError).ErrorCode).To(Equal("INVALID_PARAMETERS"))
		_, err = sharedClient.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{Ledger: ledger, QueryName: "tag"})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument), "%v", err)
		empty := parityDecode[struct {
			Cursor struct{ AccountData []struct{ Address string } }
		}](parityHTTP(http.MethodPost, "/"+ledger+"/prepared-queries/tag/execute", `{"parameters":{"x":""}}`, http.StatusOK))
		Expect(empty.Cursor.AccountData).To(HaveLen(1))
		Expect(empty.Cursor.AccountData[0].Address).To(Equal("lower"))
	})

	It("rejects trailing JSON before creating, updating or executing a prepared query", func() {
		ctx := GinkgoT().Context()
		const ledger = "parity-prepared-framing"
		path := "/" + ledger + "/prepared-queries"
		parityApply(actions.CreateLedgerAction(ledger, nil), actions.CreatePreparedQueryAction("existing", ledger, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil))
		parityHTTP(http.MethodPost, path, `{"name":"extra","target":"ACCOUNTS","filter":null} {}`, http.StatusBadRequest)
		parityHTTP(http.MethodPut, path+"/existing", `{"target":"TRANSACTIONS","filter":null} {}`, http.StatusBadRequest)
		parityHTTP(http.MethodPost, path+"/existing/execute", `{"pageSize":1} {}`, http.StatusBadRequest)

		queries, err := sharedClient.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{Ledger: ledger})
		Expect(err).To(Succeed())
		Expect(queries.GetQueries()).To(HaveLen(1))
		Expect(queries.GetQueries()[0].GetName()).To(Equal("existing"))
		Expect(queries.GetQueries()[0].GetTarget()).To(Equal(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS))
		parityHTTP(http.MethodPost, path+"/existing/execute", `{"pageSize":1} `, http.StatusOK)
	})

	It("selects quoted metadata strings the same way as a typed gRPC filter", func() {
		ctx := GinkgoT().Context()
		const ledger = "parity-quoted-filter"
		const account = commonpb.TargetType_TARGET_TYPE_ACCOUNT
		const transaction = commonpb.TargetType_TARGET_TYPE_TRANSACTION
		parityApply(
			actions.CreateLedgerWithSchemaAction(ledger, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{TargetType: account, Key: "label", Type: commonpb.MetadataType_METADATA_TYPE_STRING},
				{TargetType: transaction, Key: "label", Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			}),
			actions.CreateAccountMetadataIndexAction(ledger, "label"),
			actions.CreateTransactionMetadataIndexAction(ledger, "label"),
		)
		Expect(actions.WaitForMetadataIndexReady(ctx, sharedClient, ledger, account, "label")).To(Succeed())
		Expect(actions.WaitForMetadataIndexReady(ctx, sharedClient, ledger, transaction, "label")).To(Succeed())
		parityApply(
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{actions.NewPosting("world", "alice", big.NewInt(7), "USD/2")}, map[string]string{"label": "true"}, nil),
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{actions.NewPosting("world", "bob", big.NewInt(19), "USD/2")}, map[string]string{"label": "42"}, nil),
			actions.SaveAccountMetadataAction(ledger, "alice", map[string]string{"label": "true"}),
			actions.SaveAccountMetadataAction(ledger, "bob", map[string]string{"label": "42"}),
		)
		for i, label := range []string{"true", "42"} {
			address := []string{"alice", "bob"}[i]
			amount := []string{"7", "19"}[i]
			path := "/" + ledger
			query := "?filter=" + url.QueryEscape(fmt.Sprintf(`metadata[label] == %q`, label))
			filter := actions.StringMetadataFilter("label", label)
			hAccounts := parityDecode[struct{ Data []struct{ Address string } }](
				parityHTTP(http.MethodGet, path+"/accounts"+query, "", http.StatusOK))
			gAccounts, err := actions.ListAccountsFiltered(ctx, sharedClient, ledger, 100, "", filter)
			Expect(err).To(Succeed())
			Expect(hAccounts.Data).To(HaveLen(1))
			Expect(gAccounts).To(HaveLen(1))
			Expect(hAccounts.Data[0].Address).To(Equal(address))
			Expect(gAccounts[0].GetAddress()).To(Equal(address))

			hTx := parityDecode[struct{ Data []struct{ ID uint64 } }](
				parityHTTP(http.MethodGet, path+"/transactions"+query, "", http.StatusOK))
			gTx, err := actions.ListTransactionsFiltered(ctx, sharedClient, ledger, 100, 0, filter)
			Expect(err).To(Succeed())
			Expect(hTx.Data).To(HaveLen(1))
			Expect(gTx).To(HaveLen(1))
			Expect(hTx.Data[0].ID).To(Equal(uint64(i + 1)))
			Expect(gTx[0].GetId()).To(Equal(hTx.Data[0].ID))

			hVol := parityDecode[struct {
				Data struct{ Volumes []struct{ Input json.Number } }
			}](parityHTTP(http.MethodGet, path+"/volumes"+query, "", http.StatusOK))
			gVol, err := sharedClient.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{Ledger: ledger, Filter: filter})
			Expect(err).To(Succeed())
			Expect(hVol.Data.Volumes).To(HaveLen(1))
			Expect(gVol.GetVolumes()).To(HaveLen(1))
			Expect(hVol.Data.Volumes[0].Input.String()).To(Equal(amount))
			Expect(gVol.GetVolumes()[0].GetInput().DecimalString()).To(Equal(amount))
		}
	})

	It("honors sub-microsecond date bounds on REST transaction and log lists", func() {
		ctx := GinkgoT().Context()
		const ledger = "parity-date"
		const txIndex = commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP
		const logIndex = commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE
		parityApply(actions.CreateLedgerAction(ledger, nil), actions.CreateBuiltinTxIndexAction(ledger, txIndex),
			actions.CreateLogBuiltinIndexAction(ledger, logIndex))
		Expect(actions.WaitForBuiltinIndexReady(ctx, sharedClient, ledger, txIndex)).To(Succeed())
		Expect(actions.WaitForLogBuiltinIndexReady(ctx, sharedClient, ledger, logIndex)).To(Succeed())

		stamp := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
		transaction := func(at time.Time) *servicepb.Request {
			req := actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(1), "USD/2"),
			}, nil, nil)
			req.GetApply().GetAction().GetCreateTransaction().Timestamp = &commonpb.Timestamp{Data: uint64(at.UnixMicro())}

			return req
		}
		parityApply(transaction(stamp), transaction(stamp.Add(time.Microsecond)))
		for _, lower := range []bool{true, false} {
			bound := uint64(stamp.Add(time.Microsecond).UnixMicro())
			arg := "startDate"
			condition := &commonpb.UintCondition{Min: &bound}
			want := uint64(2)
			if !lower {
				arg = "endDate"
				condition = &commonpb.UintCondition{Max: &bound, MaxExclusive: true}
				want = 1
			}
			filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{Field: txIndex, Cond: condition},
			}}
			gTx, err := actions.ListTransactionsFiltered(ctx, sharedClient, ledger, 100, 0, filter)
			Expect(err).To(Succeed())
			hTx := parityDecode[struct{ Data []struct{ ID uint64 } }](parityHTTP(http.MethodGet,
				"/"+ledger+"/transactions?"+arg+"="+url.QueryEscape(stamp.Add(500*time.Nanosecond).Format(time.RFC3339Nano)), "", http.StatusOK))
			Expect(gTx).To(HaveLen(1))
			Expect(hTx.Data).To(HaveLen(1))
			Expect(gTx[0].GetId()).To(Equal(want))
			Expect(hTx.Data[0].ID).To(Equal(want))
		}

		unfiltered, err := actions.ListAllLogs(ctx, sharedClient, ledger)
		Expect(err).To(Succeed())
		Expect(unfiltered).NotTo(BeEmpty())
		first := unfiltered[len(unfiltered)-1]
		firstDate := first.GetPayload().GetApply().GetLog().GetDate()
		Expect(firstDate).NotTo(BeNil())
		nextTick := firstDate.GetData() + 1
		filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
			LogBuiltinUint: &commonpb.LogBuiltinUintCondition{Field: logIndex, Cond: &commonpb.UintCondition{Max: &nextTick, MaxExclusive: true}},
		}}
		gLogs, err := actions.ListLogsFiltered(ctx, sharedClient, &servicepb.ListLogsRequest{
			Ledger: ledger, Options: &commonpb.ListOptions{Filter: filter},
		})
		Expect(err).To(Succeed())
		Expect(gLogs).NotTo(BeEmpty())
		hLogs := parityDecode[struct{ Data []struct{ Sequence uint64 } }](parityHTTP(http.MethodGet,
			"/"+ledger+"/logs?endDate="+url.QueryEscape(firstDate.AsTime().Add(500*time.Nanosecond).Format(time.RFC3339Nano)), "", http.StatusOK))
		Expect(hLogs.Data).To(HaveLen(len(gLogs)))
		for i, log := range gLogs {
			Expect(hLogs.Data[i].Sequence).To(Equal(log.GetSequence()))
		}
	})
})

func parityApply(requests ...*servicepb.Request) {
	GinkgoHelper()
	_, err := sharedClient.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("", requests...))
	Expect(err).To(Succeed())
}

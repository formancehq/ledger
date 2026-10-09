package http

import (
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetTransaction_BigintHeader(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)

	for _, tc := range []struct {
		header string
		quoted bool
	}{
		{"", false}, {"false", false}, {"0", false}, {"invalid", false},
		{" true ", false}, {"true", true}, {"TRUE", true}, {"Yes", true}, {"Y", true}, {"1", true},
	} {
		t.Run(tc.header, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().GetLedgerByName(gomock.Any(), "ledger1").Return(&commonpb.LedgerInfo{Name: "ledger1"}, nil)
			backend.EXPECT().GetTransaction(gomock.Any(), "ledger1", uint64(42)).Return(&commonpb.Transaction{
				Id: 42, Postings: []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD", Amount: commonpb.NewUint256FromUint64(9007199254740993)}},
			}, nil)
			r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{"ledgerName": "ledger1", "transactionId": "42"})
			r.Header.Set("Formance-Bigint-As-String", tc.header)
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleGetTransaction(w, r)
			require.Equal(t, http.StatusOK, w.Code)
			var response any
			require.NoError(t, stdjson.Unmarshal(w.Body.Bytes(), &response))
			require.NoError(t, doc.Components.Schemas["GetTransactionResponse"].Value.VisitJSON(response))
			if tc.quoted {
				require.Contains(t, w.Body.String(), `"amount":"9007199254740993"`)
			} else {
				require.Contains(t, w.Body.String(), `"amount":9007199254740993`)
			}
		})
	}
}

func TestOpenAPIMonetaryHeaderCoverage(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	expected := map[string]bool{
		"createTransaction": true, "getTransaction": true, "listLedgerTransactions": true,
		"revertTransaction": true, "bulkOperations": true, "listLedgerLogs": true,
		"getLog": true, "executePreparedQuery": true, "getAccount": true,
		"listAccounts": true, "aggregateVolumes": true, "analyzeTransactions": true,
	}
	for _, path := range doc.Paths.Map() {
		for _, operation := range path.Operations() {
			if !expected[operation.OperationID] {
				continue
			}
			found := false
			for _, parameter := range operation.Parameters {
				if parameter.Value.Name == HeaderBigIntAsString && parameter.Value.In == "header" {
					found = true
				}
			}
			require.True(t, found, "missing monetary header on %s", operation.OperationID)
			delete(expected, operation.OperationID)
		}
	}
	require.Empty(t, expected, "operation coverage must not silently become stale")

	posting := doc.Components.Schemas["PostingAmount"].Value
	for _, value := range []any{float64(0), float64(9007199254740992), "0", "9007199254740993", "123456789012345678901234567890"} {
		require.NoError(t, posting.VisitJSON(value))
	}
	for _, value := range []any{float64(-1), 1.5, "-1", "01", "1e2", "0x10", ""} {
		require.Error(t, posting.VisitJSON(value))
	}
	for _, value := range []any{float64(-1), "-123456789012345678901234567890"} {
		require.NoError(t, doc.Components.Schemas["SignedMonetaryAmount"].Value.VisitJSON(value))
	}
}

func TestBigintHeaderConcurrentRequests(t *testing.T) {
	t.Parallel()
	transaction := &commonpb.Transaction{Id: 42, Postings: []*commonpb.Posting{{
		Source: "world", Destination: "bank", Asset: "USD", Amount: commonpb.NewUint256FromUint64(9007199254740993),
	}}}
	const volume = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	transaction.PostCommitVolumes = &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{
		"bank": {Volumes: []*commonpb.VolumeEntry{{Asset: "USD", Volumes: &commonpb.Volumes{Input: commonpb.MustBigUintFromDecimal(volume), Output: commonpb.MustBigUintFromDecimal("0")}}}},
	}}
	before, err := proto.MarshalOptions{Deterministic: true}.Marshal(transaction)
	require.NoError(t, err)
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), "ledger1").Return(&commonpb.LedgerInfo{Name: "ledger1"}, nil).Times(64)
	backend.EXPECT().GetTransaction(gomock.Any(), "ledger1", uint64(42)).Return(transaction, nil).Times(64)
	server := newTestServer(t, backend)
	start := make(chan struct{})
	type result struct {
		quoted bool
		body   string
		code   int
	}
	results := make(chan result, 64)
	var group sync.WaitGroup
	for i := range 64 {
		group.Go(func() {
			r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{"ledgerName": "ledger1", "transactionId": "42"})
			quoted := i%2 == 0
			if quoted {
				r.Header.Set(HeaderBigIntAsString, "true")
			}
			w := httptest.NewRecorder()
			<-start
			server.handleGetTransaction(w, r)
			results <- result{quoted: quoted, body: w.Body.String(), code: w.Code}
		})
	}
	close(start)
	group.Wait()
	close(results)
	for result := range results {
		require.Equal(t, http.StatusOK, result.code)
		if result.quoted {
			require.Contains(t, result.body, `"amount":"9007199254740993"`)
			require.Contains(t, result.body, `"input":"`+volume+`"`)
		} else {
			require.Contains(t, result.body, `"amount":9007199254740993`)
			require.Contains(t, result.body, `"input":`+volume)
		}
	}
	after, err := proto.MarshalOptions{Deterministic: true}.Marshal(transaction)
	require.NoError(t, err)
	require.Equal(t, before, after, "HTTP encoding must not mutate shared/audited protobuf payloads")
}

func TestMonetaryCheckedResponseNestedFailure(t *testing.T) {
	t.Parallel()
	for _, header := range []string{"", "true"} {
		t.Run(header, func(t *testing.T) {
			t.Parallel()
			transaction := &commonpb.Transaction{PostCommitVolumes: &commonpb.PostCommitVolumes{
				VolumesByAccount: map[string]*commonpb.VolumesByAssets{
					"private-account": {Volumes: []*commonpb.VolumeEntry{{Asset: "USD"}}},
				},
			}}
			log := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
				Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: transaction},
				}}),
			}}}}
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().GetLog(gomock.Any(), uint64(7)).Return(log, nil)
			r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{"sequence": "7"})
			r.Header.Set(HeaderBigIntAsString, header)
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleGetLog(w, r)
			require.Equal(t, http.StatusInternalServerError, w.Code)
			require.True(t, stdjson.Valid(w.Body.Bytes()), "the failure must leave one complete error object")
			errorResponse := decodeResponse[ErrorResponse](t, w)
			require.Equal(t, "INTERNAL_ERROR", errorResponse.ErrorCode)
			require.Contains(t, errorResponse.ErrorMessage, "internal server error (correlation ID:")
			require.NotContains(t, w.Body.String(), "private-account")
			require.NotContains(t, w.Body.String(), `"data"`)
		})
	}
}

func TestMonetaryResponseDefaultCompatibility(t *testing.T) {
	t.Parallel()
	transaction := &commonpb.Transaction{
		Id: 42, Reference: "<>&\u2028\u2029", Timestamp: &commonpb.Timestamp{Data: 1700000000000000},
		Postings: []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD", Amount: commonpb.NewUint256FromUint64(9007199254740993)}},
		Metadata: map[string]*commonpb.MetadataValue{
			"amount": {Type: &commonpb.MetadataValue_IntValue{IntValue: 9007199254740993}},
			"html":   {Type: &commonpb.MetadataValue_StringValue{StringValue: "<>&\u2028\u2029"}},
		},
	}
	for _, checked := range []bool{false, true} {
		t.Run(map[bool]string{false: "streaming", true: "buffered"}[checked], func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			legacy, current := httptest.NewRecorder(), httptest.NewRecorder()
			if checked {
				writeOKChecked(legacy, r, transaction)
				writeMonetaryOKChecked(current, r, transaction)
			} else {
				writeOK(legacy, transaction)
				writeMonetaryOK(current, r, transaction)
			}
			require.JSONEq(t, legacy.Body.String(), current.Body.String())
			type wire struct {
				Data struct {
					Reference stdjson.RawMessage            `json:"reference"`
					Metadata  map[string]stdjson.RawMessage `json:"metadata"`
				} `json:"data"`
			}
			oldWire, newWire := decodeResponse[wire](t, legacy), decodeResponse[wire](t, current)
			require.Equal(t, oldWire.Data.Reference, newWire.Data.Reference, "preserve route-specific escaping")
			require.Equal(t, oldWire.Data.Metadata, newWire.Data.Metadata)
			require.Equal(t, !checked, current.Body.Bytes()[current.Body.Len()-1] == '\n')
			r.Header.Set(HeaderBigIntAsString, "true")
			optIn := httptest.NewRecorder()
			writeMonetaryOKChecked(optIn, r, transaction)
			require.Contains(t, optIn.Body.String(), `"amount":9007199254740993`, "integer metadata stays numeric")
			require.Contains(t, optIn.Body.String(), `"amount":"9007199254740993"`, "only the posting opts into strings")
		})
	}
}

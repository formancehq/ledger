package http

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

func TestPublicReceiptDisabledBeforeDispatch(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/v3/ledger1/bulk", strings.NewReader(`[]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Formance-Public-Receipt", "v1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Contains(t, response.Body.String(), "RESPONSE_SIGNING_UNAVAILABLE")
}

func receiptTransactionLog(id uint64) *commonpb.Log {
	return &commonpb.Log{Sequence: 91, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 4, Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: id}}}}}}}}}
}

func TestPublicReceiptResultAndReplay(t *testing.T) {
	t.Parallel()
	for _, replayed := range []bool{false, true} {
		t.Run(strconv.FormatBool(replayed), func(t *testing.T) {
			t.Parallel()
			signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
			log := receiptTransactionLog(42)
			original := log.CloneVT()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{log}, Replayed: replayed}, nil).Times(2)
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
			target := "/v3/ledger1/transactions"
			var responses []*httptest.ResponseRecorder
			for _, opt := range []bool{false, true} {
				req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD"}]}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", "historical-key")
				if opt {
					req.Header.Set(httpreceipt.Header, httpreceipt.Version)
				}
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				responses = append(responses, response)
			}
			require.Equal(t, 201, responses[1].Code, responses[1].Body.String())
			require.Equal(t, responses[0].Body.String(), responses[1].Body.String())
			require.Empty(t, responses[0].Header().Get(httpreceipt.Header))
			proof := responses[1].Header().Get(httpreceipt.Header)
			claims, err := httpreceipt.Verify(proof, signer.PublicKey(), http.MethodPost, target, 201, responses[1].Body.Bytes())
			require.NoError(t, err)
			require.Equal(t, uint64(1), claims.Applies)
			require.Equal(t, uint64(1), claims.Logs)
			if replayed {
				require.Equal(t, uint64(1), claims.Replayed)
			} else {
				require.Zero(t, claims.Replayed)
			}
			require.True(t, original.EqualVT(log), "receipt must not mutate authoritative returned log")
			_, err = httpreceipt.Verify(proof, signer.PublicKey(), http.MethodPost, target, 201, []byte(`{"data":{"id":43}}`))
			require.Error(t, err)
			require.Contains(t, responses[1].Header().Values("Access-Control-Expose-Headers"), httpreceipt.Header)
		})
	}
}

func TestPublicReceiptEmptyAcknowledgement(t *testing.T) {
	t.Parallel()
	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{{Sequence: 7}}}, nil)
	router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
	target := "/v3/ledger1/metadata"
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"color":"blue"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpreceipt.Header, "v1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	require.Equal(t, 204, response.Code)
	require.Empty(t, response.Body.String())
	claims, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), http.MethodPost, target, 204, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), claims.Applies)
}

func TestPublicReceiptAccountTypeCreatedAcknowledgement(t *testing.T) {
	t.Parallel()
	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{{Sequence: 7}}}, nil)
	router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
	target := "/v3/ledger1/account-types"
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"name":"users","pattern":"users:*"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpreceipt.Header, httpreceipt.Version)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.Empty(t, response.Body.String())
	claims, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), http.MethodPost, target, response.Code, response.Body.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(1), claims.Applies)
}

func TestPublicReceiptRejectsUnsupportedAndMalformed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		method, target string
		values         []string
	}{
		{"POST", "/v3/ledger1/bulk", []string{""}}, {"POST", "/v3/ledger1/bulk", []string{"v2"}}, {"POST", "/v3/ledger1/bulk", []string{"v1", "v1"}},
		{"GET", "/v3/ledger1", []string{"v1"}}, {"POST", "/v3/numscripts/analyze", []string{"v1"}}, {"GET", "/health", []string{"v1"}}, {"GET", "/debug/pprof/", []string{"v1"}},
	} {
		t.Run(tc.method+tc.target+strings.Join(tc.values, ","), func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signing.NewResponseSigner(make([]byte, 32)))
			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(`[]`))
			req.Header.Set("Content-Type", "application/json")
			for _, value := range tc.values {
				req.Header.Add(httpreceipt.Header, value)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, 400, response.Code, response.Body.String())
			require.Empty(t, response.Header().Get(httpreceipt.Header))
		})
	}
}

func TestPublicReceiptBulk(t *testing.T) {
	t.Parallel()
	for _, atomic := range []bool{false, true} {
		t.Run(strconv.FormatBool(atomic), func(t *testing.T) {
			t.Parallel()
			signer := signing.NewResponseSigner(make([]byte, 32))
			backend := NewMockBackend(gomock.NewController(t))
			if atomic {
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{receiptTransactionLog(42), receiptTransactionLog(43)}}, nil)
			} else {
				gomock.InOrder(backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{receiptTransactionLog(42)}, Replayed: true}, nil), backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed second write")))
			}
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
			target := "/v3/ledger1/bulk?atomic=" + strconv.FormatBool(atomic)
			body := strings.TrimSuffix(bulkWriteBody, "]") + "," + strings.TrimPrefix(bulkWriteBody, "[")
			req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(httpreceipt.Header, "v1")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			if atomic {
				require.Equal(t, 200, response.Code)
			} else {
				require.Equal(t, 500, response.Code)
				require.Contains(t, response.Body.String(), "ERROR")
			}
			claims, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), "POST", target, response.Code, response.Body.Bytes())
			require.NoError(t, err)
			require.Equal(t, uint64(1), claims.Applies)
			if atomic {
				require.Equal(t, uint64(2), claims.Logs)
				require.Zero(t, claims.Replayed)
			} else {
				require.Equal(t, uint64(1), claims.Logs)
				require.Equal(t, uint64(1), claims.Replayed)
			}
		})
	}
}

func TestPublicReceiptPostApplyEncodingFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		log  *commonpb.Log
	}{
		{"overflow", &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: strings.Repeat("x", publicReceiptBodyLimit+1)}}}}},
		{"invalid enum", &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger1", Mode: commonpb.LedgerMode(999)}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{tc.log}}, nil)
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signing.NewResponseSigner(make([]byte, 32)))
			req := httptest.NewRequest(http.MethodPost, "/v3/ledger1", nil)
			req.Header.Set(httpreceipt.Header, "v1")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, 500, response.Code)
			require.Contains(t, response.Body.String(), "INTERNAL_ERROR")
			require.Empty(t, response.Header().Get(httpreceipt.Header))
			require.Less(t, response.Body.Len(), 1024)
		})
	}
}

func TestPublicReceiptCredentialProjection(t *testing.T) {
	t.Parallel()
	for _, sink := range []bool{false, true} {
		t.Run(strconv.FormatBool(sink), func(t *testing.T) {
			t.Parallel()
			signer := signing.NewResponseSigner(make([]byte, 32))
			backend := NewMockBackend(gomock.NewController(t))
			target, body := "/v3/ledger1", `{}`
			var log *commonpb.Log
			if sink {
				target = "/v3/_/events-sinks"
				body = `{"name":"sink","http":{"endpoint":"https://example.com","secret":"private-credential"}}`
				config := &commonpb.SinkConfig{}
				require.NoError(t, protojson.Unmarshal([]byte(body), config))
				log = &commonpb.Log{Sequence: 12, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config}}}}
			} else {
				source := &commonpb.MirrorSourceConfig{}
				require.NoError(t, protojson.Unmarshal([]byte(`{"http":{"baseUrl":"https://user:private-credential@example.com"}}`), source))
				log = &commonpb.Log{Sequence: 12, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger1", Mode: commonpb.LedgerMode(1), MirrorSource: source}}}}
			}
			original := log.CloneVT()
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{log}}, nil)
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
			req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(httpreceipt.Header, "v1")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, 201, response.Code, response.Body.String())
			_, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), "POST", target, 201, response.Body.Bytes())
			require.NoError(t, err)
			require.NotContains(t, response.Body.String(), "private-credential")
			require.True(t, original.EqualVT(log))
			raw, err := base64.RawURLEncoding.DecodeString(response.Header().Get(httpreceipt.Header))
			require.NoError(t, err)
			var envelope struct{ Payload []byte }
			require.NoError(t, json.Unmarshal(raw, &envelope))
			require.NotContains(t, string(envelope.Payload), "private-credential")
			require.NotContains(t, string(envelope.Payload), "example.com")
		})
	}
}

func TestPublicReceiptSequenceGrouping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target, body, want string
		calls, logs              int
	}{
		{"empty", "/v3/ledger1/bulk", `[]`, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0, 0},
		{"one group", "/v3/ledger1/bulk?atomic=true", strings.TrimSuffix(bulkWriteBody, "]") + "," + strings.TrimPrefix(bulkWriteBody, "["), "eeac2a9ea5ad421211e86ae9ec2869c02d5709f7cccce01d9d92957ad10b13e2", 1, 2},
		{"two groups", "/v3/ledger1/bulk", strings.TrimSuffix(bulkWriteBody, "]") + "," + strings.TrimPrefix(bulkWriteBody, "["), "6f7dd53108d143af610c2471259cc38beab00d67f87fcfa61d161c1de7ddd630", 2, 1},
		{"zero logs", "/v3/ledger1/metadata", `{}`, "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			signer := signing.NewResponseSigner(make([]byte, 32))
			backend := NewMockBackend(gomock.NewController(t))
			if tc.calls > 0 {
				logs := make([]*commonpb.Log, tc.logs)
				for i := range logs {
					logs[i] = receiptTransactionLog(42)
				}
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: logs}, nil).Times(tc.calls)
			}
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(httpreceipt.Header, "v1")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Less(t, response.Code, 300, response.Body.String())
			claims, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), "POST", tc.target, response.Code, response.Body.Bytes())
			require.NoError(t, err)
			// Independent SHA-256 vectors over uint64 big-endian words: empty,
			// [2,91,91], [1,91,1,91], and [0]. Group boundaries must remain distinct.
			require.Equal(t, tc.want, claims.SequencesHash)
			require.Equal(t, uint64(tc.calls), claims.Applies)
			require.Equal(t, uint64(tc.calls*tc.logs), claims.Logs)
		})
	}
}

func TestPublicReceiptBigIntegerRepresentation(t *testing.T) {
	t.Parallel()
	for _, asString := range []bool{false, true} {
		t.Run(strconv.FormatBool(asString), func(t *testing.T) {
			t.Parallel()
			signer := signing.NewResponseSigner(make([]byte, 32))
			log := receiptTransactionLog(42)
			log.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().Postings = []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD", Amount: commonpb.NewUint256FromUint64(9007199254740993)}}
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{log}}, nil)
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil, signer)
			target := "/v3/ledger1/transactions"
			req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"postings":[{"source":"world","destination":"bank","amount":"9007199254740993","asset":"USD"}]}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(httpreceipt.Header, "v1")
			req.Header.Set(HeaderBigIntAsString, strconv.FormatBool(asString))
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, 201, response.Code, response.Body.String())
			want := `"amount":9007199254740993`
			if asString {
				want = `"amount":"9007199254740993"`
			}
			require.Contains(t, response.Body.String(), want)
			_, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), "POST", target, response.Code, response.Body.Bytes())
			require.NoError(t, err)
		})
	}
}

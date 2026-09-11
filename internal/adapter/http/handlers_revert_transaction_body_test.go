package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Exercise the registered route with both explicit lengths and actual HTTP/1.1
// chunked framing. The same expected Apply payload must survive every framing.
func TestHandleRevertTransaction_BodyFraming(t *testing.T) {
	t.Parallel()

	metadata, err := commonpb.MetadataFromAnyMap(map[string]any{
		"reason": "duplicate", "count": uint64(42), "negative": int64(-7), "active": true,
	})
	require.NoError(t, err)
	options := &servicepb.RevertTransactionPayload{
		TransactionId: 1, Force: true, AtEffectiveDate: true, Metadata: metadata,
	}
	defaults := &servicepb.RevertTransactionPayload{TransactionId: 1}
	controlMetadata, err := commonpb.MetadataFromAnyMap(map[string]any{"reason": "a\x00b"})
	require.NoError(t, err)
	escapedControl := &servicepb.RevertTransactionPayload{TransactionId: 1, Metadata: controlMetadata}

	for _, tc := range []struct {
		name   string
		body   string
		status int
		want   *servicepb.RevertTransactionPayload
	}{
		{"options", `{"force":true,"atEffectiveDate":true,"metadata":{"reason":"duplicate","count":42,"negative":-7,"active":true}}`, http.StatusCreated, options},
		{"empty", "", http.StatusCreated, defaults},
		{"empty object", `{}`, http.StatusCreated, defaults},
		{"raw control", "{\"metadata\":{\"reason\":\"a\x00b\"}}", http.StatusBadRequest, nil},
		{"escaped control", `{"metadata":{"reason":"a\u0000b"}}`, http.StatusCreated, escapedControl},
		{"trailing junk", `{"force":true}garbage`, http.StatusBadRequest, nil},
		{"second value", `{"force":true}{}`, http.StatusBadRequest, nil},
		{"truncated second value", `{"force":true}{`, http.StatusBadRequest, nil},
		{"trailing whitespace", "{} \t\r\n", http.StatusCreated, defaults},
		{"exact size with whitespace", `{}` + strings.Repeat(" ", int(defaultMaxBodySize)-2), http.StatusCreated, defaults},
		{"oversized whitespace", `{}` + strings.Repeat(" ", int(defaultMaxBodySize)-1), http.StatusRequestEntityTooLarge, nil},
		{"oversized trailing junk", `{}` + strings.Repeat("x", int(defaultMaxBodySize)-1), http.StatusRequestEntityTooLarge, nil},
		{"malformed", `{"force":!}`, http.StatusBadRequest, nil},
		{"truncated", `{"force":true`, http.StatusBadRequest, nil},
		{"oversized", `{"metadata":{"reason":"` + strings.Repeat("x", int(defaultMaxBodySize)) + `"}}`, http.StatusRequestEntityTooLarge, nil},
	} {
		for _, framing := range []string{"known length", "unknown length", "HTTP1 chunked"} {
			t.Run(tc.name+"/"+framing, func(t *testing.T) {
				t.Parallel()

				calls := make(chan *servicepb.ApplyRequest, 1)
				backend := NewMockBackend(gomock.NewController(t))
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
						calls <- req

						return &domain.ApplyResult{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
									RevertedTransaction: &commonpb.RevertedTransaction{RevertTransaction: &commonpb.Transaction{Id: 2}},
								},
							}}},
						}}}}}, nil
					}).MaxTimes(1)
				router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				path := APIVersionPrefix + "/ledger1/transactions/1/revert"
				var status int
				var responseBody []byte
				if framing == "HTTP1 chunked" {
					type wireFraming struct {
						protocol string
						length   int64
						encoding []string
					}
					wire := make(chan wireFraming, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						wire <- wireFraming{r.Proto, r.ContentLength, r.TransferEncoding}
						router.ServeHTTP(w, r)
					}))
					defer server.Close()
					req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+path, io.NopCloser(strings.NewReader(tc.body)))
					require.NoError(t, err)
					req.ContentLength = -1
					req.TransferEncoding = []string{"chunked"}
					resp, err := server.Client().Do(req)
					require.NoError(t, err)
					responseBody, err = io.ReadAll(resp.Body)
					require.NoError(t, resp.Body.Close())
					require.NoError(t, err)
					status = resp.StatusCode
					observed := <-wire
					require.Equal(t, "HTTP/1.1", observed.protocol)
					require.Equal(t, int64(-1), observed.length)
					require.Equal(t, []string{"chunked"}, observed.encoding)
				} else {
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
					if framing == "unknown length" {
						req.ContentLength = -1
					}
					w := httptest.NewRecorder()
					router.ServeHTTP(w, req)
					status, responseBody = w.Code, w.Body.Bytes()
				}

				require.Equal(t, tc.status, status, "%s", responseBody)
				if tc.want == nil {
					require.Empty(t, calls, "invalid bodies must not reach Apply")
					if tc.status == http.StatusRequestEntityTooLarge {
						require.Contains(t, string(responseBody), `"errorCode":"BODY_TOO_LARGE"`)
						require.Contains(t, string(responseBody), "http: request body too large")
					} else {
						require.Contains(t, string(responseBody), `"errorCode":"INVALID_REQUEST"`)
					}

					return
				}
				require.Len(t, calls, 1)
				req := <-calls
				require.Equal(t, "ledger1", req.GetUnsigned().GetRequests()[0].GetApply().GetLedger())
				got := revertPayloadFromApply(t, req)
				require.True(t, proto.Equal(tc.want, got), "expected %v, got %v", tc.want, got)
			})
		}
	}
}

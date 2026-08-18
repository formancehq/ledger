package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleExecutePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return &servicepb.ExecutePreparedQueryResponse{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute", strings.NewReader(`{"pageSize":10}`), map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleExecutePreparedQuery_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/prepared-queries/my-query/execute", nil, map[string]string{
		"ledgerName": "",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleExecutePreparedQuery_MissingQueryName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries//execute", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleExecutePreparedQuery_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute", strings.NewReader(`not-json`), map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})
	r.Header.Set("Content-Length", "8")

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleExecutePreparedQuery_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return nil, &domain.ErrPreparedQueryNotFound{Ledger: "ledger1", Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/missing/execute", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "missing",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleExecutePreparedQuery_NoBody(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return &servicepb.ExecutePreparedQueryResponse{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute?pageSize=5&cursor=abc", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleExecutePreparedQuery_WithParameters(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			require.NotNil(t, req.GetParameters())

			return &servicepb.ExecutePreparedQueryResponse{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"parameters":{"account":"alice","active":true,"amount":42},"mode":"AGGREGATE_VOLUMES"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleExecutePreparedQuery_ChunkedBody(t *testing.T) {
	t.Parallel()

	var capturedPageSize uint32

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			capturedPageSize = req.GetPageSize()

			return &servicepb.ExecutePreparedQueryResponse{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute", strings.NewReader(`{"pageSize":42}`), map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})
	// Simulate chunked transfer-encoding by clearing Content-Length and
	// flagging the request as chunked; ContentLength == -1 must not skip
	// body decoding.
	r.ContentLength = -1
	r.TransferEncoding = []string{"chunked"}

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, uint32(42), capturedPageSize)
}

func TestHandleExecutePreparedQuery_InvalidPageSizeQueryString(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute?pageSize=not-a-number", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "pageSize")
}

func TestHandleExecutePreparedQuery_UnknownMode(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"mode":"BOGUS"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "BOGUS")
}

// TestHandleExecutePreparedQuery_AggregateEmitsColor pins that the aggregate
// variant is serialized under the `aggregateResult` envelope key through the
// same camelCase DTO as the dedicated /aggregate handler: `color` is always
// present (including the uncolored bucket, as ""), amounts are decimal strings,
// and a balance is computed. The previous raw-proto serialization dropped
// `color` on uncolored rows (json:"color,omitempty") and leaked PascalCase
// oneof wrapper keys.
func TestHandleExecutePreparedQuery_AggregateEmitsColor(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{
					Aggregate: &commonpb.AggregateResult{
						Volumes: []*commonpb.AggregatedVolume{
							{Asset: "USD", Color: "", Input: commonpb.NewUint256FromUint64(100), Output: commonpb.NewUint256FromUint64(30)},
							{Asset: "USD", Color: "RED", Input: commonpb.NewUint256FromUint64(50), Output: commonpb.NewUint256FromUint64(0)},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"mode":"AGGREGATE_VOLUMES"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	resp := decodeResponse[executePreparedQueryResponseJSON](t, w)
	require.Nil(t, resp.Cursor, "aggregate response must not set the cursor variant")
	require.NotNil(t, resp.AggregateResult)
	vols := resp.AggregateResult.Volumes
	require.Len(t, vols, 2)

	// Uncolored bucket: color present as "" (not omitted), amounts as strings.
	require.Equal(t, "", vols[0].Color)
	require.Equal(t, "USD", vols[0].Asset)
	require.Equal(t, "100", vols[0].Input)
	require.Equal(t, "30", vols[0].Output)
	require.Equal(t, "70", vols[0].Balance)

	// Colored bucket carries its color verbatim.
	require.Equal(t, "RED", vols[1].Color)
	require.Equal(t, "50", vols[1].Balance)

	// The raw body must be the camelCase `aggregateResult` envelope, carry the
	// `color` key for the uncolored row, and must NOT leak the PascalCase oneof.
	body := w.Body.String()
	require.Contains(t, body, `"aggregateResult"`)
	require.Contains(t, body, `"color":""`)
	require.NotContains(t, body, `"Result"`)
	require.NotContains(t, body, `"Aggregate"`)
}

// TestHandleExecutePreparedQuery_CursorShapeIsCamelCase pins that the LIST /
// cursor variant is serialized under the `cursor` envelope key via
// PreparedQueryCursor.MarshalJSON, not wrapped in the PascalCase Go oneof
// envelope. The nested account volume row must carry camelCase keys,
// decimal-string amounts, and `color` present even for the uncolored bucket.
func TestHandleExecutePreparedQuery_CursorShapeIsCamelCase(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
					Cursor: &commonpb.PreparedQueryCursor{
						PageSize: 15,
						HasMore:  true,
						Next:     "nxt",
						AccountData: []*commonpb.Account{
							{Address: "alice", Volumes: []*commonpb.AccountVolume{
								{Asset: "USD", Color: "", Volumes: &commonpb.VolumesWithBalance{Input: "100", Output: "30", Balance: "70"}},
							}},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"pageSize":15}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	// No PascalCase Go oneof wrapper.
	require.NotContains(t, body, `"Result"`)
	require.NotContains(t, body, `"Cursor"`)
	// camelCase `cursor` envelope key, no `aggregateResult` for this variant.
	require.Contains(t, body, `"cursor"`)
	require.NotContains(t, body, `"aggregateResult"`)
	// camelCase cursor fields.
	require.Contains(t, body, `"pageSize":15`)
	require.Contains(t, body, `"hasMore":true`)
	require.Contains(t, body, `"accountData"`)
	// color is present on the uncolored account-volume row (not dropped).
	require.Contains(t, body, `"color":""`)
	// Amounts are decimal strings, not raw numbers.
	require.Contains(t, body, `"input":"100"`)
	require.Contains(t, body, `"balance":"70"`)
}

func TestHandleExecutePreparedQuery_UnsupportedParameterType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"parameters":{"bad":[1,2,3]}}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleExecutePreparedQuery_StringAmountWire pins both amount wires of the
// prepared-query execute route (EN-1779), one case per result the oneof can
// carry.
//
// The cursor covers three targets and only two of them carry amounts, so half
// this table asserts the response does NOT move: an ACCOUNTS page is
// byte-identical in both modes (nothing below Account renders an amount as a bare
// JSON number), and the aggregate branch must keep the `cursor` key omitted
// rather than gaining `"cursor":null` — which is what retyping the field to `any`
// would cause if the assignment were hoisted out of the cursor case, or if the
// nil guard in preparedQueryCursorValue were dropped.
//
// The LOGS target is the deepest chain on the HTTP surface: cursor → log →
// payload → apply → ledger log → payload → created transaction → transaction →
// posting → amount.
//
// This route writes through writeCheckedBody (ConfigStd), so `<`/`&` come out
// escaped and the body ends in a newline, in both modes.
//
// Amount assertions read the raw body bytes and never decode: the repository's
// sonic wrapper has no UseNumber, so decoding silently truncates 2^53+1.
func TestHandleExecutePreparedQuery_StringAmountWire(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		response *servicepb.ExecutePreparedQueryResponse
		// amountMoves marks the targets whose rendering the header may change.
		// The others assert the two bodies are equal instead.
		amountMoves   bool
		htmlSensitive bool
		// forbidden are substrings that must appear in neither mode's body.
		forbidden   []string
		defaultBody string
		optInBody   string
	}

	testCases := []testCase{
		{
			name: "accounts target has no amount to move",
			response: &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
					Cursor: &commonpb.PreparedQueryCursor{
						PageSize:    15,
						AccountData: []*commonpb.Account{{Address: "world<&>"}},
					},
				},
			},
			htmlSensitive: true,
			forbidden:     []string{`"transactionData"`, `"logData"`},
			defaultBody:   `{"cursor":{"pageSize":15,"hasMore":false,"accountData":[{"address":"world\u003c\u0026\u003e","volumes":[]}]}}` + "\n",
			optInBody:     `{"cursor":{"pageSize":15,"hasMore":false,"accountData":[{"address":"world\u003c\u0026\u003e","volumes":[]}]}}` + "\n",
		},
		{
			name: "transactions target quotes the posting amount",
			response: &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
					Cursor: &commonpb.PreparedQueryCursor{
						PageSize:        15,
						TransactionData: []*commonpb.Transaction{bigAmountTransaction(t, 1)},
					},
				},
			},
			amountMoves:   true,
			htmlSensitive: true,
			forbidden:     []string{`"logData"`, `"accountData"`},
			defaultBody:   `{"cursor":{"pageSize":15,"hasMore":false,"transactionData":[{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":9007199254740993,"asset":"USD/2","color":""}],"metadata":{},"id":1,"reverted":false}]}}` + "\n",
			optInBody:     `{"cursor":{"pageSize":15,"hasMore":false,"transactionData":[{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":"9007199254740993","asset":"USD/2","color":""}],"metadata":{},"id":1,"reverted":false}]}}` + "\n",
		},
		{
			name: "logs target quotes the amount at the bottom of the log chain",
			response: &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Cursor{
					Cursor: &commonpb.PreparedQueryCursor{
						PageSize: 15,
						LogData:  []*commonpb.Log{bigAmountLog(t, 1)},
					},
				},
			},
			amountMoves:   true,
			htmlSensitive: true,
			forbidden:     []string{`"transactionData"`, `"accountData"`},
			defaultBody:   `{"cursor":{"pageSize":15,"hasMore":false,"logData":[{"sequence":1,"payload":{"apply":{"ledgerName":"ledger1","log":{"type":"NEW_TRANSACTION","data":{"createdTransaction":{"transaction":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":9007199254740993,"asset":"USD/2","color":""}],"metadata":{},"id":1,"reverted":false}}},"id":1}}},"responseSignature":{}}]}}` + "\n",
			optInBody:     `{"cursor":{"pageSize":15,"hasMore":false,"logData":[{"sequence":1,"payload":{"apply":{"ledgerName":"ledger1","log":{"type":"NEW_TRANSACTION","data":{"createdTransaction":{"transaction":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":"9007199254740993","asset":"USD/2","color":""}],"metadata":{},"id":1,"reverted":false}}},"id":1}}},"responseSignature":{}}]}}` + "\n",
		},
		{
			name: "aggregate branch never gains a cursor key",
			response: &servicepb.ExecutePreparedQueryResponse{
				Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{
					Aggregate: &commonpb.AggregateResult{
						Volumes: []*commonpb.AggregatedVolume{
							{
								Asset:  "USD",
								Input:  commonpb.NewUint256FromUint64(100),
								Output: commonpb.NewUint256FromUint64(30),
							},
						},
					},
				},
			},
			forbidden:   []string{`"cursor"`},
			defaultBody: `{"aggregateResult":{"volumes":[{"asset":"USD","color":"","input":"100","output":"30","balance":"70"}]}}` + "\n",
			optInBody:   `{"aggregateResult":{"volumes":[{"asset":"USD","color":"","input":"100","output":"30","balance":"70"}]}}` + "\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.amountMoves {
				requireOnlyAmountQuotingDiffers(t, tc.defaultBody, tc.optInBody)
			} else {
				require.Equal(t, tc.defaultBody, tc.optInBody,
					"a result that carries no amount must be byte-identical in both modes")
			}

			modes := []struct {
				name        string
				headerValue string // empty means the header is not sent at all
				wantBody    string
			}{
				{name: "default wire", wantBody: tc.defaultBody},
				{name: "opt-in wire", headerValue: "true", wantBody: tc.optInBody},
			}

			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					t.Parallel()

					backend := NewMockBackend(gomock.NewController(t))
					backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
							return tc.response, nil
						}).Times(1)
					srv := newTestServer(t, backend)

					w := httptest.NewRecorder()
					r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
						strings.NewReader(`{"pageSize":15}`), map[string]string{
							"ledgerName": "ledger1",
							"queryName":  "my-query",
						})

					if mode.headerValue != "" {
						r.Header.Set("Formance-Bigint-As-String", mode.headerValue)
					}

					srv.handleExecutePreparedQuery(w, r)

					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					require.Equal(t, mode.wantBody, w.Body.String())

					for _, forbidden := range tc.forbidden {
						require.NotContains(t, w.Body.String(), forbidden)
					}

					// The route stays on ConfigStd in both modes:
					// writeCheckedBody is byte-identical to the
					// writeJSONResponse it replaced.
					require.True(t, strings.HasSuffix(w.Body.String(), "\n"),
						"the prepared-query route writes through ConfigStd, which appends a trailing newline")

					if tc.htmlSensitive {
						require.NotContains(t, w.Body.String(), "<",
							"the prepared-query route writes through ConfigStd, which HTML-escapes")
					}
				})
			}
		})
	}
}

// TestPreparedQueryCursorValue covers the guard that keeps the retyped `cursor`
// field omitted rather than rendering `"cursor":null`. Once the field is typed
// `any`, a typed nil pointer stored in it is a non-nil interface and `omitempty`
// stops firing, so the emptiness test lives in this helper instead of the encoder.
func TestPreparedQueryCursorValue(t *testing.T) {
	t.Parallel()

	require.Nil(t, preparedQueryCursorValue(nil, false),
		"a nil cursor must produce a nil interface so omitempty still drops the key")
	require.Nil(t, preparedQueryCursorValue(nil, true),
		"the opt-in mode must not turn an absent cursor into a rendered null")

	cursor := &commonpb.PreparedQueryCursor{PageSize: 15}
	require.Equal(t, cursor, preparedQueryCursorValue(cursor, false))
	require.Equal(t,
		commonpb.StringAmountPreparedQueryCursor{PreparedQueryCursor: cursor},
		preparedQueryCursorValue(cursor, true))
}

package http

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

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
// present (including the uncolored bucket, as ""), amounts are numeric by default,
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

	resp := decodeResponse[struct {
		Cursor          *commonpb.PreparedQueryCursor `json:"cursor"`
		AggregateResult *aggregateVolumesWireJSON     `json:"aggregateResult"`
	}](t, w)
	require.Nil(t, resp.Cursor, "aggregate response must not set the cursor variant")
	require.NotNil(t, resp.AggregateResult)
	vols := resp.AggregateResult.Volumes
	require.Len(t, vols, 2)

	// Uncolored bucket: color present as "" (not omitted), amounts as numbers.
	require.Equal(t, "", vols[0].Color)
	require.Equal(t, "USD", vols[0].Asset)
	require.Equal(t, "100", string(vols[0].Input))
	require.Equal(t, "30", string(vols[0].Output))
	require.Equal(t, "70", string(vols[0].Balance))

	// Colored bucket carries its color verbatim.
	require.Equal(t, "RED", vols[1].Color)
	require.Equal(t, "50", string(vols[1].Balance))

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
// numeric amounts, and `color` present even for the uncolored bucket.
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
								{Asset: "USD", Color: "", Volumes: &commonpb.VolumesWithBalance{Input: commonpb.MustBigUintFromDecimal("100"), Output: commonpb.MustBigUintFromDecimal("30"), Balance: commonpb.MustSignedBigIntFromDecimal("70")}},
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
	// Default monetary values are JSON number tokens, matching v2.
	require.Contains(t, body, `"input":100`)
	require.Contains(t, body, `"balance":70`)
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

func TestHandleExecutePreparedQuery_Reverse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		query string
		body  string
		want  bool
	}{
		{name: "default", body: `{}`, want: false},
		{name: "body", body: `{"reverse":true}`, want: true},
		{name: "query string", query: "?reverse=true", body: `{}`, want: true},
		{name: "query string overrides body", query: "?reverse=false", body: `{"reverse":true}`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var captured *servicepb.ExecutePreparedQueryRequest

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
					captured = req

					return &servicepb.ExecutePreparedQueryResponse{}, nil
				})
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute"+tc.query, strings.NewReader(tc.body), map[string]string{
				"ledgerName": "ledger1",
				"queryName":  "my-query",
			})

			srv.handleExecutePreparedQuery(w, r)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tc.want, captured.GetReverse())
		})
	}
}

// TestHandleExecutePreparedQuery_RejectsAlteredParameters pins that the HTTP
// decoder never forwards a value different from the one the caller sent: a
// JSON null used to decode as "" and an integer outside the int64/uint64
// range used to be cast from float64, so the query ran on another value and
// answered 200 with an empty page.
func TestHandleExecutePreparedQuery_RejectsAlteredParameters(t *testing.T) {
	t.Parallel()

	for name, params := range map[string]string{
		"null":           `{"min":null}`,
		"above uint64":   `{"min":18446744073709551616}`,
		"below int64":    `{"min":-9223372036854775809}`,
		"exponent float": `{"min":1.5e0}`,
		"fraction":       `{"min":1.5}`,
		"huge exponent":  `{"min":1e1000000000}`,
		"tiny fraction":  `{"min":18446744073709551615.0000000000000000000001}`,
		// big.Float rounded these to an exact zero and forwarded Uint64Value(0).
		"underflow":          `{"min":1e-1000000000}`,
		"negative underflow": `{"min":-1e-1000000000}`,
		"huge digit string":  `{"min":1` + strings.Repeat("0", 4096) + `}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// No EXPECT: reaching the backend fails the test.
			srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
				strings.NewReader(`{"parameters":`+params+`}`),
				map[string]string{
					"ledgerName": "ledger1",
					"queryName":  "my-query",
				})

			srv.handleExecutePreparedQuery(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), `parameter \"min\"`)
		})
	}
}

// TestHandleExecutePreparedQuery_ForwardsExactIntegers pins that integers are
// forwarded exactly, including values above 2^53 that a float64 cannot hold.
func TestHandleExecutePreparedQuery_ForwardsExactIntegers(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]*commonpb.ParameterValue{
		`18446744073709551615`:     {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 18446744073709551615}},
		`9007199254740993`:         {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 9007199254740993}},
		`-9223372036854775808`:     {Value: &commonpb.ParameterValue_Int64Value{Int64Value: -9223372036854775808}},
		`-9007199254740993`:        {Value: &commonpb.ParameterValue_Int64Value{Int64Value: -9007199254740993}},
		`0`:                        {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 0}},
		`1e3`:                      {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 1000}},
		`-42.0`:                    {Value: &commonpb.ParameterValue_Int64Value{Int64Value: -42}},
		`1e19`:                     {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 10000000000000000000}},
		`1.0e2`:                    {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 100}},
		`10e-1`:                    {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 1}},
		`-9.223372036854775808e18`: {Value: &commonpb.ParameterValue_Int64Value{Int64Value: math.MinInt64}},
		`0e-1000000000`:            {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 0}},
		`true`:                     {Value: &commonpb.ParameterValue_BoolValue{BoolValue: true}},
		`""`:                       {Value: &commonpb.ParameterValue_StringValue{StringValue: ""}},
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
					require.True(t, proto.Equal(want, req.GetParameters()["min"]), "got %v", req.GetParameters()["min"])

					return &servicepb.ExecutePreparedQueryResponse{}, nil
				})
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
				strings.NewReader(`{"parameters":{"min":`+raw+`}}`),
				map[string]string{
					"ledgerName": "ledger1",
					"queryName":  "my-query",
				})

			srv.handleExecutePreparedQuery(w, r)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		})
	}
}

// TestHandleExecutePreparedQuery_ForwardsLongIntegralTokens pins the HTTP
// path for long numeric tokens: a digit string whose decimal shift leaves an
// integer is forwarded exactly, and one that does not fit uint64 is refused.
func TestHandleExecutePreparedQuery_ForwardsLongIntegralTokens(t *testing.T) {
	t.Parallel()

	const digits = 64 << 10

	for name, tc := range map[string]struct {
		raw  string
		want *commonpb.ParameterValue
	}{
		"integral after shift": {
			raw:  "1" + strings.Repeat("0", digits) + "e-" + strconv.Itoa(digits),
			want: &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 1}},
		},
		"out of range": {
			raw: "1" + strings.Repeat("0", digits),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			if tc.want != nil {
				backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
						require.True(t, proto.Equal(tc.want, req.GetParameters()["min"]), "got %v", req.GetParameters()["min"])

						return &servicepb.ExecutePreparedQueryResponse{}, nil
					})
			}
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
				strings.NewReader(`{"parameters":{"min":`+tc.raw+`}}`),
				map[string]string{
					"ledgerName": "ledger1",
					"queryName":  "my-query",
				})

			srv.handleExecutePreparedQuery(w, r)

			if tc.want != nil {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			} else {
				require.Equal(t, http.StatusBadRequest, w.Code)
				require.Contains(t, w.Body.String(), "is out of range")
			}
		})
	}
}

// TestJSONNumberToParameterValue_BoundsWork pins that numeric conversion is
// linear in the token length. big.ParseFloat builds the full mantissa before
// rounding, which is quadratic in the digit count: these 4 MiB tokens took
// 8-12s each before the fix. The lexical normalizer handles them in a few
// milliseconds (well under 1s with -race), so the bound leaves a wide margin
// either way. The conversion is called directly so that HTTP body decoding,
// which is linear but slow under -race and parallel load, is not measured.
func TestJSONNumberToParameterValue_BoundsWork(t *testing.T) {
	t.Parallel()

	const digits = 4 << 20

	for name, tc := range map[string]struct {
		raw     string
		want    *commonpb.ParameterValue
		wantErr string
	}{
		"integral after shift": {
			raw:  "1" + strings.Repeat("0", digits) + "e-" + strconv.Itoa(digits),
			want: &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 1}},
		},
		"out of range": {
			raw:     "1" + strings.Repeat("0", digits),
			wantErr: "is out of range",
		},
		"fraction": {
			raw:     "1." + strings.Repeat("0", digits) + "1",
			wantErr: "floating-point values are not supported",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			start := time.Now()
			got, err := jsonNumberToParameterValue(json.Number(tc.raw))
			elapsed := time.Since(start)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.True(t, proto.Equal(tc.want, got), "got %v", got)
			}
			require.Less(t, elapsed, 2*time.Second, "numeric parameter conversion must stay linear in the token length")
		})
	}
}

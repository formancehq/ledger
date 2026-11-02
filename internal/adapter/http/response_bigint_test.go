package http

import (
	"encoding/json/jsontext"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// bigIntegerResult exercises a custom public projection with nested collections.
type bigIntegerResult struct{ Value any }

func (v bigIntegerResult) MarshalJSONTo(enc *jsontext.Encoder) error {
	return json.MarshalEncode(enc, map[string]any{"results": []any{v.Value}})
}

func TestResponseBigIntegerEncoding(t *testing.T) {
	t.Parallel()
	// These fields represent arbitrary nonmonetary integer results. Their types,
	// rather than their names or containing message, determine response encoding.
	value := struct {
		Limit *commonpb.Uint256      `json:"limit"`
		Total *commonpb.BigUint      `json:"total"`
		Delta *commonpb.SignedBigInt `json:"delta"`
		Count uint64                 `json:"count"`
	}{
		Limit: commonpb.NewUint256FromUint64(9007199254740993),
		Total: commonpb.MustBigUintFromDecimal("9007199254740994"),
		Delta: commonpb.MustSignedBigIntFromDecimal("-9007199254740995"),
		Count: 42,
	}
	for name, write := range map[string]func(http.ResponseWriter, *http.Request, any){
		"checked":   writeOKChecked,
		"streaming": writeOK,
		"created":   writeCreated,
		"page": func(w http.ResponseWriter, r *http.Request, value any) {
			writePageOK(w, r, value, pageLinks{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct{ name, header, want string }{
				{"numbers", "", `{"data":{"limit":9007199254740993,"total":9007199254740994,"delta":-9007199254740995,"count":42}}`},
				{"strings", "true", `{"data":{"limit":"9007199254740993","total":"9007199254740994","delta":"-9007199254740995","count":42}}`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					r := httptest.NewRequest(http.MethodGet, "/", nil)
					r.Header.Set(HeaderBigIntAsString, tc.header)
					w := httptest.NewRecorder()
					write(w, r, value)
					status := http.StatusOK
					if name == "created" {
						status = http.StatusCreated
					}
					require.Equal(t, status, w.Code)
					want := tc.want
					if name == "streaming" || name == "created" {
						want += "\n"
					}
					if name == "page" {
						want = want[:len(want)-1] + `,"hasMore":false}`
					}
					require.Equal(t, want, w.Body.String())
				})
			}
		})
	}
}

func TestResponseBigIntegerEncodingConcurrentNested(t *testing.T) {
	t.Parallel()
	payload := bigIntegerResult{Value: map[string]any{
		"limit": commonpb.NewUint256FromUint64(9007199254740993),
		"total": commonpb.MustBigUintFromDecimal("9007199254740994"),
		"delta": commonpb.MustSignedBigIntFromDecimal("-9007199254740995"),
		"count": uint64(42),
	}}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 32 {
		wg.Go(func() {
			<-start
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			want := `{"data":{"results":[{"count":42,"delta":-9007199254740995,"limit":9007199254740993,"total":9007199254740994}]}}`
			if i%2 == 0 {
				r.Header.Set(HeaderBigIntAsString, "true")
				want = `{"data":{"results":[{"count":42,"delta":"-9007199254740995","limit":"9007199254740993","total":"9007199254740994"}]}}`
			}
			w := httptest.NewRecorder()
			writeOKChecked(w, r, payload)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.JSONEq(t, want, w.Body.String())
		})
	}
	close(start)
	wg.Wait()
}

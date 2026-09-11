package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Exercise NewHandler so URL.Path/RawPath and chi's route segmentation are real.
// The mock captures boundary identity; service-backed tests cover admission and
// persisted state, including rejection of percent-containing keys.
func TestMetadataKeyRouting(t *testing.T) {
	t.Parallel()
	routes := []struct {
		name, method, path string
		key                func(*servicepb.Request) string
	}{
		{"account", http.MethodDelete, "/accounts/users:001/metadata/", func(r *servicepb.Request) string { return r.GetApply().GetAction().GetDeleteMetadata().GetKey() }},
		{"transaction", http.MethodDelete, "/transactions/0/metadata/", func(r *servicepb.Request) string { return r.GetApply().GetAction().GetDeleteMetadata().GetKey() }},
		{"ledger", http.MethodDelete, "/metadata/", func(r *servicepb.Request) string { return r.GetDeleteLedgerMetadata().GetKey() }},
		{"schema_put", http.MethodPut, "/metadata-schema/account/", func(r *servicepb.Request) string { return r.GetSetMetadataFieldType().GetKey() }},
		{"schema_delete", http.MethodDelete, "/metadata-schema/account/", func(r *servicepb.Request) string { return r.GetRemoveMetadataFieldType().GetKey() }},
	}
	keys := []struct {
		name, encoded, decoded string
		rawPath                bool
	}{
		{"plain", "reviewed", "reviewed", false},
		{"slash_upper", "formance.com%2Freviewed", "formance.com/reviewed", true},
		{"slash_lower", "formance.com%2freviewed", "formance.com/reviewed", true},
		{"escaped_letter", "%72eviewed", "reviewed", true},
		{"colon", "formance.com%3Areviewed", "formance.com:reviewed", true},
		{"double_encoded_path", "formance.com%252Freviewed", "formance.com%2Freviewed", false},
		{"double_encoded_raw_path", "%66ormance.com%252Freviewed", "formance.com%2Freviewed", true},
		{"percent", "reviewed%25", "reviewed%", false},
		{"invalid_escape_literal", "reviewed%25ZZ", "reviewed%ZZ", false},
		{"plus_is_not_space", "reviewed+flag", "reviewed+flag", false},
	}
	for _, route := range routes {
		for _, key := range keys {
			t.Run(route.name+"/"+key.name, func(t *testing.T) {
				t.Parallel()
				var captured *servicepb.Request
				backend := NewMockBackend(gomock.NewController(t))
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
					require.Len(t, req.GetUnsigned().GetRequests(), 1)
					captured = req.GetUnsigned().GetRequests()[0]

					return []*commonpb.Log{{}}, nil
				})
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				req := httptest.NewRequest(route.method, "/v3/ledger1"+route.path+key.encoded, strings.NewReader(`{"type":"string"}`))
				require.Equal(t, key.rawPath, req.URL.RawPath != "")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
				require.NotNil(t, captured)
				require.Equal(t, key.decoded, route.key(captured))
			})
		}
	}
}

func TestCanonicalIDRoutingSingleDecode(t *testing.T) {
	t.Parallel()
	for _, scope := range []string{"ledger1", "_"} {
		for _, key := range []struct{ encoded, decoded string }{
			{"formance.com%2Freviewed", "formance.com/reviewed"},
			{"formance.com%252Freviewed", "formance.com%2Freviewed"},
			{"%66ormance.com%252Freviewed", "formance.com%2Freviewed"},
		} {
			t.Run(scope+"/"+key.encoded, func(t *testing.T) {
				t.Parallel()
				var captured *servicepb.GetIndexRequest
				backend := NewMockBackend(gomock.NewController(t))
				backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
					captured = req

					return &commonpb.Index{Ledger: req.GetLedger()}, nil
				})
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				req := httptest.NewRequest(http.MethodGet, "/v3/"+scope+"/indexes/metadata:TARGET_TYPE_ACCOUNT:"+key.encoded, nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.NotNil(t, captured)
				require.Equal(t, key.decoded, captured.GetId().GetMetadata().GetKey())
			})
		}
	}
}

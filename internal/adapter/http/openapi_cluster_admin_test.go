package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// TestOpenAPI_ClusterAdmin validates each cluster administration request and
// its success and documented error responses against openapi.yml.
func TestOpenAPI_ClusterAdmin(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)

	const maxNodeID = "18446744073709551615"
	for _, tc := range []struct {
		name, method, path, body string
		expect                   func(*MockBackend)
		status                   int
	}{
		{"transfer", http.MethodPost, "/v3/_/cluster/leadership/transfer", `{"transferee":"` + maxNodeID + `"}`, func(b *MockBackend) {
			b.EXPECT().TransferLeadership(gomock.Any(), gomock.Any()).Return(&clusterpb.TransferLeadershipResponse{NewLeader: 18446744073709551615}, nil)
		}, http.StatusOK},
		{"transfer unknown", http.MethodPost, "/v3/_/cluster/leadership/transfer", `{"transferee":2}`, func(b *MockBackend) {
			b.EXPECT().TransferLeadership(gomock.Any(), gomock.Any()).Return(nil, node.ErrUnknownTransferee)
		}, http.StatusConflict},
		{"add learner", http.MethodPost, "/v3/_/cluster/learners", `{"nodeId":2,"raftAddress":"r:1","serviceAddress":"s:1","instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e","capabilities":[1]}`, func(b *MockBackend) {
			b.EXPECT().AddLearner(gomock.Any(), gomock.Any()).Return(&clusterpb.AddLearnerResponse{}, nil)
		}, http.StatusNoContent},
		{"promote", http.MethodPost, "/v3/_/cluster/learners/" + maxNodeID + "/promote", "", func(b *MockBackend) {
			b.EXPECT().PromoteLearner(gomock.Any(), gomock.Any()).Return(&clusterpb.PromoteLearnerResponse{}, nil)
		}, http.StatusNoContent},
		{"remove", http.MethodDelete, "/v3/_/cluster/nodes/2?force=true", "", func(b *MockBackend) {
			b.EXPECT().RemoveNode(gomock.Any(), gomock.Any()).Return(&clusterpb.RemoveNodeResponse{}, nil)
		}, http.StatusNoContent},
		{"remove absent", http.MethodDelete, "/v3/_/cluster/nodes/2", "", func(b *MockBackend) {
			b.EXPECT().RemoveNode(gomock.Any(), gomock.Any()).Return(nil, node.ErrNodeNotInCluster)
		}, http.StatusNotFound},
		{"remove committed", http.MethodDelete, "/v3/_/cluster/nodes/2", "", func(b *MockBackend) {
			b.EXPECT().RemoveNode(gomock.Any(), gomock.Any()).Return(nil, &node.RemoveNodeCommittedError{NodeID: 2, CommittedIndex: 9})
		}, http.StatusServiceUnavailable},
		{"drain status", http.MethodGet, "/v3/_/cluster/drain", "", func(b *MockBackend) {
			b.EXPECT().GetDrainStatus(gomock.Any(), gomock.Any()).Return(&clusterpb.DrainStatus{LocalNode: 1, SupportsDrain: true, SupportsRelease: true}, nil)
		}, http.StatusOK},
		{"begin drain", http.MethodPost, "/v3/_/cluster/drain", `{"epoch":"e1"}`, func(b *MockBackend) {
			b.EXPECT().BeginDrain(gomock.Any(), gomock.Any()).Return(&clusterpb.BeginDrainResponse{CutoffIndex: 7, Members: []uint64{1, 2}}, nil)
		}, http.StatusOK},
		{"end drain", http.MethodPost, "/v3/_/cluster/drain/end", `{"epoch":"e1"}`, func(b *MockBackend) {
			b.EXPECT().EndDrain(gomock.Any(), gomock.Any()).Return(&clusterpb.EndDrainResponse{ReleaseIndex: 8}, nil)
		}, http.StatusOK},
		{"compact primary", http.MethodPost, "/v3/_/cluster/compactions/primary", "", func(b *MockBackend) {
			b.EXPECT().CompactPrimary(gomock.Any(), gomock.Any()).Return(&clusterpb.CompactPrimaryResponse{DurationMs: 3}, nil)
		}, http.StatusOK},
		{"compact secondary", http.MethodPost, "/v3/_/cluster/compactions/secondary", "", func(b *MockBackend) {
			b.EXPECT().CompactSecondary(gomock.Any(), gomock.Any()).Return(&clusterpb.CompactSecondaryResponse{DurationMs: 3, SizeBeforeBytes: 2, SizeAfterBytes: 1}, nil)
		}, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			newRequest := func() *http.Request {
				r := httptest.NewRequest(tc.method, "http://localhost:9000"+tc.path, strings.NewReader(tc.body))
				r.Header.Set("Content-Type", "application/json")

				return r
			}
			validateOpenAPIRequest(t, doc, newRequest())

			backend := NewMockBackend(gomock.NewController(t))
			tc.expect(backend)
			r := newRequest()
			w := httptest.NewRecorder()
			NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil).ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			validateOpenAPIResponse(t, doc, r, w)
		})
	}
}

func validateOpenAPIRequest(t *testing.T, doc *openapi3.T, r *http.Request) {
	t.Helper()
	router, err := legacy.NewRouter(doc, openapi3.AllowExtraSiblingFields("description"))
	require.NoError(t, err)
	route, params, err := router.FindRoute(r)
	require.NoError(t, err)
	require.NoError(t, openapi3filter.ValidateRequest(context.Background(), &openapi3filter.RequestValidationInput{
		Request: r, PathParams: params, Route: route,
		Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
	}))
}

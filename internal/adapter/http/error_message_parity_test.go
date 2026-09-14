package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestErrorMessageParityThroughRoutingWrappers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     domain.Describable
		status  int
		message string
	}{
		{"metadata", &domain.ErrMetadataFieldNotInSchema{Target: "TARGET_TYPE_ACCOUNT", Key: "never-declared"}, http.StatusBadRequest, "metadata field not declared in schema: TARGET_TYPE_ACCOUNT/never-declared"},
		{"not found", &domain.ErrLedgerNotFound{Name: "test"}, http.StatusNotFound, "ledger does not exist: test"},
		{"public internal", &domain.ErrIndexInconsistent{Index: "private-index", Detail: "private-storage-failure"}, http.StatusInternalServerError, "index is inconsistent"},
		{"numscript", &domain.ErrNumscriptRuntime{Detail: "negative posting amount"}, http.StatusInternalServerError, "numscript runtime error: negative posting amount"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			message, metadata, _ := domain.PublicErrorDetails(tc.err)
			wire := leaderStatusWithMetadata(t, grpcerr.CodeForKind(domain.Kind(tc.err)), message, tc.err.Reason(), metadata)
			for _, origin := range []struct {
				name string
				err  error
			}{
				{"local", &domain.BusinessError{Err: tc.err}},
				{"forwarded", grpcerr.FromStatusError(wire)},
			} {
				t.Run(origin.name, func(t *testing.T) {
					t.Parallel()
					wrapped := fmt.Errorf("applying raft requests: %w", origin.err)
					diagnostic := wrapped.Error()
					t.Run("unitary", func(t *testing.T) {
						w := httptest.NewRecorder()
						handleError(w, httptest.NewRequest(http.MethodPost, "/", nil), wrapped)
						require.Equal(t, tc.status, w.Code)
						response := decodeResponse[ErrorResponse](t, w)
						require.Equal(t, tc.err.Reason(), response.ErrorCode)
						require.Equal(t, tc.message, response.ErrorMessage)
					})

					t.Run("bulk", func(t *testing.T) {
						elements := []*servicepb.BulkElement{{Action: &servicepb.LedgerAction{
							Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{}},
						}}}
						w := httptest.NewRecorder()
						writeBulkResponse(w, testBulkRequest(), elements, []bulkResult{{err: wrapped}}, false)
						require.Equal(t, tc.status, w.Code)
						bulk := decodeResponse[bulkResponse](t, w)
						require.Len(t, bulk.Data, 1)
						require.Equal(t, "ERROR", bulk.Data[0].ResponseType)
						require.Equal(t, tc.err.Reason(), bulk.Data[0].ErrorCode)
						require.Equal(t, tc.message, bulk.Data[0].ErrorDescription)
					})
					require.Equal(t, diagnostic, wrapped.Error(), "rendering must preserve the diagnostic error")
				})
			}
		})
	}
}

package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestHandleErrorMissingSignature is the regression for EN-2930 (Heal
// s8-index-create-unsigned-server-error-1). The HTTP API never signs, so while
// signatures are required admission refuses every REST write with
// signing.ErrMissingSignature. Both provenances used to answer 500
// INTERNAL_ERROR: the leader's local error was an unclassified sentinel, and a
// follower received a bare Unauthenticated status that handleError rightly
// refuses to trust. The refusal must reach the caller as 401
// MISSING_SIGNATURE on both paths, for unitary and bulk writes.
func TestHandleErrorMissingSignature(t *testing.T) {
	t.Parallel()

	for _, origin := range []struct {
		name string
		err  error
	}{
		{"local", fmt.Errorf("applying raft requests: %w", signing.ErrMissingSignature)},
		{"forwarded", fmt.Errorf("applying raft requests: %w", grpcerr.FromStatusError(
			leaderStatus(t, codes.Unauthenticated, "missing signature", domain.ErrReasonMissingSignature)))},
	} {
		t.Run(origin.name, func(t *testing.T) {
			t.Parallel()

			t.Run("unitary", func(t *testing.T) {
				t.Parallel()

				w := httptest.NewRecorder()
				handleError(w, httptest.NewRequest(http.MethodPost, "/", nil), origin.err)

				require.Equal(t, http.StatusUnauthorized, w.Code)
				resp := decodeResponse[ErrorResponse](t, w)
				require.Equal(t, domain.ErrReasonMissingSignature, resp.ErrorCode)
				require.Equal(t, "missing signature", resp.ErrorMessage)
			})

			t.Run("bulk", func(t *testing.T) {
				t.Parallel()

				elements := []*servicepb.BulkElement{{Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{}},
				}}}
				w := httptest.NewRecorder()
				writeBulkResponse(w, testBulkRequest(), elements, []bulkResult{{err: origin.err}}, false)

				require.Equal(t, http.StatusUnauthorized, w.Code)
				bulk := decodeResponse[bulkResponse](t, w)
				require.Len(t, bulk.Data, 1)
				require.Equal(t, domain.ErrReasonMissingSignature, bulk.Data[0].ErrorCode)
				require.Equal(t, "missing signature", bulk.Data[0].ErrorDescription)
			})
		})
	}
}

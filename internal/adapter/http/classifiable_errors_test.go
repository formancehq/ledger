package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// TestHandleError_ClassifiableWithoutReason pins the HTTP half of the EN-2081
// classification-only tier. apierr.Describe cannot normalise these errors —
// there is no Reason to normalise — so without the Classifiable branch a
// caller mistake would degrade to a 500 while the gRPC surface answered 400 for
// the identical failure.
//
// The errorCode is the coarse, kind-level one this handler already uses for a
// reason-less classified status, not a reason invented from the Go type.
func TestHandleError_ClassifiableWithoutReason(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "aggregate on a non-accounts target",
			err:  &query.ErrPreparedQueryAggregateTarget{Target: commonpb.QueryTarget_QUERY_TARGET_LOGS},
		},
		{
			name: "unsupported query mode",
			err:  &query.ErrQueryModeUnsupported{Mode: commonpb.QueryMode(99)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			handleError(w, httptest.NewRequest(http.MethodPost, "/", nil), tc.err)

			require.Equal(t, http.StatusBadRequest, w.Code)

			var body ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, "INVALID_REQUEST", body.ErrorCode)
			require.Equal(t, tc.err.Error(), body.ErrorMessage)
		})
	}
}

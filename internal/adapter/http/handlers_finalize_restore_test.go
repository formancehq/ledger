package http

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func TestHandleFinalizeRestore(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().FinalizeRestore(gomock.Any(), gomock.Any()).Return(&restorepb.FinalizeRestoreResponse{Message: "Restore finalized (index=42)."}, nil)
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/finalize", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"message":"Restore finalized (index=42)."}}`, w.Body.String())
}

func TestHandleFinalizeRestore_Errors(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err     error
		status  int
		code    string
		message string
	}{
		"not validated":     {status.Error(codes.FailedPrecondition, "staged backup has not passed validation"), http.StatusConflict, "FAILED_PRECONDITION", "staged backup has not passed validation"},
		"validation active": {status.Error(codes.FailedPrecondition, "restore validation is still running"), http.StatusConflict, "FAILED_PRECONDITION", "restore validation is still running"},
		"concurrent":        {status.Error(codes.FailedPrecondition, "restore finalization is already running"), http.StatusConflict, "FAILED_PRECONDITION", "restore finalization is already running"},
		"failure":           {errors.New("hard linking staging to checkpoint: /data/staging: permission denied"), http.StatusInternalServerError, "INTERNAL_ERROR", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockRestoreBackend(gomock.NewController(t))
			backend.EXPECT().FinalizeRestore(gomock.Any(), gomock.Any()).Return(nil, tc.err)
			w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/finalize", "")
			require.Equal(t, tc.status, w.Code, w.Body.String())
			resp := decodeErrorResponse(t, w)
			require.Equal(t, tc.code, resp.ErrorCode)
			if tc.message != "" {
				require.Equal(t, tc.message, resp.ErrorMessage)
			} else {
				require.NotContains(t, resp.ErrorMessage, "/data/staging")
			}
		})
	}
}

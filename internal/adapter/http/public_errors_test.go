package http

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
)

func TestHTTPPublicInternalDetails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     domain.Describable
		message string
	}{
		{"index", &domain.ErrIndexInconsistent{Index: "private-index", Detail: "reading /private/pebble: secret failure"}, "index is inconsistent"},
		{"coverage", &state.ErrCoverageMiss{Attribute: "private-attribute", CanonicalHex: "deadbeef", IDHex: "0102", RaftIndex: 42}, "preload coverage miss"},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", tc.name, wrapped), func(t *testing.T) {
				t.Parallel()
				var err error = tc.err
				if wrapped {
					err = fmt.Errorf("private wrapper: %w", &domain.BusinessError{Err: tc.err})
				}
				var logs bytes.Buffer
				ctx := logging.ContextWithLogger(context.Background(), logging.NewDefaultLogger(&logs, false, false, false))
				w := httptest.NewRecorder()
				handleError(w, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), err)
				require.Equal(t, http.StatusInternalServerError, w.Code)
				response := decodeResponse[ErrorResponse](t, w)
				require.Equal(t, tc.err.Reason(), response.ErrorCode)
				require.Equal(t, tc.message, response.ErrorMessage)
				require.Contains(t, logs.String(), tc.err.Error())
				require.Contains(t, logs.String(), "correlation_id")
			})
		}
	}
}

func TestHTTPPublicDetailsPreserveOtherWrappedErrors(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("existing context: %w", &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: "test"}})
	w := httptest.NewRecorder()
	handleError(w, httptest.NewRequest(http.MethodGet, "/", nil), err)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Equal(t, err.Error(), decodeResponse[ErrorResponse](t, w).ErrorMessage)
}

func TestHTTPPublicDetailsPreserveNumscriptDiagnostics(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("existing context: %w", &domain.BusinessError{Err: &domain.ErrNumscriptRuntime{Detail: "negative posting amount"}})
	w := httptest.NewRecorder()
	handleError(w, httptest.NewRequest(http.MethodPost, "/", nil), err)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	response := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, domain.ErrReasonNumscriptRuntime, response.ErrorCode)
	require.Equal(t, err.Error(), response.ErrorMessage)
}

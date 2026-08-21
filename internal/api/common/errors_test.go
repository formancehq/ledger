package common

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	ledger "github.com/formancehq/ledger/internal"
)

func TestHandleCommonWriteErrorsMetadataLimit(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/", nil)
	response := httptest.NewRecorder()
	err := fmt.Errorf("validating write: %w", ledger.ErrMetadataLimitExceeded{
		Constraint: "value size",
		Maximum:    ledger.MaxMetadataValueSize,
		Actual:     ledger.MaxMetadataValueSize + 1,
	})

	HandleCommonWriteErrors(response, request, err)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), ErrMetadataLimitExceeded)
}

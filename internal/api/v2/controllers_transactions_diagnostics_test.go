package v2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestTransactionCreateParserDiagnostics(t *testing.T) {
	// Parse an incomplete transaction script to obtain real parser errors.
	source := "send [USD 100] ("
	parserErrors := numscript.Parse(source).GetParsingErrors()
	require.NotEmpty(t, parserErrors)

	parseErr := ledgercontroller.ErrParsing{
		Source: source,
		Errors: parserErrors,
	}

	// Isolate the HTTP handler from transaction execution and the database.
	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		CreateTransaction(gomock.Any(), gomock.Any()).
		Return(nil, nil, false, parseErr)

	payload := bulking.TransactionRequest{
		Script: ledgercontroller.ScriptV1{
			Script: ledgercontroller.Script{
				Plain: source,
			},
		},
	}

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")
	request := httptest.NewRequest(
		http.MethodPost,
		"/xxx/transactions",
		api.Buffer(t, payload),
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	t.Logf("HTTP response: %s", recorder.Body.String())

	// Define the expected public JSON contract independently of implementation.
	type position struct {
		Line      int `json:"line"`
		Character int `json:"character"`
	}
	type diagnostic struct {
		Message string   `json:"message"`
		Start   position `json:"start"`
		End     position `json:"end"`
	}
	var response struct {
		ErrorCode    string       `json:"errorCode"`
		ErrorMessage string       `json:"errorMessage"`
		Diagnostics  []diagnostic `json:"diagnostics"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	// Preserve the existing response fields.
	require.Equal(t, common.ErrInterpreterParse, response.ErrorCode)
	require.Equal(t, parseErr.Error(), response.ErrorMessage)

	// Every parser error must retain its message and source positions.
	require.Len(t, response.Diagnostics, len(parserErrors))
	for i, parserError := range parserErrors {
		require.Equal(t, parserError.Msg, response.Diagnostics[i].Message)
		require.Equal(t, position{
			Line:      parserError.Start.Line,
			Character: parserError.Start.Character,
		}, response.Diagnostics[i].Start)
		require.Equal(t, position{
			Line:      parserError.End.Line,
			Character: parserError.End.Character,
		}, response.Diagnostics[i].End)
	}
}

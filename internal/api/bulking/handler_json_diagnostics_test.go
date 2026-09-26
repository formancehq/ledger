package bulking

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestBulkParserDiagnostics(t *testing.T) {
	source := "send [USD 100] ("
	parserErrors := numscript.Parse(source).GetParsingErrors()
	require.NotEmpty(t, parserErrors)

	parseErr := ledgercontroller.ErrParsing{
		Source: source,
		Errors: parserErrors,
	}

	for _, tc := range []struct {
		name        string
		err         error
		diagnostics bool
	}{
		{"parsing error", parseErr, true},
		{"wrapped parsing error", fmt.Errorf("execution: %w", parseErr), true},
		{"unrelated error", errors.New("unrelated failure"), false},
		{"success", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeJSONResponse(
				recorder,
				[]string{ActionCreateTransaction},
				[]BulkElementResult{{
					ElementID: 0,
					Error:     tc.err,
				}},
				nil,
			)

			expectedStatus := http.StatusOK
			if tc.err != nil {
				expectedStatus = http.StatusBadRequest
			}
			require.Equal(t, expectedStatus, recorder.Code)

			var response struct {
				Data []map[string]json.RawMessage `json:"data"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.Len(t, response.Data, 1)
			item := response.Data[0]

			if tc.err != nil {
				var description string
				require.NoError(t, json.Unmarshal(item["errorDescription"], &description))
				require.Equal(t, tc.err.Error(), description)
			}

			if !tc.diagnostics {
				require.NotContains(t, item, "diagnostics")
				return
			}

			var code string
			require.NoError(t, json.Unmarshal(item["errorCode"], &code))
			require.Equal(t, common.ErrInterpreterParse, code)
			require.Contains(t, item, "diagnostics")

			var diagnostics []struct {
				Message string         `json:"message"`
				Start   map[string]int `json:"start"`
				End     map[string]int `json:"end"`
			}
			require.NoError(t, json.Unmarshal(item["diagnostics"], &diagnostics))
			require.Len(t, diagnostics, len(parserErrors))

			for i, parserError := range parserErrors {
				require.Equal(t, parserError.Msg, diagnostics[i].Message)
				require.Equal(t, map[string]int{
					"line":      parserError.Start.Line,
					"character": parserError.Start.Character,
				}, diagnostics[i].Start)
				require.Equal(t, map[string]int{
					"line":      parserError.End.Line,
					"character": parserError.End.Character,
				}, diagnostics[i].End)
			}
		})
	}
}

package common

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

// DiagnosticPosition preserves the parser's zero-based source coordinates.
type DiagnosticPosition struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// ParserDiagnostic describes one parsing error and its source range.
type ParserDiagnostic struct {
	Message string             `json:"message"`
	Start   DiagnosticPosition `json:"start"`
	End     DiagnosticPosition `json:"end"`
}

// ParserDiagnostics extracts diagnostics from a parsing error,
// including errors wrapped with additional context.
func ParserDiagnostics(err error) []ParserDiagnostic {
	var parsingError ledgercontroller.ErrParsing
	if !errors.As(err, &parsingError) {
		var parsingErrorPointer *ledgercontroller.ErrParsing
		if !errors.As(err, &parsingErrorPointer) || parsingErrorPointer == nil {
			return nil
		}
		parsingError = *parsingErrorPointer
	}

	diagnostics := make([]ParserDiagnostic, 0, len(parsingError.Errors))
	for _, item := range parsingError.Errors {
		diagnostics = append(diagnostics, ParserDiagnostic{
			Message: item.Msg,
			Start: DiagnosticPosition{
				Line:      item.Start.Line,
				Character: item.Start.Character,
			},
			End: DiagnosticPosition{
				Line:      item.End.Line,
				Character: item.End.Character,
			},
		})
	}
	return diagnostics
}

// WriteParsingError preserves the existing error response and adds diagnostics.
func WriteParsingError(w http.ResponseWriter, err error) {
	response := struct {
		api.ErrorResponse
		Diagnostics []ParserDiagnostic `json:"diagnostics,omitempty"`
	}{
		ErrorResponse: api.ErrorResponse{
			ErrorCode:    ErrInterpreterParse,
			ErrorMessage: err.Error(),
		},
		Diagnostics: ParserDiagnostics(err),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	if encodeErr := json.NewEncoder(w).Encode(response); encodeErr != nil {
		panic(encodeErr)
	}
}

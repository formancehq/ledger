package common

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

// DiagnosticPosition is a zero-based source position measured in Unicode code points.
type DiagnosticPosition struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// ParserDiagnostic describes one parsing error and its half-open [start, end) source range.
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
		start, end := toDiagnosticRange(
			parsingError.Source,
			item.Start.Line,
			item.Start.Character,
			item.End.Character,
		)
		diagnostics = append(diagnostics, ParserDiagnostic{
			Message: item.Msg,
			Start:   start,
			End:     end,
		})
	}
	return diagnostics
}

// toDiagnosticRange maps a Numscript parser-native range onto the public API contract:
// zero-based lines, Unicode code-point columns, exclusive end, clamped to source.
// Numscript reports start.character in code points, an inclusive end.character,
// and token length in UTF-8 bytes. Parser errors are single-line.
func toDiagnosticRange(source string, line, startChar, endChar int) (DiagnosticPosition, DiagnosticPosition) {
	lines := strings.Split(source, "\n")
	if line < 0 {
		line = 0
	}
	if line >= len(lines) {
		last := len(lines) - 1
		p := DiagnosticPosition{
			Line:      last,
			Character: utf8.RuneCountInString(lines[last]),
		}
		return p, p
	}

	selected := lines[line]
	runeLen := utf8.RuneCountInString(selected)

	byteLen := endChar - startChar + 1
	if byteLen < 0 {
		byteLen = 0
	}

	if startChar < 0 {
		startChar = 0
	}
	if startChar > runeLen {
		startChar = runeLen
	}

	start := DiagnosticPosition{Line: line, Character: startChar}
	if startChar == runeLen {
		return start, start
	}

	offset := 0
	runeIndex := 0
	for runeIndex < startChar {
		_, size := utf8.DecodeRuneInString(selected[offset:])
		offset += size
		runeIndex++
	}

	consumed := 0
	endCharNormalized := startChar
	for consumed < byteLen && offset < len(selected) {
		_, size := utf8.DecodeRuneInString(selected[offset:])
		offset += size
		consumed += size
		endCharNormalized++
	}

	return start, DiagnosticPosition{Line: line, Character: endCharNormalized}
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

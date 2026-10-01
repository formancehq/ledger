package common

import (
	"errors"
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestParserDiagnostics(t *testing.T) {
	source := "abcdefghij\nsecond line\n0123456789abcdef"

	// Distinct single-line positions; Numscript parser errors are always single-line.
	// Inclusive parser ends become exclusive public ends after normalization.
	first := numscript.ParserError{Msg: "first error"}
	first.Start.Line = 0
	first.Start.Character = 2
	first.End.Line = 0
	first.End.Character = 5

	second := numscript.ParserError{Msg: "second error"}
	second.Start.Line = 2
	second.Start.Character = 4
	second.End.Line = 2
	second.End.Character = 8

	parseErr := ledgercontroller.ErrParsing{
		Source: source,
		Errors: []numscript.ParserError{first, second},
	}

	expected := []ParserDiagnostic{
		{
			Message: "first error",
			Start:   DiagnosticPosition{Line: 0, Character: 2},
			End:     DiagnosticPosition{Line: 0, Character: 6},
		},
		{
			Message: "second error",
			Start:   DiagnosticPosition{Line: 2, Character: 4},
			End:     DiagnosticPosition{Line: 2, Character: 9},
		},
	}

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"value", parseErr},
		{"pointer", &parseErr},
		{"wrapped value", fmt.Errorf("execution: %w", parseErr)},
		{"wrapped pointer", fmt.Errorf("execution: %w", &parseErr)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, expected, ParserDiagnostics(tc.err))
		})
	}

	t.Run("nil error", func(t *testing.T) {
		require.Nil(t, ParserDiagnostics(nil))
	})

	t.Run("unrelated error", func(t *testing.T) {
		require.Nil(t, ParserDiagnostics(errors.New("unrelated failure")))
	})
}

func TestParserDiagnosticsNormalizesNonASCIIParserRange(t *testing.T) {
	source := `"café☕"`
	parserErrors := numscript.Parse(source).GetParsingErrors()
	require.Len(t, parserErrors, 1)

	parseErr := ledgercontroller.ErrParsing{
		Source: source,
		Errors: parserErrors,
	}
	diagnostics := ParserDiagnostics(parseErr)

	require.Len(t, diagnostics, len(parserErrors))
	require.Equal(t, 0, diagnostics[0].Start.Character)

	normalizedEnd := utf8.RuneCountInString(source)
	require.Equal(t, normalizedEnd, diagnostics[0].End.Character)

	// Numscript reports an inclusive end using the token's UTF-8 byte length,
	// so the raw parser end is not a code-point-exclusive public position.
	require.NotEqual(t, parserErrors[0].End.Character, diagnostics[0].End.Character)
}

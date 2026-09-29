package common

import (
	"errors"
	"fmt"
	"testing"

	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestParserDiagnostics(t *testing.T) {
	// Distinct positions ensure we preserve every error in order.
	first := numscript.ParserError{Msg: "first error"}
	first.Start.Line = 0
	first.Start.Character = 2
	first.End.Line = 0
	first.End.Character = 5

	second := numscript.ParserError{Msg: "second error"}
	second.Start.Line = 3
	second.Start.Character = 4
	second.End.Line = 4
	second.End.Character = 1

	parseErr := ledgercontroller.ErrParsing{
		Errors: []numscript.ParserError{first, second},
	}

	expected := []ParserDiagnostic{
		{
			Message: "first error",
			Start:   DiagnosticPosition{Line: 0, Character: 2},
			End:     DiagnosticPosition{Line: 0, Character: 5},
		},
		{
			Message: "second error",
			Start:   DiagnosticPosition{Line: 3, Character: 4},
			End:     DiagnosticPosition{Line: 4, Character: 1},
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

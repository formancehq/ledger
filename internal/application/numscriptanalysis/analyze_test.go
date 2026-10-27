package numscriptanalysis

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnalyzeInputsAndRepeatability(t *testing.T) {
	t.Parallel()
	source := `vars {
    account $destination
    monetary $amount
    string $note
    account $origin = @world
}
send $amount (source = $origin destination = $destination)
set_tx_meta("note", $note)`
	want := []Input{{Name: "amount", Type: "monetary"}, {Name: "destination", Type: "account"}, {Name: "note", Type: "string"}}
	for range 3 {
		result, err := Analyze(source, "v1")
		require.NoError(t, err)
		require.Empty(t, result.Diagnostics)
		require.Equal(t, want, result.Inputs)
	}
}

func TestAnalyzeDiagnostics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, source, version, kind, code string
	}{
		{"version", `send [USD/2 1] (source = @world destination = @alice)`, "v2", "version", "UNSUPPORTED_VERSION"},
		{"empty", "", "v1", "syntax", "EMPTY_SOURCE"},
		{"syntax", `vars { monetary $amount`, "v1", "syntax", "SYNTAX_ERROR"},
		{"feature", `#![feature("not-a-flag")]
send [USD/2 1] (source = @world destination = @alice)`, "v1", "feature", "INVALID_FEATURE"},
		{"feature-gate", `send [USD/2 1] (source = oneof { @world @other } destination = @alice)`, "v1", "feature", "EXPERIMENTAL_FEATURE"},
		{"compile", `vars { string $source } send [USD/2 1] (source = $source destination = @alice)`, "v1", "compile", "TYPE_MISMATCH"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := Analyze(tc.source, tc.version)
			require.NoError(t, err)
			require.Empty(t, result.Inputs)
			require.NotEmpty(t, result.Diagnostics)
			require.Equal(t, tc.kind, result.Diagnostics[0].Kind)
			require.Equal(t, tc.code, result.Diagnostics[0].Code)
			require.NotEmpty(t, result.Diagnostics[0].Message)
		})
	}
}

func TestAnalyzeRejectsUnsafeSourceBeforeParsing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, source, code string
	}{
		{"deep block comments", strings.Repeat("/*", maxBlockCommentDepth+1) + strings.Repeat("*/", maxBlockCommentDepth+1), "SOURCE_TOO_COMPLEX"},
		{"large source", strings.Repeat(" ", maxAnalyzedSourceBytes+1), "SOURCE_TOO_LONG"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := Analyze(tc.source, "v1")
			require.NoError(t, err)
			require.Empty(t, result.Inputs)
			require.Equal(t, []Diagnostic{{Kind: "syntax", Code: tc.code, Message: "Numscript source exceeds the analysis complexity limit"}}, result.Diagnostics)
		})
	}
}

func TestSourceLimitationConservativelyCountsMarkersInStrings(t *testing.T) {
	t.Parallel()
	source := `set_tx_meta("example", "` + strings.Repeat("/*", maxBlockCommentDepth+1) + `")`
	require.Equal(t, "SOURCE_TOO_COMPLEX", sourceLimitation(source))
	result, err := Analyze(source, "v1")
	require.NoError(t, err)
	require.Equal(t, "SOURCE_TOO_COMPLEX", result.Diagnostics[0].Code)
}

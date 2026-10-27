package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAnalyzeNumscriptReadOnlyRPC(t *testing.T) {
	t.Parallel()
	server := &BucketServiceServerImpl{}
	request := &servicepb.AnalyzeNumscriptRequest{
		Source:  `vars { account $target } send [USD/2 1] (source = @world destination = $target)`,
		Version: "v1",
	}
	for range 2 {
		response, err := server.AnalyzeNumscript(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, []*servicepb.NumscriptInput{{Name: "target", Type: "account"}}, response.GetInputs())
		require.Empty(t, response.GetDiagnostics())
	}
}

func TestAnalyzeNumscriptConvertsDiagnostics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source, version, code string
		kind                        servicepb.NumscriptDiagnosticKind
		hasRange                    bool
	}{
		{"version", `send [USD/2 1] (source = @world destination = @alice)`, "v2", "UNSUPPORTED_VERSION", servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_VERSION, false},
		{"syntax", `vars { monetary $amount`, "v1", "SYNTAX_ERROR", servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_SYNTAX, true},
		{"feature", "#![feature(\"not-a-flag\")]\nsend [USD/2 1] (source = @world destination = @alice)", "v1", "INVALID_FEATURE", servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_FEATURE, true},
		{"compile", `vars { string $source } send [USD/2 1] (source = $source destination = @alice)`, "v1", "TYPE_MISMATCH", servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_COMPILE, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			response, err := (&BucketServiceServerImpl{}).AnalyzeNumscript(t.Context(), &servicepb.AnalyzeNumscriptRequest{Source: tc.source, Version: tc.version})
			require.NoError(t, err)
			require.Empty(t, response.GetInputs())
			require.Len(t, response.GetDiagnostics(), 1)
			diagnostic := response.GetDiagnostics()[0]
			require.Equal(t, tc.kind, diagnostic.GetKind())
			require.Equal(t, tc.code, diagnostic.GetCode())
			require.NotEmpty(t, diagnostic.GetMessage())
			if tc.hasRange {
				require.NotNil(t, diagnostic.StartLine)
				require.NotNil(t, diagnostic.StartColumn)
				require.NotNil(t, diagnostic.EndLine)
				require.NotNil(t, diagnostic.EndColumn)
			} else {
				require.Nil(t, diagnostic.StartLine)
				require.Nil(t, diagnostic.StartColumn)
				require.Nil(t, diagnostic.EndLine)
				require.Nil(t, diagnostic.EndColumn)
			}
		})
	}
}

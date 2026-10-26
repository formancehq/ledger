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

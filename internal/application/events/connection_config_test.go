package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/connectionconfig"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func connectionTestURL(t *testing.T, raw string) *commonpb.ConnectionURL {
	t.Helper()
	if raw == "" {
		return nil
	}
	config, err := connectionconfig.Sink(&commonpb.SinkConfigInput{Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: raw}}})
	require.NoError(t, err)

	return config.GetHttp().GetEndpoint()
}

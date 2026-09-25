package events

import (
	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const (
	secretSet  = sensitive.Marker
	secretNone = "(none)"
)

func redactSecret(value string) string {
	if value == "" {
		return secretNone
	}

	return secretSet
}

// Use the same schema annotations as the server for every output format.
func redactSinkConfig(config *commonpb.SinkConfig) *commonpb.SinkConfig {
	return sensitive.Clone(config)
}

func redactGetEventsSinksResponse(response *servicepb.GetEventsSinksResponse) *servicepb.GetEventsSinksResponse {
	return sensitive.Clone(response)
}

package http

import (
	jsonv2 "encoding/json/v2"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleGetEventsSinks handles GET /_/events-sinks to list configured event
// sinks together with their per-sink status (error status + last-emitted
// cursor), at parity with the gRPC GetEventsSinks RPC. The response shape is
// {sinks, sinkStatuses}.
func (s *Server) handleGetEventsSinks(w http.ResponseWriter, r *http.Request) {
	sinks, statuses, err := s.backend.GetEventsSinks(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	// Marshal through the generated standard-library projection so
	// SinkConfig/SinkStatus and nested oneof sink configs use protobuf JSON names.
	for i, s := range sinks {
		sinks[i] = sensitive.Redact(s)
	}
	raw, err := jsonv2.Marshal(&servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, r, json.RawValue(raw))
}

package http

import (
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
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

	// Marshal via protojson so SinkConfig/SinkStatus (and the nested oneof sink
	// configs) serialize in camelCase — the sonic default would leak snake_case
	// proto tags (sink_name) and the untagged oneof wrapper field. Reusing the
	// gRPC GetEventsSinksResponse gives both transports an identical shape.
	raw, err := protojson.Marshal(&servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, json.RawValue(raw))
}

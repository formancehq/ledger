package http

import (
	"errors"
	"io"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleAddEventsSink handles POST /_/events-sinks with a SinkConfig in
// protobuf JSON form. Apply owns validation, conflicts and idempotency.
func (s *Server) handleAddEventsSink(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	config := &commonpb.SinkConfig{}
	if err := protojson.Unmarshal(body, config); err != nil {
		// Decoder diagnostics can contain credential values from the input.
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid event sink configuration JSON"))

		return
	}
	if config.GetName() == "" || config.GetType() == nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("sink name and one sink type are required"))

		return
	}

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config}},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}
	details := map[string]any{"sink": config.GetName()}
	entry := exactlyOneLog("add-events-sink", logs, details)
	added := entry.GetPayload().GetAddedEventsSink()
	if added == nil {
		panic(unexpectedLogPayload("add-events-sink", entry, details))
	}
	// Acknowledge the committed identity without reflecting sink credentials.
	writeCreated(w, map[string]string{"name": added.GetConfig().GetName()})
}

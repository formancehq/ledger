package events

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
	"google.golang.org/protobuf/encoding/protojson"
)

// Format specifies the serialization format for events.
type Format string

const (
	FormatJSON  Format = "json"
	FormatProto Format = "protobuf"
)

// LogToEvent converts a committed global log entry into a domain event.
func LogToEvent(log *commonpb.Log) *eventspb.Event {
	event := &eventspb.Event{
		LogSequence: log.Sequence,
		Log:         log,
	}

	switch p := log.Payload.Type.(type) {
	case *commonpb.LogPayload_CreateLedger:
		event.Type = eventspb.EventType_CREATED_LEDGER
		event.Ledger = p.CreateLedger.Info.Name
		event.Date = p.CreateLedger.Info.CreatedAt
	case *commonpb.LogPayload_DeleteLedger:
		event.Type = eventspb.EventType_DELETED_LEDGER
		event.Ledger = p.DeleteLedger.Info.Name
		event.Date = p.DeleteLedger.Info.DeletedAt
	case *commonpb.LogPayload_Apply:
		event.Ledger = p.Apply.LedgerName
		event.Date = p.Apply.Log.Date

		switch p.Apply.Log.Data.Payload.(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			event.Type = eventspb.EventType_COMMITTED_TRANSACTION
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			event.Type = eventspb.EventType_REVERTED_TRANSACTION
		case *commonpb.LedgerLogPayload_SavedMetadata:
			event.Type = eventspb.EventType_SAVED_METADATA
		case *commonpb.LedgerLogPayload_DeletedMetadata:
			event.Type = eventspb.EventType_DELETED_METADATA
		}
	}

	return event
}

// SerializeEvent serializes an event in the specified format.
func SerializeEvent(event *eventspb.Event, format Format) ([]byte, error) {
	switch format {
	case FormatJSON:
		data, err := protojson.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("marshaling event to JSON: %w", err)
		}
		return data, nil
	case FormatProto:
		data, err := event.MarshalVT()
		if err != nil {
			return nil, fmt.Errorf("marshaling event to protobuf: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported event format: %s", format)
	}
}

package events

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
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
		LogSequence: log.GetSequence(),
		Log:         log,
	}

	switch p := log.GetPayload().GetType().(type) {
	case *commonpb.LogPayload_CreateLedger:
		event.Type = commonpb.EventType_CREATED_LEDGER
		event.Ledger = p.CreateLedger.GetName()
		event.Date = p.CreateLedger.GetCreatedAt()
	case *commonpb.LogPayload_DeleteLedger:
		event.Type = commonpb.EventType_DELETED_LEDGER
		event.Ledger = p.DeleteLedger.GetName()
		event.Date = p.DeleteLedger.GetDeletedAt()
	case *commonpb.LogPayload_Apply:
		event.Ledger = p.Apply.GetLedgerName()
		event.Date = p.Apply.GetLog().GetDate()

		switch p.Apply.GetLog().GetData().GetPayload().(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			event.Type = commonpb.EventType_COMMITTED_TRANSACTION
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			event.Type = commonpb.EventType_REVERTED_TRANSACTION
		case *commonpb.LedgerLogPayload_SavedMetadata:
			event.Type = commonpb.EventType_SAVED_METADATA
		case *commonpb.LedgerLogPayload_DeletedMetadata:
			event.Type = commonpb.EventType_DELETED_METADATA
		case *commonpb.LedgerLogPayload_SetMetadataFieldType:
			// Schema operations — no dedicated event type, use unspecified
		case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
			// Schema operations — no dedicated event type, use unspecified
		case *commonpb.LedgerLogPayload_ConvertMetadataBatch:
			// Background conversion — no dedicated event type
		case *commonpb.LedgerLogPayload_MetadataConversionComplete:
			// Background conversion — no dedicated event type
		}
	}

	return event
}

// SerializeEvent serializes an event in the specified format.
func SerializeEvent(event *eventspb.Event, format Format) ([]byte, error) {
	switch format {
	case FormatJSON:
		data, err := json.Marshal(event)
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

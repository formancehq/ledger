package events

import (
	"fmt"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// Format specifies the serialization format for events.
type Format string

const (
	FormatJSON  Format = "json"
	FormatProto Format = "protobuf"
)

const (
	EventApp     = "ledger"
	EventVersion = "v3"
)

// LogToEvent converts a committed global log entry into a domain event.
func LogToEvent(log *ledgerpb.Log) *eventspb.Event {
	event := &eventspb.Event{
		LogSequence: log.GetSequence(),
		Log:         log,
		App:         EventApp,
		Version:     EventVersion,
	}

	switch p := log.GetPayload().GetType().(type) {
	case *ledgerpb.LogPayload_CreateLedger:
		event.Type = ledgerpb.EventType_CREATED_LEDGER
		event.Ledger = p.CreateLedger.GetName()
		event.Date = p.CreateLedger.GetCreatedAt()
	case *ledgerpb.LogPayload_DeleteLedger:
		event.Type = ledgerpb.EventType_DELETED_LEDGER
		event.Ledger = p.DeleteLedger.GetName()
		event.Date = p.DeleteLedger.GetDeletedAt()
	case *ledgerpb.LogPayload_Apply:
		event.Ledger = p.Apply.GetLedgerName()
		event.Date = p.Apply.GetLog().GetDate()

		switch p.Apply.GetLog().GetData().GetPayload().(type) {
		case *ledgerpb.LedgerLogPayload_CreatedTransaction:
			event.Type = ledgerpb.EventType_COMMITTED_TRANSACTION
		case *ledgerpb.LedgerLogPayload_RevertedTransaction:
			event.Type = ledgerpb.EventType_REVERTED_TRANSACTION
		case *ledgerpb.LedgerLogPayload_SavedMetadata:
			event.Type = ledgerpb.EventType_SAVED_METADATA
		case *ledgerpb.LedgerLogPayload_DeletedMetadata:
			event.Type = ledgerpb.EventType_DELETED_METADATA
		case *ledgerpb.LedgerLogPayload_SetMetadataFieldType:
			// Schema operations — no dedicated event type, use unspecified
		case *ledgerpb.LedgerLogPayload_RemovedMetadataFieldType:
			// Schema operations — no dedicated event type, use unspecified
		case *ledgerpb.LedgerLogPayload_OrderSkipped:
			event.Type = ledgerpb.EventType_SKIPPED_ORDER
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

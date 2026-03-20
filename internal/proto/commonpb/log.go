package commonpb

import (
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

// NewLedgerLog creates a new LedgerLog from a LedgerLogPayload.
func NewLedgerLog(payload *LedgerLogPayload) *LedgerLog {
	return &LedgerLog{
		Data: payload,
	}
}

// WithDate sets the date of the log.
func (l *LedgerLog) WithDate(date time.Time) *LedgerLog {
	if l == nil {
		l = &LedgerLog{}
	}

	l.Date = NewTimestamp(date)

	return l
}

// WithID sets the ID of the log.
func (l *LedgerLog) WithID(id uint64) *LedgerLog {
	if l == nil {
		l = &LedgerLog{}
	}

	l.Id = id

	return l
}

// UnmarshalJSON implements json.Unmarshaler for LedgerLog.
func (l *LedgerLog) UnmarshalJSON(data []byte) error {
	type auxLog struct {
		Type LogType       `json:"type"`
		Data json.RawValue `json:"data"`
		Date *time.Time    `json:"date"`
		ID   *uint64       `json:"id"`
	}

	rawLog := auxLog{}

	err := json.Unmarshal(data, &rawLog)
	if err != nil {
		return err
	}

	if rawLog.Date != nil {
		l.Date = NewTimestamp(*rawLog.Date)
	}

	if rawLog.ID != nil {
		l.Id = *rawLog.ID
	}

	// Parse LedgerLogPayload from JSON using the type from rawLog
	if len(rawLog.Data) > 0 {
		payload, err := HydrateLog(rawLog.Type, rawLog.Data)
		if err != nil {
			return err
		}

		switch p := payload.(type) {
		case *CreatedTransaction:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: p,
				},
			}
		case *RevertedTransaction:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_RevertedTransaction{
					RevertedTransaction: p,
				},
			}
		case *SavedMetadata:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_SavedMetadata{
					SavedMetadata: p,
				},
			}
		case *DeletedMetadata:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_DeletedMetadata{
					DeletedMetadata: p,
				},
			}
		case *SetMetadataFieldTypeLog:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_SetMetadataFieldType{
					SetMetadataFieldType: p,
				},
			}
		case *RemovedMetadataFieldTypeLog:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_RemovedMetadataFieldType{
					RemovedMetadataFieldType: p,
				},
			}
		case *ConvertMetadataBatchLog:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_ConvertMetadataBatch{
					ConvertMetadataBatch: p,
				},
			}
		case *MetadataConversionCompleteLog:
			l.Data = &LedgerLogPayload{
				Payload: &LedgerLogPayload_MetadataConversionComplete{
					MetadataConversionComplete: p,
				},
			}
		}
	}

	return nil
}

// MarshalJSON implements json.Marshaler for LedgerLog.
func (l *LedgerLog) MarshalJSON() ([]byte, error) {
	type auxLog struct {
		Type LogType           `json:"type"`
		Data *LedgerLogPayload `json:"data"`
		Date *time.Time        `json:"date,omitempty"`
		ID   *uint64           `json:"id,omitempty"`
	}

	aux := auxLog{
		Type: GetLogTypeFromLog(l),
		Data: l.GetData(),
	}

	if l.GetDate() != nil {
		t := l.GetDate().AsTime()
		aux.Date = &t
	}

	if l.GetId() != 0 {
		aux.ID = new(l.GetId())
	}

	return json.Marshal(aux)
}

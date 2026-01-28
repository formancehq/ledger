package commonpb

import (
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/json"
)

// NewLedgerLog creates a new LedgerLog from a LogPayload
func NewLedgerLog(payload *LogPayload) *LedgerLog {
	return &LedgerLog{
		Data: payload,
	}
}

// WithDate sets the date of the log
func (l *LedgerLog) WithDate(date time.Time) *LedgerLog {
	if l == nil {
		l = &LedgerLog{}
	}
	l.Date = NewTimestamp(date)
	return l
}

// WithID sets the ID of the log
func (l *LedgerLog) WithID(id uint64) *LedgerLog {
	if l == nil {
		l = &LedgerLog{}
	}
	l.Id = id
	return l
}

// UnmarshalJSON implements json.Unmarshaler for LedgerLog
func (l *LedgerLog) UnmarshalJSON(data []byte) error {
	type auxLog struct {
		Type LogType       `json:"type"`
		Data json.RawValue `json:"data"`
		Date *time.Time    `json:"date"`
		ID   *uint64       `json:"id"`
	}
	rawLog := auxLog{}
	if err := json.Unmarshal(data, &rawLog); err != nil {
		return err
	}

	if rawLog.Date != nil {
		l.Date = NewTimestamp(*rawLog.Date)
	}
	if rawLog.ID != nil {
		l.Id = *rawLog.ID
	}

	// Parse LogPayload from JSON using the type from rawLog
	if len(rawLog.Data) > 0 {
		payload, err := HydrateLog(rawLog.Type, rawLog.Data)
		if err != nil {
			return err
		}
		switch p := payload.(type) {
		case *CreatedTransaction:
			l.Data = &LogPayload{
				Payload: &LogPayload_CreatedTransaction{
					CreatedTransaction: p,
				},
			}
		case *RevertedTransaction:
			l.Data = &LogPayload{
				Payload: &LogPayload_RevertedTransaction{
					RevertedTransaction: p,
				},
			}
		case *SavedMetadata:
			l.Data = &LogPayload{
				Payload: &LogPayload_SavedMetadata{
					SavedMetadata: p,
				},
			}
		case *DeletedMetadata:
			l.Data = &LogPayload{
				Payload: &LogPayload_DeletedMetadata{
					DeletedMetadata: p,
				},
			}
		}
	}
	return nil
}

// MarshalJSON implements json.Marshaler for LedgerLog
func (l *LedgerLog) MarshalJSON() ([]byte, error) {
	type auxLog struct {
		Type LogType     `json:"type"`
		Data *LogPayload `json:"data"`
		Date *time.Time  `json:"date,omitempty"`
		ID   *uint64     `json:"id,omitempty"`
	}

	aux := auxLog{
		Type: GetLogTypeFromLog(l),
		Data: l.Data,
	}

	if l.Date != nil {
		t := l.Date.AsTime()
		aux.Date = &t
	}
	if l.Id != 0 {
		aux.ID = pointer.For(l.Id)
	}

	return json.Marshal(aux)
}

package ledgerpb

import (
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/json"
)

// NewLog creates a new Log from a LogPayload
func NewLog(payload *LogPayload) *Log {
	return &Log{
		Data: payload,
	}
}

// WithDate sets the date of the log
func (l *Log) WithDate(date time.Time) *Log {
	if l == nil {
		l = &Log{}
	}
	l.Date = NewTimestamp(date)
	return l
}

// WithIdempotencyKey sets the idempotency key
func (l *Log) WithIdempotency(key string, hash []byte) *Log {
	if l == nil {
		l = &Log{}
	}
	l.Idempotency = &Idempotency{
		Key:  key,
		Hash: hash,
	}

	return l
}

// WithID sets the ID of the log
func (l *Log) WithID(id uint64) *Log {
	if l == nil {
		l = &Log{}
	}
	l.Id = id
	return l
}

// WithLedgerID sets the ledger of the log
func (l *Log) WithLedgerID(ledgerID uint32) *Log {
	if l == nil {
		l = &Log{}
	}
	l.LedgerId = ledgerID
	return l
}

// UnmarshalJSON implements json.Unmarshaler for Log
func (l *Log) UnmarshalJSON(data []byte) error {
	type auxLog struct {
		Type            LogType       `json:"type"`
		Data            json.RawValue `json:"data"`
		Date            *time.Time    `json:"date"`
		IdempotencyKey  string        `json:"idempotencyKey"`
		IdempotencyHash []byte        `json:"idempotencyHash"`
		ID              *uint64       `json:"id"`
	}
	rawLog := auxLog{}
	if err := json.Unmarshal(data, &rawLog); err != nil {
		return err
	}

	if rawLog.Date != nil {
		l.Date = NewTimestamp(*rawLog.Date)
	}
	l.Idempotency = &Idempotency{
		Key:  rawLog.IdempotencyKey,
		Hash: rawLog.IdempotencyHash,
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

// MarshalJSON implements json.Marshaler for Log
func (l *Log) MarshalJSON() ([]byte, error) {
	type auxLog struct {
		Type            LogType     `json:"type"`
		Data            *LogPayload `json:"data"`
		Date            *time.Time  `json:"date,omitempty"`
		IdempotencyKey  string      `json:"idempotencyKey,omitempty"`
		IdempotencyHash []byte      `json:"idempotencyHash,omitempty"`
		ID              *uint64     `json:"id,omitempty"`
	}

	aux := auxLog{
		Type:            GetLogTypeFromLog(l),
		Data:            l.Data,
		IdempotencyKey:  l.Idempotency.Key,
		IdempotencyHash: l.Idempotency.Hash,
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

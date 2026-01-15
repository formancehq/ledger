package ledgerpb

import (
	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
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
		Key: key,
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

// WithLedger sets the ledger of the log
func (l *Log) WithLedger(ledger string) *Log {
	if l == nil {
		l = &Log{}
	}
	l.Ledger = ledger
	return l
}

// ChainLog creates a new log chained to the previous one
func ChainLog(l *Log, previous *Log) *Log {
	if l == nil {
		l = &Log{}
	}
	ret := &Log{
		Data:        l.Data,
		Date:        l.Date,
		Idempotency: l.Idempotency,
		Id:          l.Id,
		Ledger:      l.Ledger,
	}
	if previous != nil && previous.Id != 0 {
		ret.Id = previous.Id + 1
	} else {
		ret.Id = 1
	}
	return ret
}

// UnmarshalJSON implements json.Unmarshaler for Log
func (l *Log) UnmarshalJSON(data []byte) error {
	type auxLog struct {
		Type            LogType        `json:"type"`
		Data            jsontext.Value `json:"data"`
		Date            *time.Time     `json:"date"`
		IdempotencyKey  string         `json:"idempotencyKey"`
		IdempotencyHash []byte         `json:"idempotencyHash"`
		ID              *uint64        `json:"id"`
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
		dataBytes, err := json.Marshal(rawLog.Data)
		if err != nil {
			return err
		}
		payload, err := HydrateLog(rawLog.Type, dataBytes)
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

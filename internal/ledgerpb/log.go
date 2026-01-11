package ledgerpb

import (
	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
)

// LogType constants matching internal/log.go
const (
	LogTypeSetMetadata         = int32(0) // SET_METADATA
	LogTypeNewTransaction      = int32(1) // NEW_TRANSACTION
	LogTypeRevertedTransaction = int32(2) // REVERTED_TRANSACTION
	LogTypeDeleteMetadata      = int32(3) // DELETE_METADATA
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
func (l *Log) WithIdempotencyKey(key string) *Log {
	if l == nil {
		l = &Log{}
	}
	l.IdempotencyKey = key
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

// ChainLog creates a new log chained to the previous one
func ChainLog(l *Log, previous *Log) *Log {
	if l == nil {
		l = &Log{}
	}
	ret := &Log{
		Data:            l.Data,
		Date:            l.Date,
		IdempotencyKey:  l.IdempotencyKey,
		IdempotencyHash: l.IdempotencyHash,
		Id:              l.Id,
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
		IdempotencyHash string         `json:"idempotencyHash"`
		ID              *uint64        `json:"id"`
	}
	rawLog := auxLog{}
	if err := json.Unmarshal(data, &rawLog); err != nil {
		return err
	}

	if rawLog.Date != nil {
		l.Date = NewTimestamp(*rawLog.Date)
	}
	l.IdempotencyKey = rawLog.IdempotencyKey
	l.IdempotencyHash = rawLog.IdempotencyHash
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
		l.Data, err = LogPayloadToProtobuf(payload)
		if err != nil {
			return err
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
		IdempotencyHash string      `json:"idempotencyHash,omitempty"`
		ID              *uint64     `json:"id,omitempty"`
	}

	aux := auxLog{
		Type:            GetLogTypeFromLog(l),
		Data:            l.Data,
		IdempotencyKey:  l.IdempotencyKey,
		IdempotencyHash: l.IdempotencyHash,
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

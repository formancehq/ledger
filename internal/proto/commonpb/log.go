package commonpb

import (
	"errors"

	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
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
	if aux.Type.String() == "" {
		return nil, errors.New("missing log payload")
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

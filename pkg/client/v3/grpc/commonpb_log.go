package grpc

import (
	"errors"
	"time"

	"github.com/formancehq/ledger/pkg/client/v3/internal/json"
)

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

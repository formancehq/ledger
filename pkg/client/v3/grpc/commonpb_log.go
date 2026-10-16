package grpc

import (
	"encoding/json/jsontext"
	"errors"
	"time"

	"github.com/formancehq/ledger/pkg/client/v3/internal/json"
)

// MarshalJSON implements json.Marshaler for LedgerLog.
func (l *LedgerLog) MarshalJSON() ([]byte, error) {
	value, err := l.jsonValue()
	if err != nil {
		return nil, err
	}

	return json.Marshal(value)
}

// MarshalJSONTo preserves request-scoped options through the public projection.
func (l *LedgerLog) MarshalJSONTo(enc *jsontext.Encoder) error {
	value, err := l.jsonValue()
	if err != nil {
		return err
	}

	return json.MarshalEncode(enc, value)
}

func (l *LedgerLog) jsonValue() (any, error) {
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

	return aux, nil
}

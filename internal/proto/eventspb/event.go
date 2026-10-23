package eventspb

import (
	"time"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// MarshalJSON implements json.Marshaler for Event.
func (x *Event) MarshalJSON() ([]byte, error) {
	type Aux struct {
		App         string        `json:"app"`
		Version     string        `json:"version"`
		Type        string        `json:"type"`
		Ledger      string        `json:"ledger"`
		Date        *time.Time    `json:"date,omitempty"`
		LogSequence uint64        `json:"logSequence"`
		Log         *ledgerpb.Log `json:"log,omitempty"`
	}

	aux := Aux{
		App:         x.GetApp(),
		Version:     x.GetVersion(),
		Type:        x.GetType().String(),
		Ledger:      x.GetLedger(),
		LogSequence: x.GetLogSequence(),
		Log:         x.GetLog(),
	}

	if x.GetDate() != nil {
		t := x.GetDate().AsTime()
		aux.Date = &t
	}

	return json.Marshal(aux)
}

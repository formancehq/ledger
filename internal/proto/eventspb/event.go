package eventspb

import (
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// MarshalJSON implements json.Marshaler for Event.
func (x *Event) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Type        string        `json:"type"`
		Ledger      string        `json:"ledger"`
		Date        *time.Time    `json:"date,omitempty"`
		LogSequence uint64        `json:"logSequence"`
		Log         *commonpb.Log `json:"log,omitempty"`
	}

	aux := Aux{
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

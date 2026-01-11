package systempb

import (
	"encoding/json/v2"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

func (x *State) MarshalJSON() ([]byte, error) {
	if x == nil {
		return json.Marshal(&struct {
			NextLedgerID uint64                          `json:"nextLedgerID,omitempty"`
			Ledgers      map[string]*ledgerpb.LedgerInfo `json:"ledgers,omitempty"`
		}{})
	}

	return json.Marshal(&struct {
		NextLedgerID uint64                          `json:"nextLedgerID,omitempty"`
		Ledgers      map[string]*ledgerpb.LedgerInfo `json:"ledgers,omitempty"`
	}{
		NextLedgerID: x.NextLedgerId,
		Ledgers:      x.Ledgers,
	})
}

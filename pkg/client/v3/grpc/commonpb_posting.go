package grpc

import "github.com/formancehq/ledger/pkg/client/v3/internal/json"

// MarshalJSON implements json.Marshaler for Posting. Color is always emitted
// (even when empty) so clients can distinguish the uncolored bucket from an
// older response shape that predates the dimension — same contract as
// VolumeEntry and accountVolumeJSON.
func (x *Posting) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Source      string   `json:"source"`
		Destination string   `json:"destination"`
		Amount      *Uint256 `json:"amount,omitempty"`
		Asset       string   `json:"asset"`
		Color       string   `json:"color"`
	}{
		Source:      x.GetSource(),
		Destination: x.GetDestination(),
		Amount:      x.GetAmount(),
		Asset:       x.GetAsset(),
		Color:       x.GetColor(),
	})
}

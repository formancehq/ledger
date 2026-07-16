package commonpb

import (
	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// MarshalJSON implements json.Marshaler for AccountVolume. Color is always
// emitted (even when empty) so REST clients can distinguish the uncolored
// bucket from an older response shape — same contract as VolumeEntry,
// Posting, and the accountVolumeJSON shim used by Account.MarshalJSON.
//
// Account.MarshalJSON already builds accountVolumeJSON entries by hand, so
// direct serialization through that path is safe. This method covers
// any other call site (gRPC-Gateway, ad-hoc marshaling of a single row)
// that touches *AccountVolume directly.
func (x *AccountVolume) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Asset   string              `json:"asset"`
		Color   string              `json:"color"`
		Volumes *VolumesWithBalance `json:"volumes,omitempty"`
	}{
		Asset:   x.GetAsset(),
		Color:   x.GetColor(),
		Volumes: x.GetVolumes(),
	})
}

// FindVolume returns the VolumesWithBalance for a given (asset, color) tuple
// on this account, or nil if absent. Color "" is the uncolored bucket.
//
// Account.Volumes is a sorted list, so this is an O(n) linear scan. For
// repeated lookups, callers should build their own map. This helper exists
// to keep direct lookups ergonomic in tests and CLI rendering.
func (a *Account) FindVolume(asset, color string) *VolumesWithBalance {
	if a == nil {
		return nil
	}
	for _, entry := range a.GetVolumes() {
		if entry.GetAsset() == asset && entry.GetColor() == color {
			return entry.GetVolumes()
		}
	}

	return nil
}

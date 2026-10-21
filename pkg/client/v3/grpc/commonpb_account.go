package grpc

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

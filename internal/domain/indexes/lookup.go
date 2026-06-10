package indexes

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// Find returns the Index entry for the given id, or nil if absent.
func Find(info *commonpb.LedgerInfo, id *commonpb.IndexID) *commonpb.Index {
	if info == nil || id == nil {
		return nil
	}

	for _, idx := range info.GetIndexes() {
		if Equal(idx.GetId(), id) {
			return idx
		}
	}

	return nil
}

// IsReady returns true iff an index with the given id exists and is in READY state.
func IsReady(info *commonpb.LedgerInfo, id *commonpb.IndexID) bool {
	idx := Find(info, id)
	if idx == nil {
		return false
	}

	return idx.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
}

// Status returns the build status for the given id. Returns UNSPECIFIED when
// the index does not exist (callers should distinguish via Find when the
// difference matters).
func Status(info *commonpb.LedgerInfo, id *commonpb.IndexID) commonpb.IndexBuildStatus {
	idx := Find(info, id)
	if idx == nil {
		return commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED
	}

	return idx.GetBuildStatus()
}

// Put upserts an Index entry by id. Existing entries with the same id are
// replaced in place. Caller is responsible for cloning the LedgerInfo before
// mutating it through this helper (the FSM convention CloneVT applies).
func Put(info *commonpb.LedgerInfo, idx *commonpb.Index) {
	if info == nil || idx == nil {
		return
	}

	for i, existing := range info.GetIndexes() {
		if Equal(existing.GetId(), idx.GetId()) {
			info.Indexes[i] = idx

			return
		}
	}

	info.Indexes = append(info.Indexes, idx)
}

// Remove deletes the Index entry matching id. Returns true if an entry was
// removed.
func Remove(info *commonpb.LedgerInfo, id *commonpb.IndexID) bool {
	if info == nil || id == nil {
		return false
	}

	for i, idx := range info.GetIndexes() {
		if Equal(idx.GetId(), id) {
			info.Indexes = append(info.Indexes[:i], info.GetIndexes()[i+1:]...)

			return true
		}
	}

	return false
}

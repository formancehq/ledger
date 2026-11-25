package backup

import "strings"

// CheckpointFileKey returns the content-addressed S3 key for a checkpoint file.
// The contentHash (hex sha256 of the file bytes) is appended to the local
// filename so a file whose content changes between checkpoints lands on a NEW
// key instead of overwriting an object the currently published manifest still
// references — the immutability guarantee behind EN-1055. Two checkpoints that
// carry byte-identical content for the same filename produce the same key, so
// unchanged files are deduped and skipped on re-upload.
//
// The filename is kept in the key (rather than a bare hash) so operators can
// still recognise objects in the bucket; correctness rests entirely on the
// hash suffix.
func CheckpointFileKey(bucketID, filename, contentHash string) string {
	return CheckpointPrefix(bucketID) + filename + "." + contentHash
}

// CheckpointPrefix returns the key prefix shared by every checkpoint file.
func CheckpointPrefix(bucketID string) string {
	return bucketID + "/data/"
}

// ExportPrefix returns the key prefix shared by every export segment.
func ExportPrefix(bucketID string) string {
	return bucketID + "/exports/"
}

// Pruning is a delete-by-listing: it lists a root and removes what the current
// manifest does not reference. An object-store listing by prefix also returns
// the objects of any OTHER backup chain whose own bucketID nests under this
// one — a chain rooted at "tenant/data/archive" is returned when a chain rooted
// at "tenant" lists its checkpoint root. Those are a live chain's objects, not
// this chain's orphans, and deleting them destroys that backup, manifest
// included.
//
// So a prune never deletes "whatever the listing returned". It deletes only
// keys this chain's own writers could have produced, recognised below. Both
// recognisers require a single path segment under the root, which is what the
// checkpoint writer and the legacy export writer emitted; anything deeper
// belongs to a nested chain.

// sha256HexLen is the length of the hex-encoded sha256 CheckpointFileKey
// appends to every checkpoint object key.
const sha256HexLen = 64

// exportSegmentExt is the suffix of legacy export objects eligible for cleanup.
const exportSegmentExt = ".bin"

// ownsCheckpointKey reports whether key is one CheckpointFileKey could have
// written for bucketID: one path segment under the checkpoint root, ending in
// the content hash that builder appends.
func ownsCheckpointKey(bucketID, key string) bool {
	name, found := strings.CutPrefix(key, CheckpointPrefix(bucketID))
	if !found || strings.Contains(name, "/") {
		return false
	}

	_, ok := checkpointKeyHash(name)

	return ok
}

// checkpointKeyHash returns the hex sha256 that CheckpointFileKey appended to
// key, and false when key does not end in one.
func checkpointKeyHash(key string) (string, bool) {
	dot := strings.LastIndexByte(key, '.')
	if dot < 0 || !isLowerHex(key[dot+1:], sha256HexLen) {
		return "", false
	}

	return key[dot+1:], true
}

// ownsExportKey reports whether key has the legacy export shape for bucketID:
// one path segment under the export root with the .bin suffix.
func ownsExportKey(bucketID, key string) bool {
	name, found := strings.CutPrefix(key, ExportPrefix(bucketID))
	if !found || strings.Contains(name, "/") {
		return false
	}

	return strings.HasSuffix(name, exportSegmentExt)
}

// isLowerHex reports whether s is exactly n lower-case hex digits, the shape
// hex.EncodeToString produces. Upper case is rejected: no builder emits it, so
// accepting it would widen what a prune is willing to delete.
func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}

	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}

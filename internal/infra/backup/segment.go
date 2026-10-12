package backup

import (
	"fmt"
	"strings"
)

// A single incremental range is split into one or more size-bounded parts
// (see maxExportSegmentBytes). All parts of a range share the [startSeq, endSeq]
// prefix so they group together under exports/, and a zero-padded part index
// makes each object key unique. The manifest's ExportSegment records the exact
// per-part sub-range; the filename range is the overall incremental range.

// ExportLogSegmentKey returns the S3 key for part `part` of a log export.
func ExportLogSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/exports/logs-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// ExportAuditSegmentKey returns the S3 key for part `part` of an audit-entry export.
func ExportAuditSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/exports/audit-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// ExportAuditItemSegmentKey returns the S3 key for part `part` of an audit-item
// export. Audit items (the per-order detail the audit hash is computed over)
// live in a separate subzone from audit entries and must be exported alongside
// them, or a restored incremental backup cannot reconstruct the hash chain.
func ExportAuditItemSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/exports/audit-items-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// ExportAppliedProposalSegmentKey returns the S3 key for part `part` of an
// AppliedProposal export. AppliedProposal sequences are 1:1 with AuditEntry on
// the success path, so the segment shares the audit range — restoring a backup
// without this segment would leave the index builder unable to learn the
// transient-account exclusion set for replayed logs.
func ExportAppliedProposalSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/exports/applied-proposals-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

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
// builders above always emit; anything deeper belongs to a nested chain.

// sha256HexLen is the length of the hex-encoded sha256 CheckpointFileKey
// appends to every checkpoint object key.
const sha256HexLen = 64

// exportSegmentExt is the suffix every Export*SegmentKey builder emits.
// TestExportBuildersProduceOwnedKeys pins this to the builders.
const exportSegmentExt = ".bin"

// ownsCheckpointKey reports whether key is one CheckpointFileKey could have
// written for bucketID: one path segment under the checkpoint root, ending in
// the content hash that builder appends.
func ownsCheckpointKey(bucketID, key string) bool {
	name, found := strings.CutPrefix(key, CheckpointPrefix(bucketID))
	if !found || strings.Contains(name, "/") {
		return false
	}

	dot := strings.LastIndexByte(name, '.')
	if dot < 0 {
		return false
	}

	return isLowerHex(name[dot+1:], sha256HexLen)
}

// ownsExportKey reports whether key is one the Export*SegmentKey builders could
// have written for bucketID: one path segment under the export root carrying
// the suffix they all emit.
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

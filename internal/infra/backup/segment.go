package backup

import "fmt"

// A single incremental range is split into one or more size-bounded parts
// (see maxExportSegmentBytes). All parts of a range share the [startSeq, endSeq]
// prefix so they group together under exports/, and a zero-padded part index
// makes each object key unique. The manifest's ExportSegment records the exact
// per-part sub-range; the filename range is the overall incremental range.

// ExportLogSegmentKey returns the S3 key for part `part` of a log export.
func ExportLogSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/backups/exports/logs-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// ExportAuditSegmentKey returns the S3 key for part `part` of an audit-entry export.
func ExportAuditSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/backups/exports/audit-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// ExportAuditItemSegmentKey returns the S3 key for part `part` of an audit-item
// export. Audit items (the per-order detail the audit hash is computed over)
// live in a separate subzone from audit entries and must be exported alongside
// them, or a restored incremental backup cannot reconstruct the hash chain.
func ExportAuditItemSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/backups/exports/audit-items-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// ExportAppliedProposalSegmentKey returns the S3 key for part `part` of an
// AppliedProposal export. AppliedProposal sequences are 1:1 with AuditEntry on
// the success path, so the segment shares the audit range — restoring a backup
// without this segment would leave the index builder unable to learn the
// transient-account exclusion set for replayed logs.
func ExportAppliedProposalSegmentKey(bucketID string, startSeq, endSeq uint64, part int) string {
	return fmt.Sprintf("%s/backups/exports/applied-proposals-%020d-%020d-%05d.bin", bucketID, startSeq, endSeq, part)
}

// CheckpointFileKey returns the S3 key for a checkpoint file.
func CheckpointFileKey(bucketID, filename string) string {
	return CheckpointPrefix(bucketID) + filename
}

// CheckpointPrefix returns the key prefix shared by every checkpoint file.
func CheckpointPrefix(bucketID string) string {
	return bucketID + "/backups/data/"
}

// ExportPrefix returns the key prefix shared by every export segment.
func ExportPrefix(bucketID string) string {
	return bucketID + "/backups/exports/"
}

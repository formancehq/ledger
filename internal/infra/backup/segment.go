package backup

import "fmt"

// ExportLogSegmentKey returns the S3 key for a log export segment.
func ExportLogSegmentKey(bucketID string, startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s/backups/exports/logs-%020d-%020d.bin", bucketID, startSeq, endSeq)
}

// ExportAuditSegmentKey returns the S3 key for an audit-entry export segment.
func ExportAuditSegmentKey(bucketID string, startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s/backups/exports/audit-%020d-%020d.bin", bucketID, startSeq, endSeq)
}

// ExportAuditItemSegmentKey returns the S3 key for an audit-item export
// segment. Audit items (the per-order detail the audit hash is computed over)
// live in a separate subzone from audit entries and must be exported alongside
// them, or a restored incremental backup cannot reconstruct the hash chain.
func ExportAuditItemSegmentKey(bucketID string, startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s/backups/exports/audit-items-%020d-%020d.bin", bucketID, startSeq, endSeq)
}

// ExportAppliedProposalSegmentKey returns the S3 key for an AppliedProposal
// export segment. AppliedProposal sequences are 1:1 with AuditEntry on the
// success path, so the segment shares the audit range — restoring a backup
// without this segment would leave the index builder unable to learn the
// transient-account exclusion set for replayed logs.
func ExportAppliedProposalSegmentKey(bucketID string, startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s/backups/exports/applied-proposals-%020d-%020d.bin", bucketID, startSeq, endSeq)
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

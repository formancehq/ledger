package backup

import "fmt"

// ExportLogSegmentKey returns the S3 key for a log export segment.
func ExportLogSegmentKey(bucketID string, startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s/backups/exports/logs-%020d-%020d.bin", bucketID, startSeq, endSeq)
}

// ExportAuditSegmentKey returns the S3 key for an audit export segment.
func ExportAuditSegmentKey(bucketID string, startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s/backups/exports/audit-%020d-%020d.bin", bucketID, startSeq, endSeq)
}

// CheckpointFileKey returns the S3 key for a checkpoint file.
func CheckpointFileKey(bucketID, filename string) string {
	return bucketID + "/backups/data/" + filename
}

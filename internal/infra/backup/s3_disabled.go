//go:build !s3

package backup

import "errors"

// NewS3BackupStorage returns an error when the s3 build tag is not set.
func NewS3BackupStorage(_, _, _ string) (Storage, error) {
	return nil, errors.New("S3 backup storage not available: build without the 's3' tag")
}

//go:build !s3

package coldstorage

import "fmt"

// NewS3ColdStorage returns an error when the s3 build tag is not set.
func NewS3ColdStorage(_, _, _ string) (ColdStorage, error) {
	return nil, fmt.Errorf("S3 cold storage not available: build without the 's3' tag")
}

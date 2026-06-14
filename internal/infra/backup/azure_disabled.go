//go:build !azure

package backup

import "errors"

// NewAzureBackupStorage returns an error when the azure build tag is not set.
func NewAzureBackupStorage(_, _, _, _ string) (Storage, error) {
	return nil, errors.New("backup driver 'azure' not available: rebuild with the 'azure' build tag")
}

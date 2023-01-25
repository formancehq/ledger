package dummypay

import (
	"github.com/spf13/afero"
)

type fs afero.Fs

// newFS creates a new file system access point.
func newFS() fs {
	return afero.NewOsFs()
}

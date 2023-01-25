package dummypay

import "github.com/spf13/afero"

func newTestFS() fs {
	return afero.NewMemMapFs()
}

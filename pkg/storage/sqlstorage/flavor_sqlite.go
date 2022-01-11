//go:build cgo
// +build cgo

// File is part of the build only if cgo is enabled.
// Otherwise, the compilation will complains about missing type sqlite3,Error
// It is due to the fact than sqlite lib use import "C" statement.
// The presence of these statement during the build exclude the file if CGO is disabled.
package sqlstorage

import (
	"github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/pkg/storage"
)

func init() {
	errorHandlers[SQLite] = func(err error) error {
		eerr, ok := err.(sqlite3.Error)
		if !ok {
			return err
		}
		if eerr.Code == sqlite3.ErrConstraint {
			return storage.NewError(storage.ConstraintFailed, err)
		}
		return err
	}
}

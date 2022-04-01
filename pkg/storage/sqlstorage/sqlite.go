//go:build cgo
// +build cgo

// File is part of the build only if cgo is enabled.
// Otherwise, the compilation will complains about missing type sqlite3,Error
// It is due to the fact than sqlite lib use import "C" statement.
// The presence of these statement during the build exclude the file if CGO is disabled.
package sqlstorage

import (
	"database/sql"
	"encoding/json"
	"github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"regexp"
)

func init() {
	errorHandlers[SQLite] = func(err error) error {
		eerr, ok := err.(sqlite3.Error)
		if !ok {
			return storage.NewError(storage.Unknown, err)
		}
		if eerr.Code == sqlite3.ErrConstraint {
			return storage.NewError(storage.ConstraintFailed, err)
		}
		return err
	}
	sql.Register("sqlite3-custom", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			err := conn.RegisterFunc("hash_log", func(v1, v2 string) string {
				m1 := make(map[string]interface{})
				m2 := make(map[string]interface{})
				err := json.Unmarshal([]byte(v1), &m1)
				if err != nil {
					panic(err)
				}
				err = json.Unmarshal([]byte(v2), &m2)
				if err != nil {
					panic(err)
				}
				return core.Hash(m1, m2)
			}, true)
			if err != nil {
				return err
			}
			err = conn.RegisterFunc("regexp", func(re, s string) (bool, error) {
				b, e := regexp.MatchString(re, s)
				return b, e
			}, true)
			return err
		},
	})
	UpdateSQLDriverMapping(SQLite, "sqlite3-custom")
}

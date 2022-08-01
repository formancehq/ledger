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
	"regexp"
	"strconv"

	"github.com/buger/jsonparser"
	"github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/pkg/core"
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
			if err != nil {
				return err
			}
			err = conn.RegisterFunc("use_account", func(v string, act string) (bool, error) {
				r, err := regexp.Compile(act)
				if err != nil {
					return false, err
				}
				postings := core.Postings{}
				err = json.Unmarshal([]byte(v), &postings)
				if err != nil {
					return false, nil
				}
				for _, p := range postings {
					if r.MatchString(p.Source) || r.MatchString(p.Destination) {
						return true, nil
					}
				}
				return false, nil
			}, true)
			if err != nil {
				return err
			}
			err = conn.RegisterFunc("use_account_as_source", func(v string, act string) (bool, error) {
				r, err := regexp.Compile(act)
				if err != nil {
					return false, err
				}
				postings := core.Postings{}
				err = json.Unmarshal([]byte(v), &postings)
				if err != nil {
					return false, nil
				}
				for _, p := range postings {
					if r.MatchString(p.Source) {
						return true, nil
					}
				}
				return false, nil
			}, true)
			if err != nil {
				return err
			}
			err = conn.RegisterFunc("use_account_as_destination", func(v string, act string) (bool, error) {
				r, err := regexp.Compile(act)
				if err != nil {
					return false, err
				}
				postings := core.Postings{}
				err = json.Unmarshal([]byte(v), &postings)
				if err != nil {
					return false, nil
				}
				for _, p := range postings {
					if r.MatchString(p.Destination) {
						return true, nil
					}
				}
				return false, nil
			}, true)
			if err != nil {
				return err
			}
			err = conn.RegisterFunc(SQLCustomFuncMetaCompare, func(metadata string, value string, key ...string) bool {
				bytes, dataType, _, err := jsonparser.Get([]byte(metadata), key...)
				if err != nil {
					return false
				}
				switch dataType {
				case jsonparser.String:
					str, err := jsonparser.ParseString(bytes)
					if err != nil {
						return false
					}
					return value == str
				case jsonparser.Boolean:
					b, err := jsonparser.ParseBoolean(bytes)
					if err != nil {
						return false
					}
					switch value {
					case "true":
						return b
					case "false":
						return !b
					}
					return false
				case jsonparser.Number:
					i, err := jsonparser.ParseInt(bytes)
					if err != nil {
						return false
					}
					vi, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return false
					}
					return i == vi
				default:
					return false
				}
			}, true)
			return err
		},
	})
	UpdateSQLDriverMapping(SQLite, "sqlite3-custom")
}

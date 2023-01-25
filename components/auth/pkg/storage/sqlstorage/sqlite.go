//go:build cgo
// +build cgo

package sqlstorage

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const (
	KindSqlite = "sqlite"
)

func OpenSQLiteDatabase(uri string) gorm.Dialector {
	return sqlite.Open(uri)
}

func init() {
	registerDriverConstructor(KindSqlite, OpenSQLiteDatabase)
}

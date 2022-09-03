package database

import (
	"context"
	"database/sql"

	"github.com/numary/ledger/it/internal/pgserver"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
)

var (
	actualDatabaseName string
	driver             *sqlstorage.Driver
)

func ActualDatabaseName() string {
	return actualDatabaseName
}

func StorageDriver() *sqlstorage.Driver {
	return driver
}

func WithNewDatabase(callback func()) {
	var (
		oldDatabaseName string
	)
	BeforeEach(func() {
		oldDatabaseName = actualDatabaseName
		actualDatabaseName = uuid.New()

		// TODO: Break this dependency
		_ = pgserver.CreateDatabase(actualDatabaseName)

		sqlDB, err := sql.Open("pgx", pgserver.ConnString(actualDatabaseName))
		Expect(err).To(BeNil())

		driver = sqlstorage.NewDriver("postgres", sqlstorage.NewPostgresDB(sqlDB))
		Expect(driver.Initialize(context.Background())).To(BeNil())
	})
	AfterEach(func() {
		actualDatabaseName = oldDatabaseName
	})
	callback()
}

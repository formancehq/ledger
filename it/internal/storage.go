package internal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/numary/ledger/it/internal/pgserver"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	. "github.com/onsi/gomega"
)

var (
	driver *sqlstorage.Driver
)

func SetDatabase(database string) {
	fmt.Println("storage driver connect to", pgserver.ConnString(database))
	sqlDB, err := sql.Open("pgx", pgserver.ConnString(database))
	Expect(err).To(BeNil())

	driver = sqlstorage.NewDriver("postgres", sqlstorage.NewPostgresDB(sqlDB))
	Expect(driver.Initialize(context.Background())).To(BeNil())
}

func StorageDriver() *sqlstorage.Driver {
	return driver
}

func GetLedgerStore() *sqlstorage.Store {
	store, _, err := StorageDriver().GetLedgerStore(context.Background(), CurrentLedger(), true)
	Expect(err).To(BeNil())
	return store
}

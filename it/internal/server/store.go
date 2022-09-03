package server

import (
	"context"

	. "github.com/numary/ledger/it/internal/database"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	. "github.com/onsi/gomega"
)

func GetLedgerStore() *sqlstorage.Store {
	store, _, err := StorageDriver().GetLedgerStore(context.Background(), CurrentLedger(), true)
	Expect(err).To(BeNil())
	_, err = store.Initialize(context.Background())
	Expect(err).To(BeNil())
	return store
}

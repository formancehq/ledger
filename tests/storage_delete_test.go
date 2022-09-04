package tests

import (
	"context"

	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/numary/ledger/tests/internal/database"
	"github.com/numary/ledger/tests/internal/pgserver"
	. "github.com/numary/ledger/tests/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Delete storage", func() {
	NewDatabase(func() {
		WithNewLedger(func() {
			BeforeEach(func() {
				// Force store creation
				_ = GetLedgerStore()
			})
			NewCommand(func() {
				BeforeEach(func() {
					AppendArgs("storage", "delete", CurrentLedger(),
						Flag(cmd.StorageDriverFlag, "postgres"),
						Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
					)
				})
				WhenExecuteCommand("storage delete", func() {
					It("should delete the storage on database", func() {
						Eventually(CommandTerminated).Should(BeTrue())
						Expect(CommandError()).Should(BeNil())

						ledgers, err := StorageDriver().GetSystemStore().ListLedgers(context.Background())
						Expect(err).To(BeNil())
						Expect(ledgers).To(HaveLen(0))
					})
				})
			})
		})
	})
})

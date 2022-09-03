package suite

import (
	"context"

	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/it/internal"
	"github.com/numary/ledger/it/internal/pgserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
)

var _ = Describe("Delete storage", func() {
	WithNewDatabase(func() {
		Describe("Given one registered ledger", func() {
			var (
				ledgerName string
			)
			BeforeEach(func() {
				ledgerName = uuid.New()
				_, _, err := StorageDriver().GetLedgerStore(context.Background(), ledgerName, true)
				Expect(err).To(BeNil())
			})
			PrepareCommand(func() {
				BeforeEach(func() {
					AppendArgs("storage", "delete", ledgerName,
						Flag(cmd.StorageDriverFlag, "postgres"),
						Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
					)
				})
				WhenExecuteCommand("deleting storage", func() {
					It("Should delete the storage on database", func() {
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

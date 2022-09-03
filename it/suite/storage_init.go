package suite

import (
	"context"

	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/it/internal/command"
	. "github.com/numary/ledger/it/internal/database"
	"github.com/numary/ledger/it/internal/pgserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Init storage", func() {
	WithNewDatabase(func() {
		PrepareCommand(func() {
			BeforeEach(func() {
				AppendArgs("storage", "init",
					Flag(cmd.StorageDriverFlag, "postgres"),
					Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
				)
			})
			WhenExecuteCommand("initializing storage using CLI", func() {
				It("Should be ok", func() {
					Eventually(CommandTerminated).Should(BeTrue())
					Expect(CommandError()).Should(BeNil())

					ledgers, err := StorageDriver().GetSystemStore().ListLedgers(context.Background())
					Expect(err).To(BeNil())
					Expect(ledgers).To(HaveLen(1))
				})
			})
		})
	})
})

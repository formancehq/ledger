package tests

import (
	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/numary/ledger/tests/internal/database"
	"github.com/numary/ledger/tests/internal/pgserver"
	. "github.com/numary/ledger/tests/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Upgrade storage", func() {
	NewDatabase(func() {
		WithNewLedger(func() {
			BeforeEach(func() {
				// Force store creation
				_ = GetLedgerStore()
			})
			NewCommand(func() {
				BeforeEach(func() {
					AppendArgs("storage", "upgrade", CurrentLedger(),
						Flag(cmd.StorageDriverFlag, "postgres"),
						Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
					)
				})
				WhenExecuteCommand("storage upgrade", func() {
					It("should be ok", func() {
						Eventually(CommandTerminated).Should(BeTrue())
						Expect(CommandError()).Should(BeNil())
					})
				})
			})
		})
	})
})

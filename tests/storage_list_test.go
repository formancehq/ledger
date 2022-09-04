package tests

import (
	"fmt"
	"io"

	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/numary/ledger/tests/internal/database"
	"github.com/numary/ledger/tests/internal/pgserver"
	. "github.com/numary/ledger/tests/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("List storages", func() {
	NewDatabase(func() {
		WithNewLedger(func() {
			BeforeEach(func() {
				// Force store creation
				_ = GetLedgerStore()
			})
			NewCommand(func() {
				BeforeEach(func() {
					AppendArgs("storage", "list",
						Flag(cmd.StorageDriverFlag, "postgres"),
						Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
					)
				})
				WhenExecuteCommand("storage list", func() {
					It("should return one ledger", func() {
						Eventually(CommandTerminated).Should(BeTrue())
						Expect(CommandError()).Should(BeNil())

						data, err := io.ReadAll(CommandStdout())
						Expect(err).To(BeNil())
						Expect(string(data)).To(Equal(fmt.Sprintf("Ledgers:\n- %s\n", CurrentLedger())))
					})
				})
			})
		})
	})
})

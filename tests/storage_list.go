package tests

import (
	"context"
	"fmt"
	"io"

	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/numary/ledger/tests/internal/database"
	"github.com/numary/ledger/tests/internal/pgserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
)

var _ = Describe("List storages", func() {
	NewDatabase(func() {
		Describe("Given one registered ledger", func() {
			var (
				ledgerName string
			)
			BeforeEach(func() {
				ledgerName = uuid.New()
				_, _, err := StorageDriver().GetLedgerStore(context.Background(), ledgerName, true)
				Expect(err).To(BeNil())
			})
			NewCommand(func() {
				BeforeEach(func() {
					AppendArgs("storage", "list",
						Flag(cmd.StorageDriverFlag, "postgres"),
						Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
					)
				})
				WhenExecuteCommand("listings storages", func() {
					It("Should return one ledger", func() {
						Eventually(CommandTerminated).Should(BeTrue())
						Expect(CommandError()).Should(BeNil())

						data, err := io.ReadAll(CommandStdout())
						Expect(err).To(BeNil())
						Expect(string(data)).To(Equal(fmt.Sprintf("Ledgers:\n- %s\n", ledgerName)))
					})
				})
			})
		})
	})
})

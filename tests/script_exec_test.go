package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/numary/ledger/cmd"
	"github.com/numary/ledger/pkg/ledger"
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/numary/ledger/tests/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const numscript = `send [COIN 100] (
  source = @world
  destination = @centralbank
)`

var _ = DescribeServerExecute("Execute numscript", func() {
	var port int
	BeforeEach(func() {
		port = cmd.Port(ActualCommand().Context())
	})
	WithNewLedger(func() {
		NewCommand(func() {
			var (
				path string
			)
			BeforeEach(func() {
				path = filepath.Join(os.TempDir(), "numscript.num")
				Expect(os.WriteFile(path, []byte(numscript), 0666)).To(BeNil())
				AppendArgs("exec", CurrentLedger(), path,
					Flag(cmd.ServerHttpBindAddressFlag, fmt.Sprintf("localhost:%d", port)))
			})
			AfterEach(func() {
				Expect(os.RemoveAll(path)).To(BeNil())
			})
			ExecuteCommand(func() {
				It("Should create a transaction on database", func() {
					Eventually(CommandTerminated).Should(BeTrue())
					Expect(CommandError()).Should(BeNil())

					count, err := GetLedgerStore().CountTransactions(context.Background(), *ledger.NewTransactionsQuery())
					Expect(err).To(BeNil())
					Expect(count).To(Equal(uint64(1)))
				})
			})
		})
	})
})

package suite

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/it/internal"
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
		WithCommand(func() {
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
			Execute(func() {
				It("Should be ok", func() {
					Eventually(Terminated).Should(BeTrue())
					Expect(Error()).Should(BeNil())
				})
			})
		})
	})
})

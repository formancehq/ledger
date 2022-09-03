package suite

import (
	"os"
	"path/filepath"

	. "github.com/numary/ledger/it/internal/command"
	. "github.com/numary/ledger/it/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeServerExecute("Check numscript", func() {
	NewCommand(func() {
		var (
			path string
		)
		BeforeEach(func() {
			path = filepath.Join(os.TempDir(), "numscript.num")
			Expect(os.WriteFile(path, []byte(`send [COIN 100] (
				  source = @world
				  destination = @centralbank
				)`), 0666)).To(BeNil())
			AppendArgs("check", path)
		})
		AfterEach(func() {
			Expect(os.RemoveAll(path)).To(BeNil())
		})
		ExecuteCommand(func() {
			It("Should be ok", func() {
				Eventually(CommandTerminated).Should(BeTrue())
				Expect(CommandError()).Should(BeNil())
			})
		})
	})
})

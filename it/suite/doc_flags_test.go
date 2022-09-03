package suite

import (
	. "github.com/numary/ledger/it/internal/command"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("List flags", func() {
	PrepareCommand(func() {
		BeforeEach(func() {
			AppendArgs("doc", "flags")
		})
		WhenExecuteCommand("doc flags", func() {
			It("Should be ok", func() {
				Eventually(CommandTerminated).Should(BeTrue())
				Expect(CommandError()).Should(BeNil())
			})
		})
	})
})

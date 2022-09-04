package tests

import (
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("List flags", func() {
	NewCommand(func() {
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

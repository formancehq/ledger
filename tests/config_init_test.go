package tests

import (
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
)

var _ = Describe("Init config", func() {
	NewCommand(func() {
		BeforeEach(func() {
			AppendArgs("config", "init")
		})
		WhenExecuteCommand("init config", func() {
			It("Should be ok", func() {
				Eventually(CommandTerminated).Should(BeTrue())
				Expect(CommandError()).Should(
					Or(
						BeNil(),
						BeAssignableToTypeOf(viper.ConfigFileAlreadyExistsError("")),
					),
				)
			})
		})
	})
})

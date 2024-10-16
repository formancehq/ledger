//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/logging"
	. "github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger integration tests", func() {
	var (
		db  = UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	testServer := testserver.UseNewTestServer(func() testserver.Configuration {
		return testserver.Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
		}
	})
	When("Starting the ledger", func() {
		It("Should be ok", func() {
			info, err := testServer.GetValue().Client().Ledger.V2.GetInfo(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.V2ConfigInfoResponse.Version).To(Equal("develop"))
		})
	})
})

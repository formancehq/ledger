//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
			NatsURL:               natsServer.GetValue().ClientURL(),
		}
	})

	When("creating 10 ledger", func() {
		BeforeEach(func() {
			for i := range 10 {
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: fmt.Sprintf("ledger%d", i),
				})
				Expect(err).To(BeNil())
			}
		})
		It("should be listable", func() {
			ledgers, err := ListLedgers(ctx, testServer.GetValue(), operations.V2ListLedgersRequest{})
			Expect(err).To(BeNil())
			Expect(ledgers.Data).To(HaveLen(10))
		})
	})
})

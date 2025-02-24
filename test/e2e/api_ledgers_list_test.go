//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			CommonConfiguration: CommonConfiguration{
				PostgresConfiguration: db.GetValue().ConnectionOptions(),
				Output:                GinkgoWriter,
				Debug:                 debug,
			},
			NatsURL:         natsServer.GetValue().ClientURL(),
			MaxPageSize:     5,
			DefaultPageSize: 5,
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
			ledgers, err := ListLedgers(ctx, testServer.GetValue(), operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
			})
			Expect(err).To(BeNil())
			Expect(ledgers.Data).To(HaveLen(5))
		})
	})
})

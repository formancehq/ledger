//go:build it

package test_suite

import (
	"database/sql"
	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/time"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/uptrace/bun"
	"math/big"
)

var _ = Context("API - Timeout", func() {
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
			APIResponseTimeout:    time.Second,
		}
	})

	When("creating a ledger", func() {
		BeforeEach(func() {
			err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
				Ledger: "foo",
			})
			Expect(err).ToNot(HaveOccurred())
		})
		Context("triggering a request longer than the timeout", func() {
			var (
				sqlTx bun.Tx
				err   error
			)
			BeforeEach(func() {
				// lock logs table to block transactions creation requests and trigger a timeout
				db := testServer.GetValue().Database()
				sqlTx, err = db.BeginTx(ctx, &sql.TxOptions{})
				Expect(err).To(BeNil())
				DeferCleanup(func() {
					_ = sqlTx.Rollback()
				})

				_, err = sqlTx.NewRaw("lock table _default.logs").Exec(ctx)
				Expect(err).To(BeNil())

				_, err = CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
					Ledger: "foo",
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Destination: "bank",
							Source:      "world",
						}},
					},
				})
			})
			It("should respond with a 504", func() {
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumTimeout)))
			})
		})
	})
})

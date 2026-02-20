//go:build it

package test_suite

import (
	"fmt"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/pointer"
	. "github.com/formancehq/go-libs/v4/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Ledger transactions list API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	JustBeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})
	const (
		pageSize = int64(10)
		txCount  = 2 * pageSize
	)
	When(fmt.Sprintf("creating %d transactions", txCount), func() {
		var (
			timestamp    = time.Now()
			transactions []components.Transaction
		)
		JustBeforeEach(func(specContext SpecContext) {
			for i := 0; i < int(txCount); i++ {
				offset := time.Duration(int(txCount)-i) * time.Minute
				// 1 transaction of 2 is backdated to test pagination using effective date
				if offset%2 == 0 {
					offset += 1
				} else {
					offset -= 1
				}
				txTimestamp := timestamp.Add(-offset)

				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.CreateTransaction(
					ctx,
					operations.CreateTransactionRequest{
						PostTransaction: components.PostTransaction{
							Metadata: map[string]any{},
							Postings: []components.Posting{
								{
									Amount:      big.NewInt(100),
									Asset:       "USD",
									Source:      "world",
									Destination: fmt.Sprintf("account:%d", i),
								},
								{
									Amount:      big.NewInt(100),
									Asset:       "EUR",
									Source:      "world",
									Destination: fmt.Sprintf("account:%d", i),
								},
							},
							Timestamp: pointer.For(txTimestamp),
							Reference: pointer.For(fmt.Sprintf("ref-%d", i)),
						},
						Ledger: "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				transactions = append([]components.Transaction{
					response.TransactionsResponse.Data[0],
				}, transactions...)
			}
		})
		AfterEach(func() {
			transactions = nil
		})
		When("listing transactions using a page size of 5", func() {
			var (
				rsp *operations.ListTransactionsResponse
				err error
			)
			JustBeforeEach(func(specContext SpecContext) {
				rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListTransactions(
					ctx,
					operations.ListTransactionsRequest{
						Ledger:   "default",
						PageSize: pointer.For(int64(5)),
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			When("using next page with a page size of 10", func() {
				JustBeforeEach(func(specContext SpecContext) {
					rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListTransactions(
						ctx,
						operations.ListTransactionsRequest{
							Ledger:   "default",
							Cursor:   rsp.TransactionsCursorResponse.Cursor.Next,
							PageSize: pointer.For(int64(10)),
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should return 10 elements", func() {
					Expect(rsp.TransactionsCursorResponse.Cursor.Data).To(HaveLen(10))
				})
			})
		})
		When(fmt.Sprintf("listing transactions using page size of %d", pageSize), func() {
			var (
				rsp *operations.ListTransactionsResponse
				req operations.ListTransactionsRequest
				err error
			)
			BeforeEach(func() {
				req = operations.ListTransactionsRequest{
					Ledger:   "default",
					PageSize: pointer.For(pageSize),
					EndTime:  pointer.For(time.Now()),
					Source:   pointer.For("world"),
				}
			})
			JustBeforeEach(func(specContext SpecContext) {
				rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListTransactions(ctx, req)
				Expect(err).ToNot(HaveOccurred())
			})
			Context("with a filter on reference", func() {
				BeforeEach(func() {
					req.Reference = pointer.For("ref-0")
				})
				It("Should be ok, and returns transactions with reference 'ref-0'", func() {
					Expect(rsp.TransactionsCursorResponse.Cursor.Data).To(HaveLen(1))
					Expect(rsp.TransactionsCursorResponse.Cursor.Data[0]).To(Equal(transactions[txCount-1]))
				})
			})
			It("Should be ok", func() {
				Expect(rsp.TransactionsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
				Expect(rsp.TransactionsCursorResponse.Cursor.Data).To(Equal(transactions[:pageSize]))
			})
			When("following next cursor", func() {
				JustBeforeEach(func(specContext SpecContext) {
					Expect(rsp.TransactionsCursorResponse.Cursor.HasMore).To(BeTrue())
					Expect(rsp.TransactionsCursorResponse.Cursor.Previous).To(BeNil())
					Expect(rsp.TransactionsCursorResponse.Cursor.Next).NotTo(BeNil())

					rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListTransactions(
						ctx,
						operations.ListTransactionsRequest{
							Cursor: rsp.TransactionsCursorResponse.Cursor.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(rsp.TransactionsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
					Expect(rsp.TransactionsCursorResponse.Cursor.Data).To(Equal(transactions[pageSize : 2*pageSize]))
					Expect(rsp.TransactionsCursorResponse.Cursor.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					JustBeforeEach(func(specContext SpecContext) {
						var err error
						rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListTransactions(
							ctx,
							operations.ListTransactionsRequest{
								Cursor: rsp.TransactionsCursorResponse.Cursor.Previous,
								Ledger: "default",
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(rsp.TransactionsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
						Expect(rsp.TransactionsCursorResponse.Cursor.Data).To(Equal(transactions[:pageSize]))
						Expect(rsp.TransactionsCursorResponse.Cursor.Previous).To(BeNil())
					})
				})
			})
		})
	})
})

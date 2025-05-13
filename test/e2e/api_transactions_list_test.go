//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/query"
	. "github.com/formancehq/go-libs/v2/testing/api"
	libtime "github.com/formancehq/go-libs/v2/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math/big"
	"slices"
	"sort"
	"time"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
)

var _ = Context("Ledger transactions list API tests", func() {
	var (
		db  = UseTemplatedDatabase()
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
	JustBeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
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
			transactions []components.V2Transaction
		)
		JustBeforeEach(func() {
			for i := 0; i < int(txCount); i++ {
				offset := time.Duration(int(txCount)-i) * time.Minute
				// 1 transaction of 2 is backdated to test pagination using effective date
				if offset%2 == 0 {
					offset += 1
				} else {
					offset -= 1
				}
				txTimestamp := timestamp.Add(-offset)

				response, err := CreateTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2CreateTransactionRequest{
						V2PostTransaction: components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{
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

				transactions = append([]components.V2Transaction{
					*response,
				}, transactions...)
			}
		})
		AfterEach(func() {
			transactions = nil
		})
		When("listing transaction using reverse option", func() {
			var (
				rsp *components.V2TransactionsCursorResponseCursor
				err error
			)
			JustBeforeEach(func(specContext SpecContext) {
				rsp, err = ListTransactions(
					ctx,
					testServer.GetValue(),
					operations.V2ListTransactionsRequest{
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
						Expand:   pointer.For("volumes,effectiveVolumes"),
						Reverse:  pointer.For(true),
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should be ok", func() {
				expectedTxs := transactions[pageSize:]
				slices.Reverse(expectedTxs)
				Expect(rsp.Data).To(Equal(expectedTxs))
			})
		})
		When("listing transactions using a page size of 5", func() {
			var (
				rsp *components.V2TransactionsCursorResponseCursor
				err error
			)
			JustBeforeEach(func() {
				rsp, err = ListTransactions(
					ctx,
					testServer.GetValue(),
					operations.V2ListTransactionsRequest{
						Ledger:   "default",
						PageSize: pointer.For(int64(5)),
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			When("using next page with a page size of 10", func() {
				JustBeforeEach(func() {
					rsp, err = ListTransactions(
						ctx,
						testServer.GetValue(),
						operations.V2ListTransactionsRequest{
							Ledger:   "default",
							Cursor:   rsp.Next,
							PageSize: pointer.For(int64(10)),
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should return 10 elements", func() {
					Expect(rsp.Data).To(HaveLen(10))
				})
			})
		})
		When(fmt.Sprintf("listing transactions using page size of %d", pageSize), func() {
			var (
				rsp *components.V2TransactionsCursorResponseCursor
				req operations.V2ListTransactionsRequest
				err error
			)
			BeforeEach(func() {
				req = operations.V2ListTransactionsRequest{
					Ledger:   "default",
					PageSize: pointer.For(pageSize),
					Expand:   pointer.For("volumes,effectiveVolumes"),
					Pit:      pointer.For(time.Now()),
					RequestBody: map[string]any{
						"$and": []map[string]any{
							{
								"$match": map[string]any{
									"source": "world",
								},
							},
							{
								"$not": map[string]any{
									"$exists": map[string]any{
										"metadata": "foo",
									},
								},
							},
						},
					},
				}
			})
			JustBeforeEach(func() {
				rsp, err = ListTransactions(ctx, testServer.GetValue(), req)
				Expect(err).ToNot(HaveOccurred())
			})
			Context("with a filter on reference", func() {
				BeforeEach(func() {
					req.RequestBody = map[string]any{
						"$match": map[string]any{
							"reference": "ref-0",
						},
					}
				})
				It("Should be ok, and returns transactions with reference 'ref-0'", func() {
					Expect(rsp.Data).To(HaveLen(1))
					Expect(rsp.Data[0]).To(Equal(transactions[txCount-1]))
				})
			})
			Context("with effective ordering", func() {
				BeforeEach(func() {
					req.Order = pointer.For(operations.OrderEffective)
				})
				It("Should be ok, and returns transactions ordered by effective timestamp", func() {
					Expect(rsp.PageSize).To(Equal(pageSize))
					sorted := transactions[:pageSize]
					sort.SliceStable(sorted, func(i, j int) bool {
						return sorted[i].Timestamp.After(sorted[j].Timestamp)
					})
					Expect(rsp.Data).To(Equal(sorted))
				})
			})
			It("Should be ok", func() {
				Expect(rsp.PageSize).To(Equal(pageSize))
				Expect(rsp.Data).To(Equal(transactions[:pageSize]))
			})
			When("following next cursor", func() {
				JustBeforeEach(func() {
					Expect(rsp.HasMore).To(BeTrue())
					Expect(rsp.Previous).To(BeNil())
					Expect(rsp.Next).NotTo(BeNil())

					// Create a new transaction to ensure cursor is stable
					_, err := CreateTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2CreateTransactionRequest{
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{},
								Postings: []components.V2Posting{
									{
										Amount:      big.NewInt(100),
										Asset:       "USD",
										Source:      "world",
										Destination: "account:0",
									},
								},
								Timestamp: pointer.For(time.Now()),
							},
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())

					rsp, err = ListTransactions(
						ctx,
						testServer.GetValue(),
						operations.V2ListTransactionsRequest{
							Cursor: rsp.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(rsp.PageSize).To(Equal(pageSize))
					Expect(rsp.Data).To(Equal(transactions[pageSize : 2*pageSize]))
					Expect(rsp.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					JustBeforeEach(func() {
						var err error
						rsp, err = ListTransactions(
							ctx,
							testServer.GetValue(),
							operations.V2ListTransactionsRequest{
								Cursor: rsp.Previous,
								Ledger: "default",
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(rsp.PageSize).To(Equal(pageSize))
						Expect(rsp.Data).To(Equal(transactions[:pageSize]))
						Expect(rsp.Previous).To(BeNil())
					})
				})
			})
		})

		When("listing transactions using filter on a single match", func() {
			var (
				err      error
				response *components.V2TransactionsCursorResponseCursor
				now      = time.Now().Round(time.Second).UTC()
			)
			JustBeforeEach(func() {
				response, err = ListTransactions(
					ctx,
					testServer.GetValue(),
					operations.V2ListTransactionsRequest{
						RequestBody: map[string]interface{}{
							"$match": map[string]any{
								"source": "world",
							},
						},
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
						Pit:      &now,
					},
				)
				Expect(err).To(BeNil())
			})
			It("Should be ok", func() {
				Expect(response.Next).NotTo(BeNil())
				cursor := &ledgercontroller.ColumnPaginatedQuery[any]{}
				Expect(bunpaginate.UnmarshalCursor(*response.Next, cursor)).To(BeNil())
				Expect(cursor.PageSize).To(Equal(uint64(10)))
				Expect(cursor.Options).To(Equal(ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("source", "world"),
					PIT:     pointer.For(libtime.New(now)),
				}))
			})
		})
		When("listing transactions using filter on a single match", func() {
			var (
				err      error
				response *components.V2TransactionsCursorResponseCursor
				now      = time.Now().Round(time.Second).UTC()
			)
			JustBeforeEach(func() {
				response, err = ListTransactions(
					ctx,
					testServer.GetValue(),
					operations.V2ListTransactionsRequest{
						RequestBody: map[string]interface{}{
							"$and": []map[string]any{
								{
									"$match": map[string]any{
										"source": "world",
									},
								},
								{
									"$match": map[string]any{
										"destination": "account:",
									},
								},
							},
						},
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
						Pit:      &now,
					},
				)
				Expect(err).To(BeNil())
			})
			It("Should be ok", func() {
				Expect(response.Next).NotTo(BeNil())
				cursor := &ledgercontroller.ColumnPaginatedQuery[any]{}
				Expect(bunpaginate.UnmarshalCursor(*response.Next, cursor)).To(BeNil())
				Expect(cursor.PageSize).To(Equal(uint64(10)))
				Expect(cursor.Options).To(Equal(ledgercontroller.ResourceQuery[any]{
					Builder: query.And(
						query.Match("source", "world"),
						query.Match("destination", "account:"),
					),
					PIT: pointer.For(libtime.New(now)),
				}))
			})
		})
		When("listing transactions using invalid filter", func() {
			var (
				err error
			)
			JustBeforeEach(func() {
				_, err = ListTransactions(
					ctx,
					testServer.GetValue(),
					operations.V2ListTransactionsRequest{
						RequestBody: map[string]interface{}{
							"$match": map[string]any{
								"invalid-key": 0,
							},
						},
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
					},
				)
				Expect(err).To(HaveOccurred())
			})
			It("Should fail with "+string(components.V2ErrorsEnumValidation)+" error code", func() {
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
			})
		})
	})
	var (
		timestamp1 = time.Date(2023, 4, 10, 10, 0, 0, 0, time.UTC)
		timestamp2 = time.Date(2023, 4, 11, 10, 0, 0, 0, time.UTC)
		timestamp3 = time.Date(2023, 4, 12, 10, 0, 0, 0, time.UTC)

		m1 = metadata.Metadata{
			"foo": "bar",
		}
	)

	var (
		t1  *components.V2Transaction
		t2  *components.V2Transaction
		t3  *components.V2Transaction
		err error
	)
	When("creating transactions", func() {
		JustBeforeEach(func() {
			t1, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: m1,
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "foo:foo",
							},
						},
						Timestamp: &timestamp1,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			t2, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: m1,
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "foo:bar",
							},
						},
						Timestamp: &timestamp2,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			t3, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "foo:baz",
							},
						},
						Timestamp: &timestamp3,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be countable on api", func() {
			response, err := CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(3))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "foo:",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(3))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "not_existing",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"destination": ":baz",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(1))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"destination": "not_existing",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"source": "foo:",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"source": "world",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(3))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"metadata[foo]": "bar",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(2))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"metadata[foo]": "not_existing",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$gte": map[string]any{
							"timestamp": timestamp2.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(2))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$gte": map[string]any{
							"timestamp": timestamp3.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(1))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$gte": map[string]any{
							"timestamp": time.Now().UTC().Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$lt": map[string]any{
							"timestamp": timestamp3.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(2))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$lt": map[string]any{
							"timestamp": timestamp2.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(1))

			response, err = CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$lt": map[string]any{
							"timestamp": time.Date(2023, 4, 9, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))
		})
		It("should be listed on api", func() {
			response, err := ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(3))
			Expect(response.Data[0]).Should(Equal(*t3))
			Expect(response.Data[1]).Should(Equal(*t2))
			Expect(response.Data[2]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "foo:",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(3))
			Expect(response.Data[0]).Should(Equal(*t3))
			Expect(response.Data[1]).Should(Equal(*t2))
			Expect(response.Data[2]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "not_existing",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(0))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"destination": "foo:",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(3))
			Expect(response.Data[0]).Should(Equal(*t3))
			Expect(response.Data[1]).Should(Equal(*t2))
			Expect(response.Data[2]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"destination": "not_existing",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(0))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"source": "foo:",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(0))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"source": "world",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(3))
			Expect(response.Data[0]).Should(Equal(*t3))
			Expect(response.Data[1]).Should(Equal(*t2))
			Expect(response.Data[2]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"metadata[foo]": "bar",
						},
					},
					Expand: pointer.For("volumes,effectiveVolumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(2))
			Expect(response.Data[0]).Should(Equal(*t2))
			Expect(response.Data[1]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"metadata[foo]": "not_existing",
						},
					},
					Expand: pointer.For("volumes,effectiveVolumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(0))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$gte": map[string]any{
							"timestamp": timestamp2.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(2))
			Expect(response.Data[0]).Should(Equal(*t3))
			Expect(response.Data[1]).Should(Equal(*t2))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$gte": map[string]any{
							"timestamp": timestamp3.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(1))
			Expect(response.Data[0]).Should(Equal(*t3))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$gte": map[string]any{
							"timestamp": time.Now().UTC().Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(0))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$lt": map[string]any{
							"timestamp": timestamp3.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(2))
			Expect(response.Data[0]).Should(Equal(*t2))
			Expect(response.Data[1]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$lt": map[string]any{
							"timestamp": timestamp2.Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(1))
			Expect(response.Data[0]).Should(Equal(*t1))

			response, err = ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes,effectiveVolumes"),
					RequestBody: map[string]interface{}{
						"$lt": map[string]any{
							"timestamp": time.Date(2023, 4, 9, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).Should(HaveLen(0))

			By("using $not operator on account 'world'", func() {
				response, err = ListTransactions(
					ctx,
					testServer.GetValue(),
					operations.V2ListTransactionsRequest{
						Ledger: "default",
						Expand: pointer.For("volumes,effectiveVolumes"),
						RequestBody: map[string]interface{}{
							"$not": map[string]any{
								"$match": map[string]any{
									"account": "foo:bar",
								},
							},
						},
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.Data).Should(HaveLen(2))
			})
		})
		It("should be gettable on api", func() {
			_, err := GetTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     t1.ID,
					Expand: pointer.For("volumes,effectiveVolumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = GetTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     t2.ID,
					Expand: pointer.For("volumes,effectiveVolumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = GetTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     t3.ID,
					Expand: pointer.For("volumes,effectiveVolumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = GetTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     big.NewInt(666),
				},
			)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumNotFound)))
		})
	})

	When("counting and listing transactions empty", func() {
		It("should be countable on api even if empty", func() {
			response, err := CountTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2CountTransactionsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).To(Equal(0))
		})
		It("should be listed on api even if empty", func() {
			response, err := ListTransactions(
				ctx,
				testServer.GetValue(),
				operations.V2ListTransactionsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Data).To(HaveLen(0))
		})
	})
})

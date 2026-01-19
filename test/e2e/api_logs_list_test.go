//go:build it

package test_suite

import (
	"fmt"
	"math/big"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Ledger logs list API tests", func() {
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

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "another",
		})
		Expect(err).To(BeNil())
	})
	When("listing logs", func() {
		var (
			timestamp1 = time.Date(2023, 4, 11, 10, 0, 0, 0, time.UTC)
			timestamp2 = time.Date(2023, 4, 12, 10, 0, 0, 0, time.UTC)

			m1 = map[string]string{
				"clientType": "silver",
			}
			m2 = map[string]string{
				"clientType": "gold",
			}
		)
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "world",
							Destination: "foo:foo",
						}},
						Timestamp: &timestamp1,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "world",
							Destination: "foo:foo",
						}},
						Timestamp: &timestamp1,
					},
					Ledger: "another",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: m1,
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(200),
							Asset:       "USD",
							Source:      "world",
							Destination: "foo:bar",
						}},
						Timestamp: &timestamp2,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: m2,
					Address:     "foo:baz",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		When("paginating on date", func() {
			It("should be ok", func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
					ctx,
					operations.V2ListLogsRequest{
						Ledger: "default",
						Sort:   pointer.For("date:asc"),
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
		})
		It("should be listed on api with ListLogs", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
				ctx,
				operations.V2ListLogsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2LogsCursorResponse.Cursor.Data).To(HaveLen(3))

			for _, data := range response.V2LogsCursorResponse.Cursor.Data {
				Expect(data.Hash).NotTo(BeEmpty())
			}
		})
		When("filtering logs by type", func() {
			It("should filter by NEW_TRANSACTION type", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
					ctx,
					operations.V2ListLogsRequest{
						Ledger: "default",
						RequestBody: map[string]interface{}{
							"$match": map[string]any{
								"type": "NEW_TRANSACTION",
							},
						},
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.V2LogsCursorResponse.Cursor.Data).To(HaveLen(2))
				for _, log := range response.V2LogsCursorResponse.Cursor.Data {
					Expect(log.Type).To(Equal(components.V2LogTypeNewTransaction))
				}
			})
			It("should filter by SET_METADATA type", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
					ctx,
					operations.V2ListLogsRequest{
						Ledger: "default",
						RequestBody: map[string]interface{}{
							"$match": map[string]any{
								"type": "SET_METADATA",
							},
						},
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.V2LogsCursorResponse.Cursor.Data).To(HaveLen(1))
				Expect(response.V2LogsCursorResponse.Cursor.Data[0].Type).To(Equal(components.V2LogTypeSetMetadata))
			})
			It("should return empty when filtering by non-existent type", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
					ctx,
					operations.V2ListLogsRequest{
						Ledger: "default",
						RequestBody: map[string]interface{}{
							"$match": map[string]any{
								"type": "REVERTED_TRANSACTION",
							},
						},
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.V2LogsCursorResponse.Cursor.Data).To(HaveLen(0))
			})
		})
		It("should be listed on api with ListLogs with details", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
				ctx,
				operations.V2ListLogsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2LogsCursorResponse.Cursor.Data).To(HaveLen(3))

			// Cannot check the date and the hash since they are changing at
			// every run
			Expect(response.V2LogsCursorResponse.Cursor.Data[0].ID).To(Equal(big.NewInt(3)))
			Expect(response.V2LogsCursorResponse.Cursor.Data[0].Type).To(Equal(components.V2LogTypeSetMetadata))
			Expect(response.V2LogsCursorResponse.Cursor.Data[0].Data.V2LogDataSetMetadata).NotTo(BeNil())
			Expect(response.V2LogsCursorResponse.Cursor.Data[0].Data.V2LogDataSetMetadata.TargetType).To(Equal(components.TargetTypeAccount))
			Expect(response.V2LogsCursorResponse.Cursor.Data[0].Data.V2LogDataSetMetadata.Metadata).To(Equal(map[string]string{
				"clientType": "gold",
			}))
			Expect(response.V2LogsCursorResponse.Cursor.Data[0].Data.V2LogDataSetMetadata.TargetID.Str).To(Equal(pointer.For("foo:baz")))

			Expect(response.V2LogsCursorResponse.Cursor.Data[1].ID).To(Equal(big.NewInt(2)))
			Expect(response.V2LogsCursorResponse.Cursor.Data[1].Type).To(Equal(components.V2LogTypeNewTransaction))
			// Cannot check date and txid inside Data since they are changing at
			// every run
			Expect(response.V2LogsCursorResponse.Cursor.Data[1].Data.V2LogDataNewTransaction).NotTo(BeNil())
			Expect(response.V2LogsCursorResponse.Cursor.Data[1].Data.V2LogDataNewTransaction.AccountMetadata).To(Equal(map[string]map[string]string{}))
			transaction := response.V2LogsCursorResponse.Cursor.Data[1].Data.V2LogDataNewTransaction.Transaction
			Expect(transaction.Metadata).To(Equal(map[string]string{
				"clientType": "silver",
			}))
			Expect(transaction.Timestamp.Format(time.RFC3339)).To(Equal("2023-04-12T10:00:00Z"))
			Expect(transaction.Postings).To(Equal([]components.V2Posting{
				{
					Amount:      big.NewInt(200),
					Asset:       "USD",
					Source:      "world",
					Destination: "foo:bar",
				},
			}))

			Expect(response.V2LogsCursorResponse.Cursor.Data[2].ID).To(Equal(big.NewInt(1)))
			Expect(response.V2LogsCursorResponse.Cursor.Data[2].Type).To(Equal(components.V2LogTypeNewTransaction))
			Expect(response.V2LogsCursorResponse.Cursor.Data[2].Data.V2LogDataNewTransaction).NotTo(BeNil())
			Expect(response.V2LogsCursorResponse.Cursor.Data[2].Data.V2LogDataNewTransaction.AccountMetadata).To(Equal(map[string]map[string]string{}))
			transaction = response.V2LogsCursorResponse.Cursor.Data[2].Data.V2LogDataNewTransaction.Transaction
			Expect(transaction.Metadata).To(Equal(map[string]string{}))
			Expect(transaction.Timestamp.Format(time.RFC3339)).To(Equal("2023-04-11T10:00:00Z"))
			Expect(transaction.Postings).To(Equal([]components.V2Posting{
				{
					Amount:      big.NewInt(100),
					Asset:       "USD",
					Source:      "world",
					Destination: "foo:foo",
				},
			}))
		})
	})

	type expectedLog struct {
		id       *big.Int
		typ      components.V2LogType
		postings []components.V2Posting
	}

	var (
		compareLogs = func(log components.V2Log, expected expectedLog) {
			Expect(log.ID).To(Equal(expected.id))
			Expect(log.Type).To(Equal(expected.typ))
			Expect(log.Data.V2LogDataNewTransaction).NotTo(BeNil())
			Expect(log.Data.V2LogDataNewTransaction.AccountMetadata).To(Equal(map[string]map[string]string{}))
			transaction := log.Data.V2LogDataNewTransaction.Transaction
			Expect(transaction.Metadata).To(Equal(map[string]string{}))
			Expect(transaction.Postings).To(Equal(expected.postings))
		}
	)

	const (
		pageSize      = int64(10)
		accountCounts = 2 * pageSize
	)
	When("creating logs with transactions", func() {
		var (
			expectedLogs []expectedLog
		)
		BeforeEach(func(specContext SpecContext) {
			for i := int64(0); i < accountCounts; i++ {
				now := time.Now().Round(time.Millisecond).UTC()

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
					ctx,
					operations.V2CreateTransactionRequest{
						V2PostTransaction: components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: fmt.Sprintf("foo:%d", i),
							}},
							Timestamp: &now,
						},
						Ledger: "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				expectedLogs = append(expectedLogs, expectedLog{
					id:  big.NewInt(i + 1),
					typ: components.V2LogTypeNewTransaction,
					postings: []components.V2Posting{
						{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "world",
							Destination: fmt.Sprintf("foo:%d", i),
						},
					},
				})
			}

			sort.Slice(expectedLogs, func(i, j int) bool {
				return expectedLogs[i].id.Cmp(expectedLogs[j].id) > 0
			})
		})
		AfterEach(func() {
			expectedLogs = nil
		})
		When(fmt.Sprintf("listing logs using page size of %d", pageSize), func() {
			var (
				rsp *operations.V2ListLogsResponse
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
					ctx,
					operations.V2ListLogsRequest{
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(rsp.V2LogsCursorResponse.Cursor.HasMore).To(BeTrue())
				Expect(rsp.V2LogsCursorResponse.Cursor.Previous).To(BeNil())
				Expect(rsp.V2LogsCursorResponse.Cursor.Next).NotTo(BeNil())
			})
			It("should return the first page", func() {
				Expect(rsp.V2LogsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
				Expect(len(rsp.V2LogsCursorResponse.Cursor.Data)).To(Equal(len(expectedLogs[:pageSize])))
				for i := range rsp.V2LogsCursorResponse.Cursor.Data {
					compareLogs(rsp.V2LogsCursorResponse.Cursor.Data[i], expectedLogs[i])
				}
			})
			When("following next cursor", func() {
				BeforeEach(func(specContext SpecContext) {
					rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
						ctx,
						operations.V2ListLogsRequest{
							Cursor: rsp.V2LogsCursorResponse.Cursor.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(rsp.V2LogsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
					Expect(len(rsp.V2LogsCursorResponse.Cursor.Data)).To(Equal(len(expectedLogs[pageSize : 2*pageSize])))
					for i := range rsp.V2LogsCursorResponse.Cursor.Data {
						compareLogs(rsp.V2LogsCursorResponse.Cursor.Data[i], expectedLogs[int64(i)+pageSize])
					}
					Expect(rsp.V2LogsCursorResponse.Cursor.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					BeforeEach(func(specContext SpecContext) {
						rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(
							ctx,
							operations.V2ListLogsRequest{
								Cursor: rsp.V2LogsCursorResponse.Cursor.Previous,
								Ledger: "default",
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(rsp.V2LogsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
						Expect(len(rsp.V2LogsCursorResponse.Cursor.Data)).To(Equal(len(expectedLogs[:pageSize])))
						for i := range rsp.V2LogsCursorResponse.Cursor.Data {
							compareLogs(rsp.V2LogsCursorResponse.Cursor.Data[i], expectedLogs[i])
						}
						Expect(rsp.V2LogsCursorResponse.Cursor.Previous).To(BeNil())
					})
				})
			})
		})
	})
})

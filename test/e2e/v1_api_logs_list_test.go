//go:build it

package test_suite

import (
	"fmt"
	"math/big"
	"sort"
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

			m1 = map[string]any{
				"clientType": "silver",
			}
			m2 = map[string]any{
				"clientType": "gold",
			}
		)
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.CreateTransaction(
				ctx,
				operations.CreateTransactionRequest{
					PostTransaction: components.PostTransaction{
						Metadata: map[string]any{},
						Postings: []components.Posting{{
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

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.CreateTransaction(
				ctx,
				operations.CreateTransactionRequest{
					PostTransaction: components.PostTransaction{
						Metadata: map[string]any{},
						Postings: []components.Posting{{
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

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.CreateTransaction(
				ctx,
				operations.CreateTransactionRequest{
					PostTransaction: components.PostTransaction{
						Metadata: m1,
						Postings: []components.Posting{{
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

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.AddMetadataToAccount(
				ctx,
				operations.AddMetadataToAccountRequest{
					RequestBody: m2,
					Address:     "foo:baz",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be listed on api with ListLogs", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.ListLogs(
				ctx,
				operations.ListLogsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.LogsCursorResponse.Cursor.Data).To(HaveLen(3))

			for _, data := range response.LogsCursorResponse.Cursor.Data {
				Expect(data.Hash).NotTo(BeEmpty())
			}

			// Cannot check the date and the hash since they are changing at
			// every run
			Expect(response.LogsCursorResponse.Cursor.Data[0].ID).To(Equal(int64(3)))
			Expect(response.LogsCursorResponse.Cursor.Data[0].Type).To(Equal(components.TypeSetMetadata))
			Expect(response.LogsCursorResponse.Cursor.Data[0].Data).To(Equal(map[string]any{
				"targetType": "ACCOUNT",
				"metadata": map[string]any{
					"clientType": "gold",
				},
				"targetId": "foo:baz",
			}))

			Expect(response.LogsCursorResponse.Cursor.Data[1].ID).To(Equal(int64(2)))
			Expect(response.LogsCursorResponse.Cursor.Data[1].Type).To(Equal(components.TypeNewTransaction))
			// Cannot check date and txid inside Data since they are changing at
			// every run
			Expect(response.LogsCursorResponse.Cursor.Data[1].Data["accountMetadata"]).To(Equal(map[string]any{}))
			Expect(response.LogsCursorResponse.Cursor.Data[1].Data["transaction"]).To(BeAssignableToTypeOf(map[string]any{}))
			transaction := response.LogsCursorResponse.Cursor.Data[1].Data["transaction"].(map[string]any)
			Expect(transaction["metadata"]).To(Equal(map[string]any{
				"clientType": "silver",
			}))
			Expect(transaction["timestamp"]).To(Equal("2023-04-12T10:00:00Z"))
			Expect(transaction["postings"]).To(Equal([]any{
				map[string]any{
					"amount":      float64(200),
					"asset":       "USD",
					"source":      "world",
					"destination": "foo:bar",
				},
			}))

			Expect(response.LogsCursorResponse.Cursor.Data[2].ID).To(Equal(int64(1)))
			Expect(response.LogsCursorResponse.Cursor.Data[2].Type).To(Equal(components.TypeNewTransaction))
			Expect(response.LogsCursorResponse.Cursor.Data[2].Data["accountMetadata"]).To(Equal(map[string]any{}))
			Expect(response.LogsCursorResponse.Cursor.Data[2].Data["transaction"]).To(BeAssignableToTypeOf(map[string]any{}))
			transaction = response.LogsCursorResponse.Cursor.Data[2].Data["transaction"].(map[string]any)
			Expect(transaction["metadata"]).To(Equal(map[string]any{}))
			Expect(transaction["timestamp"]).To(Equal("2023-04-11T10:00:00Z"))
			Expect(transaction["postings"]).To(Equal([]any{
				map[string]any{
					"amount":      float64(100),
					"asset":       "USD",
					"source":      "world",
					"destination": "foo:foo",
				},
			}))
		})
	})

	type expectedLog struct {
		id       *big.Int
		typ      components.Type
		postings []any
	}

	var (
		compareLogs = func(log components.Log, expected expectedLog) {
			Expect(log.ID).To(Equal(expected.id.Int64()))
			Expect(log.Type).To(Equal(expected.typ))
			Expect(log.Data["accountMetadata"]).To(Equal(map[string]any{}))
			Expect(log.Data["transaction"]).To(BeAssignableToTypeOf(map[string]any{}))
			transaction := log.Data["transaction"].(map[string]any)
			Expect(transaction["metadata"]).To(Equal(map[string]any{}))
			Expect(transaction["postings"]).To(Equal(expected.postings))
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

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.CreateTransaction(
					ctx,
					operations.CreateTransactionRequest{
						PostTransaction: components.PostTransaction{
							Metadata: map[string]any{},
							Postings: []components.Posting{{
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
					typ: components.TypeNewTransaction,
					postings: []any{
						map[string]any{
							"amount":      float64(100),
							"asset":       "USD",
							"source":      "world",
							"destination": fmt.Sprintf("foo:%d", i),
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
				rsp *operations.ListLogsResponse
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListLogs(
					ctx,
					operations.ListLogsRequest{
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(rsp.LogsCursorResponse.Cursor.HasMore).To(BeTrue())
				Expect(rsp.LogsCursorResponse.Cursor.Previous).To(BeNil())
				Expect(rsp.LogsCursorResponse.Cursor.Next).NotTo(BeNil())
			})
			It("should return the first page", func() {
				Expect(rsp.LogsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
				Expect(len(rsp.LogsCursorResponse.Cursor.Data)).To(Equal(len(expectedLogs[:pageSize])))
				for i := range rsp.LogsCursorResponse.Cursor.Data {
					compareLogs(rsp.LogsCursorResponse.Cursor.Data[i], expectedLogs[i])
				}
			})
			When("following next cursor", func() {
				BeforeEach(func(specContext SpecContext) {
					rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListLogs(
						ctx,
						operations.ListLogsRequest{
							Cursor: rsp.LogsCursorResponse.Cursor.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(rsp.LogsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
					Expect(len(rsp.LogsCursorResponse.Cursor.Data)).To(Equal(len(expectedLogs[pageSize : 2*pageSize])))
					for i := range rsp.LogsCursorResponse.Cursor.Data {
						compareLogs(rsp.LogsCursorResponse.Cursor.Data[i], expectedLogs[int64(i)+pageSize])
					}
					Expect(rsp.LogsCursorResponse.Cursor.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					BeforeEach(func(specContext SpecContext) {
						rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListLogs(
							ctx,
							operations.ListLogsRequest{
								Cursor: rsp.LogsCursorResponse.Cursor.Previous,
								Ledger: "default",
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(rsp.LogsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
						Expect(len(rsp.LogsCursorResponse.Cursor.Data)).To(Equal(len(expectedLogs[:pageSize])))
						for i := range rsp.LogsCursorResponse.Cursor.Data {
							compareLogs(rsp.LogsCursorResponse.Cursor.Data[i], expectedLogs[i])
						}
						Expect(rsp.LogsCursorResponse.Cursor.Previous).To(BeNil())
					})
				})
			})
		})
	})
})

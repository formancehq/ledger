//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math/big"
	"sort"
	"time"
)

var _ = Context("Ledger logs list API tests", func() {
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
	BeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		err = CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
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
		BeforeEach(func() {
			_, err := CreateTransaction(
				ctx,
				testServer.GetValue(),
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

			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
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

			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
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

			err = AddMetadataToAccount(
				ctx,
				testServer.GetValue(),
				operations.V2AddMetadataToAccountRequest{
					RequestBody: m2,
					Address:     "foo:baz",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be listed on api with ListLogs", func() {
			response, err := ListLogs(
				ctx,
				testServer.GetValue(),
				operations.V2ListLogsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.Data).To(HaveLen(3))

			// Cannot check the date and the hash since they are changing at
			// every run
			Expect(response.Data[0].ID).To(Equal(big.NewInt(3)))
			Expect(response.Data[0].Type).To(Equal(components.V2LogTypeSetMetadata))
			Expect(response.Data[0].Data).To(Equal(map[string]any{
				"targetType": "ACCOUNT",
				"metadata": map[string]any{
					"clientType": "gold",
				},
				"targetId": "foo:baz",
			}))

			Expect(response.Data[1].ID).To(Equal(big.NewInt(2)))
			Expect(response.Data[1].Type).To(Equal(components.V2LogTypeNewTransaction))
			// Cannot check date and txid inside Data since they are changing at
			// every run
			Expect(response.Data[1].Data["accountMetadata"]).To(Equal(map[string]any{}))
			Expect(response.Data[1].Data["transaction"]).To(BeAssignableToTypeOf(map[string]any{}))
			transaction := response.Data[1].Data["transaction"].(map[string]any)
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

			Expect(response.Data[2].ID).To(Equal(big.NewInt(1)))
			Expect(response.Data[2].Type).To(Equal(components.V2LogTypeNewTransaction))
			Expect(response.Data[2].Data["accountMetadata"]).To(Equal(map[string]any{}))
			Expect(response.Data[2].Data["transaction"]).To(BeAssignableToTypeOf(map[string]any{}))
			transaction = response.Data[2].Data["transaction"].(map[string]any)
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
		typ      components.V2LogType
		postings []any
	}

	var (
		compareLogs = func(log components.V2Log, expected expectedLog) {
			Expect(log.ID).To(Equal(expected.id))
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
		BeforeEach(func() {
			for i := int64(0); i < accountCounts; i++ {
				now := time.Now().Round(time.Millisecond).UTC()

				_, err := CreateTransaction(
					ctx,
					testServer.GetValue(),
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
		When(fmt.Sprintf("listing accounts using page size of %d", pageSize), func() {
			var (
				rsp *components.V2LogsCursorResponseCursor
				err error
			)
			BeforeEach(func() {
				rsp, err = ListLogs(
					ctx,
					testServer.GetValue(),
					operations.V2ListLogsRequest{
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(rsp.HasMore).To(BeTrue())
				Expect(rsp.Previous).To(BeNil())
				Expect(rsp.Next).NotTo(BeNil())
			})
			It("should return the first page", func() {
				Expect(rsp.PageSize).To(Equal(pageSize))
				Expect(len(rsp.Data)).To(Equal(len(expectedLogs[:pageSize])))
				for i := range rsp.Data {
					compareLogs(rsp.Data[i], expectedLogs[i])
				}
			})
			When("following next cursor", func() {
				BeforeEach(func() {
					rsp, err = ListLogs(
						ctx,
						testServer.GetValue(),
						operations.V2ListLogsRequest{
							Cursor: rsp.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(rsp.PageSize).To(Equal(pageSize))
					Expect(len(rsp.Data)).To(Equal(len(expectedLogs[pageSize : 2*pageSize])))
					for i := range rsp.Data {
						compareLogs(rsp.Data[i], expectedLogs[int64(i)+pageSize])
					}
					Expect(rsp.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					BeforeEach(func() {
						rsp, err = ListLogs(
							ctx,
							testServer.GetValue(),
							operations.V2ListLogsRequest{
								Cursor: rsp.Previous,
								Ledger: "default",
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(rsp.PageSize).To(Equal(pageSize))
						Expect(len(rsp.Data)).To(Equal(len(expectedLogs[:pageSize])))
						for i := range rsp.Data {
							compareLogs(rsp.Data[i], expectedLogs[i])
						}
						Expect(rsp.Previous).To(BeNil())
					})
				})
			})
		})
	})
})

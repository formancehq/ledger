//go:build it

package test_suite

import (
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"

	"github.com/formancehq/go-libs/v2/metadata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db          = UseTemplatedDatabase()
		ctx         = logging.TestingContext()
		bulkMaxSize = 5
	)

	for _, data := range []struct {
		description      string
		numscriptRewrite bool
	}{
		{"default", false},
		{"numscript rewrite", true},
	} {

		Context(data.description, func() {
			var (
				db          = pgtesting.UsePostgresDatabase(pgServer)
				ctx         = logging.TestingContext()
				bulkMaxSize = 5
			)

			testServer := NewTestServer(func() Configuration {
				return Configuration{
					PostgresConfiguration:        db.GetValue().ConnectionOptions(),
					Output:                       GinkgoWriter,
					Debug:                        debug,
					NatsURL:                      natsServer.GetValue().ClientURL(),
					BulkMaxSize:                  bulkMaxSize,
					ExperimentalNumscriptRewrite: data.numscriptRewrite,
				}
			})
			BeforeEach(func() {
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
			})
			When("creating a bulk on a ledger", func() {
				var (
					now   = time.Now().Round(time.Microsecond).UTC()
					items []components.V2BulkElement
					err   error
				)
				BeforeEach(func() {
					items = []components.V2BulkElement{
						components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
							Data: &components.V2PostTransaction{
								Metadata: map[string]string{},
								Postings: []components.V2Posting{{
									Amount:      big.NewInt(100),
									Asset:       "USD/2",
									Destination: "bank",
									Source:      "world",
								}},
								Timestamp: &now,
							},
						}),
						components.CreateV2BulkElementAddMetadata(components.V2BulkElementAddMetadata{
							Data: &components.Data{
								Metadata: metadata.Metadata{
									"foo":  "bar",
									"role": "admin",
								},
								TargetID:   components.CreateV2TargetIDBigint(big.NewInt(1)),
								TargetType: components.V2TargetTypeTransaction,
							},
						}),
						components.CreateV2BulkElementDeleteMetadata(components.V2BulkElementDeleteMetadata{
							Data: &components.V2BulkElementDeleteMetadataData{
								Key:        "foo",
								TargetID:   components.CreateV2TargetIDBigint(big.NewInt(1)),
								TargetType: components.V2TargetTypeTransaction,
							},
						}),
						components.CreateV2BulkElementRevertTransaction(components.V2BulkElementRevertTransaction{
							Data: &components.V2BulkElementRevertTransactionData{
								ID: big.NewInt(1),
							},
						}),
					}
				})
				JustBeforeEach(func() {
					_, err = CreateBulk(ctx, testServer.GetValue(), operations.V2CreateBulkRequest{
						RequestBody: items,
						Ledger:      "default",
					})
				})
				It("should be ok", func() {
					Expect(err).To(Succeed())

					tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
						ID:     big.NewInt(1),
						Ledger: "default",
					})
					Expect(err).To(Succeed())

					tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
						ID:     big.NewInt(1),
						Ledger: "default",
					})
					Expect(err).To(Succeed())
					reversedTx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
						ID:     big.NewInt(2),
						Ledger: "default",
					})
					Expect(err).To(Succeed())

					Expect(*tx).To(Equal(components.V2Transaction{
						ID: big.NewInt(1),
						Metadata: metadata.Metadata{
							"role": "admin",
						},
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD/2",
							Destination: "bank",
							Source:      "world",
						}},
						Reverted:   true,
						RevertedAt: &reversedTx.Timestamp,
						Timestamp:  now,
						InsertedAt: tx.InsertedAt,
					}))
				})
			})
			Context("with exceeded batch size", func() {
				BeforeEach(func() {
					items = make([]components.V2BulkElement, 0)
					for i := 0; i < bulkMaxSize+1; i++ {
						items = append(items, components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
							Data: &components.V2PostTransaction{
								Metadata: map[string]string{},
								Postings: []components.V2Posting{{
									Amount:      big.NewInt(100),
									Asset:       "USD/2",
									Destination: "bank",
									Source:      "world",
								}},
								Timestamp: &now,
							},
						}))
					}
				})
				It("should respond with an error", func() {
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumBulkSizeExceeded)))
				})
			})
			When("creating a bulk with an error on a ledger", func() {
				var (
					now          = time.Now().Round(time.Microsecond).UTC()
					err          error
					bulkResponse []components.V2BulkElementResult
				)
				BeforeEach(func() {
					bulkResponse, err = CreateBulk(ctx, testServer.GetValue(), operations.V2CreateBulkRequest{
						RequestBody: []components.V2BulkElement{
							components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
								Data: &components.V2PostTransaction{
									Metadata: map[string]string{},
									Postings: []components.V2Posting{{
										Amount:      big.NewInt(100),
										Asset:       "USD/2",
										Destination: "bank",
										Source:      "world",
									}},
									Timestamp: &now,
								},
							}),
							components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
								Data: &components.V2PostTransaction{
									Metadata: map[string]string{},
									Postings: []components.V2Posting{{
										Amount:      big.NewInt(200), // Insufficient fund
										Asset:       "USD/2",
										Destination: "user",
										Source:      "bank",
									}},
									Timestamp: &now,
								},
							}),
						},
						Ledger: "default",
					})
					Expect(err).To(Succeed())
				})
				It("should respond with an error", func() {
					var expectedErr string
					if data.numscriptRewrite {
						expectedErr = "INTERPRETER_RUNTIME"
					} else {
						expectedErr = "INSUFFICIENT_FUND"
					}
					Expect(bulkResponse[1].Type).To(Equal(components.V2BulkElementResultType("ERROR")))
					Expect(bulkResponse[1].V2BulkElementResultError.ErrorCode).To(Equal(expectedErr))
				})
			})
		})
	}

})

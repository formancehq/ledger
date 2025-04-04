//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	. "github.com/formancehq/go-libs/v2/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v2/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"math/big"
	"sort"
	"time"

	"github.com/formancehq/go-libs/v2/pointer"

	"github.com/formancehq/go-libs/v2/metadata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger accounts list API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
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
	})
	When("counting and listing accounts", func() {
		var (
			metadata1 = map[string]string{
				"clientType": "gold",
			}

			metadata2 = map[string]string{
				"clientType": "silver",
			}

			timestamp = time.Now().Round(time.Second).UTC()
			bigInt, _ = big.NewInt(0).SetString("999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)
		)
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata1,
					Address:     "foo:foo",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata2,
					Address:     "foo:bar",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{{
							Amount:      bigInt,
							Asset:       "USD",
							Source:      "world",
							Destination: "foo:foo",
						}},
						Timestamp: &timestamp,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should return a "+string(components.V2ErrorsEnumValidation)+" on invalid filter", func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"invalid-key": 0,
						},
					},
				},
			)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})
		It("should be countable on api", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CountAccounts(
				ctx,
				operations.V2CountAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Headers["Count"]).To(Equal([]string{"3"}))
		})
		It("should be listed on api", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
					Expand: pointer.For("volumes"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.V2AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(3))
			Expect(accountsCursorResponse[0]).To(Equal(components.V2Account{
				Address:  "foo:bar",
				Metadata: metadata2,
			}))
			Expect(accountsCursorResponse[1]).To(Equal(components.V2Account{
				Address:  "foo:foo",
				Metadata: metadata1,
				Volumes: map[string]components.V2Volume{
					"USD": {
						Input:   bigInt,
						Output:  big.NewInt(0),
						Balance: bigInt,
					},
				},
			}))
			Expect(accountsCursorResponse[2]).To(Equal(components.V2Account{
				Address:  "world",
				Metadata: metadata.Metadata{},
				Volumes: map[string]components.V2Volume{
					"USD": {
						Output:  bigInt,
						Input:   big.NewInt(0),
						Balance: big.NewInt(0).Neg(bigInt),
					},
				},
			}))
		})
		It("should be listed on api using address filters", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"address": "foo:",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.V2AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(2))
			Expect(accountsCursorResponse[0]).To(Equal(components.V2Account{
				Address:  "foo:bar",
				Metadata: metadata2,
			}))
			Expect(accountsCursorResponse[1]).To(Equal(components.V2Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))

			response, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"address": ":foo",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse = response.V2AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(1))
			Expect(accountsCursorResponse[0]).To(Equal(components.V2Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))
		})
		It("should be listed on api using metadata filters", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"metadata[clientType]": "gold",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.V2AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(1))
			Expect(accountsCursorResponse[0]).To(Equal(components.V2Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))
		})
		It("should be listable on api using $not filter", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
					RequestBody: map[string]interface{}{
						"$not": map[string]any{
							"$match": map[string]any{
								"address": "world",
							},
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.V2AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(2))
		})
	})

	When("counting and listing accounts empty", func() {
		It("should be countable on api even if empty", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CountAccounts(
				ctx,
				operations.V2CountAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Headers["Count"]).To(Equal([]string{"0"}))
		})
		It("should be listed on api even if empty", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
				ctx,
				operations.V2ListAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2AccountsCursorResponse.Cursor.Data).To(HaveLen(0))
		})
	})

	const (
		pageSize      = int64(10)
		accountCounts = 2 * pageSize
	)
	When("creating accounts", func() {
		var (
			accounts []components.V2Account
		)
		BeforeEach(func(specContext SpecContext) {
			for i := 0; i < int(accountCounts); i++ {
				m := map[string]string{
					"id": fmt.Sprintf("%d", i),
				}

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
					ctx,
					operations.V2AddMetadataToAccountRequest{
						RequestBody: m,
						Address:     fmt.Sprintf("foo:%d", i),
						Ledger:      "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				accounts = append(accounts, components.V2Account{
					Address:  fmt.Sprintf("foo:%d", i),
					Metadata: m,
				})

				sort.Slice(accounts, func(i, j int) bool {
					return accounts[i].Address < accounts[j].Address
				})
			}
		})
		AfterEach(func() {
			accounts = nil
		})
		When(fmt.Sprintf("listing accounts using page size of %d", pageSize), func() {
			var (
				response *operations.V2ListAccountsResponse
				err      error
			)
			BeforeEach(func(specContext SpecContext) {
				response, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
					ctx,
					operations.V2ListAccountsRequest{
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.V2AccountsCursorResponse.Cursor.HasMore).To(BeTrue())
				Expect(response.V2AccountsCursorResponse.Cursor.Previous).To(BeNil())
				Expect(response.V2AccountsCursorResponse.Cursor.Next).NotTo(BeNil())
			})
			It("should return the first page", func() {
				Expect(response.V2AccountsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
				Expect(response.V2AccountsCursorResponse.Cursor.Data).To(Equal(accounts[:pageSize]))
			})
			When("following next cursor", func() {
				BeforeEach(func(specContext SpecContext) {
					response, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
						ctx,
						operations.V2ListAccountsRequest{
							Cursor: response.V2AccountsCursorResponse.Cursor.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(response.V2AccountsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
					Expect(response.V2AccountsCursorResponse.Cursor.Data).To(Equal(accounts[pageSize : 2*pageSize]))
					Expect(response.V2AccountsCursorResponse.Cursor.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					BeforeEach(func(specContext SpecContext) {
						response, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(
							ctx,
							operations.V2ListAccountsRequest{
								Ledger: "default",
								Cursor: response.V2AccountsCursorResponse.Cursor.Previous,
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(response.V2AccountsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
						Expect(response.V2AccountsCursorResponse.Cursor.Data).To(Equal(accounts[:pageSize]))
						Expect(response.V2AccountsCursorResponse.Cursor.Previous).To(BeNil())
					})
				})
			})
		})
	})

	When("Inserting one transaction in past and one in the future", func() {
		now := time.Now()
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{{
						Amount:      big.NewInt(100),
						Asset:       "USD",
						Destination: "foo",
						Source:      "world",
					}},
					Timestamp: pointer.For(now.Add(-12 * time.Hour)),
					Metadata:  map[string]string{},
				},
				Ledger: "default",
			})
			Expect(err).To(Succeed())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{{
						Amount:      big.NewInt(100),
						Asset:       "USD",
						Destination: "foo",
						Source:      "world",
					}},
					Timestamp: pointer.For(now.Add(12 * time.Hour)),
					Metadata:  map[string]string{},
				},
				Ledger: "default",
			})
			Expect(err).To(Succeed())
		})
		When("getting account in the present", func() {
			It("should ignore future transaction on effective volumes", func(specContext SpecContext) {
				accountResponse, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
					Address: "foo",
					Expand:  pointer.For("effectiveVolumes"),
					Ledger:  "default",
					Pit:     pointer.For(time.Now().Add(time.Minute)),
				})
				Expect(err).To(Succeed())
				Expect(accountResponse.V2AccountResponse.Data.EffectiveVolumes["USD"].Balance).To(Equal(big.NewInt(100)))
			})
		})
	})
})

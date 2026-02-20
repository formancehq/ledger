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
	"github.com/formancehq/go-libs/v4/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
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

	When("counting and listing accounts", func() {
		var (
			metadata1 = map[string]any{
				"clientType": "gold",
			}

			metadata2 = map[string]any{
				"clientType": "silver",
			}

			timestamp = time.Now().Round(time.Second).UTC()
			bigInt, _ = big.NewInt(0).SetString("999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)
		)
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.AddMetadataToAccount(
				ctx,
				operations.AddMetadataToAccountRequest{
					RequestBody: metadata1,
					Address:     "foo:foo",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.AddMetadataToAccount(
				ctx,
				operations.AddMetadataToAccountRequest{
					RequestBody: metadata2,
					Address:     "foo:bar",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.CreateTransaction(
				ctx,
				operations.CreateTransactionRequest{
					PostTransaction: components.PostTransaction{
						Metadata: map[string]any{},
						Postings: []components.Posting{{
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
		It("should be countable on api", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.CountAccounts(
				ctx,
				operations.CountAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Headers["Count"]).To(Equal([]string{"3"}))
		})
		It("should be listed on api", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
				ctx,
				operations.ListAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(3))
			Expect(accountsCursorResponse[0]).To(Equal(components.Account{
				Address:  "foo:bar",
				Metadata: metadata2,
			}))
			Expect(accountsCursorResponse[1]).To(Equal(components.Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))
			Expect(accountsCursorResponse[2]).To(Equal(components.Account{
				Address:  "world",
				Metadata: map[string]any{},
			}))
		})
		It("should be listed on api using address filters", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
				ctx,
				operations.ListAccountsRequest{
					Ledger:  "default",
					Address: pointer.For("foo:"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(2))
			Expect(accountsCursorResponse[0]).To(Equal(components.Account{
				Address:  "foo:bar",
				Metadata: metadata2,
			}))
			Expect(accountsCursorResponse[1]).To(Equal(components.Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))

			response, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
				ctx,
				operations.ListAccountsRequest{
					Ledger:  "default",
					Address: pointer.For(":foo"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse = response.AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(1))
			Expect(accountsCursorResponse[0]).To(Equal(components.Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))
		})
		It("should be listed on api using metadata filters", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
				ctx,
				operations.ListAccountsRequest{
					Ledger: "default",
					Metadata: map[string]any{
						"clientType": "gold",
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			accountsCursorResponse := response.AccountsCursorResponse.Cursor.Data
			Expect(accountsCursorResponse).To(HaveLen(1))
			Expect(accountsCursorResponse[0]).To(Equal(components.Account{
				Address:  "foo:foo",
				Metadata: metadata1,
			}))
		})
	})

	When("counting and listing accounts empty", func() {
		It("should be countable on api even if empty", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.CountAccounts(
				ctx,
				operations.CountAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Headers["Count"]).To(Equal([]string{"0"}))
		})
		It("should be listed on api even if empty", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
				ctx,
				operations.ListAccountsRequest{
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.AccountsCursorResponse.Cursor.Data).To(HaveLen(0))
		})
	})

	const (
		pageSize      = int64(10)
		accountCounts = 2 * pageSize
	)
	When("creating accounts", func() {
		var (
			accounts []components.Account
		)
		BeforeEach(func(specContext SpecContext) {
			for i := 0; i < int(accountCounts); i++ {
				m := map[string]any{
					"id": fmt.Sprintf("%d", i),
				}

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V1.AddMetadataToAccount(
					ctx,
					operations.AddMetadataToAccountRequest{
						RequestBody: m,
						Address:     fmt.Sprintf("foo:%d", i),
						Ledger:      "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				accounts = append(accounts, components.Account{
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
				response *operations.ListAccountsResponse
				err      error
			)
			BeforeEach(func(specContext SpecContext) {
				response, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
					ctx,
					operations.ListAccountsRequest{
						Ledger:   "default",
						PageSize: pointer.For(pageSize),
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.AccountsCursorResponse.Cursor.HasMore).To(BeTrue())
				Expect(response.AccountsCursorResponse.Cursor.Previous).To(BeNil())
				Expect(response.AccountsCursorResponse.Cursor.Next).NotTo(BeNil())
			})
			It("should return the first page", func() {
				Expect(response.AccountsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
				Expect(response.AccountsCursorResponse.Cursor.Data).To(Equal(accounts[:pageSize]))
			})
			When("following next cursor", func() {
				BeforeEach(func(specContext SpecContext) {
					response, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
						ctx,
						operations.ListAccountsRequest{
							Cursor: response.AccountsCursorResponse.Cursor.Next,
							Ledger: "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should return next page", func() {
					Expect(response.AccountsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
					Expect(response.AccountsCursorResponse.Cursor.Data).To(Equal(accounts[pageSize : 2*pageSize]))
					Expect(response.AccountsCursorResponse.Cursor.Next).To(BeNil())
				})
				When("following previous cursor", func() {
					BeforeEach(func(specContext SpecContext) {
						response, err = Wait(specContext, DeferClient(testServer)).Ledger.V1.ListAccounts(
							ctx,
							operations.ListAccountsRequest{
								Ledger: "default",
								Cursor: response.AccountsCursorResponse.Cursor.Previous,
							},
						)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should return first page", func() {
						Expect(response.AccountsCursorResponse.Cursor.PageSize).To(Equal(pageSize))
						Expect(response.AccountsCursorResponse.Cursor.Data).To(Equal(accounts[:pageSize]))
						Expect(response.AccountsCursorResponse.Cursor.Previous).To(BeNil())
					})
				})
			})
		})
	})
})

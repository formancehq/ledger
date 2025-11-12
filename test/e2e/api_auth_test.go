//go:build it

package test_suite

import (
	"context"
	"math/big"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/oauth2"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

// clientWithToken creates a client with a bearer token using oauth2.StaticTokenSource
func clientWithToken(srv *deferred.Deferred[*testservice.Service], token string) *deferred.Deferred[*client.Formance] {
	return deferred.Map(srv, func(s *testservice.Service) *client.Formance {
		var httpClient *http.Client
		if token != "" {
			// Use oauth2.StaticTokenSource for static bearer token
			tokenSource := oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: token,
				TokenType:   "Bearer",
			})
			httpClient = oauth2.NewClient(context.Background(), tokenSource)
		} else {
			// No token - use default HTTP client
			httpClient = &http.Client{}
		}
		return client.New(
			client.WithServerURL(testservice.GetServerURL(s).String()),
			client.WithClient(httpClient),
		)
	})
}

var _ = Context("Ledger authentication API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			AuthInstrumentation(GetTestIssuer()), // Use the mock OIDC server URL as issuer
		),
		testservice.WithLogger(GinkgoT()),
	)

	BeforeEach(func(specContext SpecContext) {
		// Create a ledger for testing
		validToken, err := GenerateValidToken()
		Expect(err).To(BeNil())

		// Create client with valid token
		client := clientWithToken(testServer, validToken)

		_, err = Wait(specContext, client).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})

	Context("when listing transactions", func() {
		BeforeEach(func(specContext SpecContext) {
			// Create a transaction first
			validToken, err := GenerateValidToken()
			Expect(err).To(BeNil())

			client := clientWithToken(testServer, validToken)
			_, err = Wait(specContext, client).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{
						{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "world",
							Destination: "alice",
						},
					},
				},
				Ledger: "default",
			})
			Expect(err).To(BeNil())
		})

		When("request without token", func() {
			var (
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				// Create client without token
				client := clientWithToken(testServer, "")
				_, err = Wait(specContext, client).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
			})
			It("should return 401 Unauthorized", func() {
				Expect(err).To(HaveOccurred())

			})
		})

		When("request with invalid token", func() {
			var (
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				invalidToken, tokenErr := GenerateInvalidToken()
				Expect(tokenErr).To(BeNil())

				client := clientWithToken(testServer, invalidToken)
				_, err = Wait(specContext, client).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
			})
			It("should return 401 Unauthorized", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("request with expired token", func() {
			var (
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				expiredToken, tokenErr := GenerateExpiredToken()
				Expect(tokenErr).To(BeNil())

				client := clientWithToken(testServer, expiredToken)
				_, err = Wait(specContext, client).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
			})
			It("should return 401 Unauthorized", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("request with valid token", func() {
			var (
				response *operations.V2ListTransactionsResponse
				err      error
			)
			BeforeEach(func(specContext SpecContext) {
				validToken, tokenErr := GenerateValidToken()
				Expect(tokenErr).To(BeNil())

				client := clientWithToken(testServer, validToken)
				response, err = Wait(specContext, client).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
			})
			It("should return 200 OK with transactions", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(response).ToNot(BeNil())
				Expect(response.V2TransactionsCursorResponse.Cursor.Data).To(HaveLen(1))
			})
		})
	})

	Context("when creating a transaction", func() {
		When("request without token", func() {
			var (
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				// Create client without token
				client := clientWithToken(testServer, "")
				_, err = Wait(specContext, client).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "alice",
							},
						},
					},
					Ledger: "default",
				})
			})
			It("should return 401 Unauthorized", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("request with invalid token", func() {
			var (
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				invalidToken, tokenErr := GenerateInvalidToken()
				Expect(tokenErr).To(BeNil())

				client := clientWithToken(testServer, invalidToken)
				_, err = Wait(specContext, client).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "alice",
							},
						},
					},
					Ledger: "default",
				})
			})
			It("should return 401 Unauthorized", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("request with expired token", func() {
			var (
				err error
			)
			BeforeEach(func(specContext SpecContext) {
				expiredToken, tokenErr := GenerateExpiredToken()
				Expect(tokenErr).To(BeNil())

				client := clientWithToken(testServer, expiredToken)
				_, err = Wait(specContext, client).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "alice",
							},
						},
					},
					Ledger: "default",
				})
			})
			It("should return 401 Unauthorized", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("request with valid token", func() {
			var (
				response *operations.V2CreateTransactionResponse
				err      error
			)
			BeforeEach(func(specContext SpecContext) {
				validToken, tokenErr := GenerateValidToken()
				Expect(tokenErr).To(BeNil())

				client := clientWithToken(testServer, validToken)
				response, err = Wait(specContext, client).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "alice",
							},
						},
					},
					Ledger: "default",
				})
			})
			It("should return 201 Created with transaction", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(response).ToNot(BeNil())
				Expect(response.V2CreateTransactionResponse.Data.ID).To(Equal(big.NewInt(1)))
			})
		})
	})
})

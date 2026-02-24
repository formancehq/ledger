//go:build e2e

package e2e

import (
	"context"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Numscript Library", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("CRUD operations", Ordered, func() {
		const (
			paymentScript = `
send [USD/2 100] (
  source = @world
  destination = @users:alice
)
`
			paymentScriptV2 = `
send [USD/2 200] (
  source = @world
  destination = @users:alice
)
`
			refundScript = `
send [USD/2 50] (
  source = @world
  destination = @users:bob
)
`
		)

		It("Should save a numscript and retrieve it", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptWithVersionAction("payment", paymentScript, "1.0.0")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify log payload
			savedLog := resp.Logs[0].Payload.GetSavedNumscript()
			Expect(savedLog).NotTo(BeNil())
			Expect(savedLog.Info.Name).To(Equal("payment"))
			Expect(savedLog.Info.Content).To(Equal(paymentScript))
			Expect(savedLog.Info.Version).To(Equal("1.0.0"))

			// Get the numscript (version="" means latest)
			info, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name: "payment",
			})
			Expect(err).To(Succeed())
			Expect(info.Name).To(Equal("payment"))
			Expect(info.Content).To(Equal(paymentScript))
			Expect(info.Version).To(Equal("1.0.0"))
		})

		It("Should save to the 'latest' version slot", func() {
			// Save with version="" (defaults to "latest") — creates its own version slot
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptAction("payment", paymentScriptV2)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetSavedNumscript().Info.Version).To(Equal("latest"))

			// version="" returns latest pointer, which is now "latest"
			latest, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name: "payment",
			})
			Expect(err).To(Succeed())
			Expect(latest.Version).To(Equal("latest"))
			Expect(latest.Content).To(Equal(paymentScriptV2))

			// v1.0.0 content is unchanged
			v1, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name:    "payment",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v1.Content).To(Equal(paymentScript))
		})

		It("Should support versioning with semver", func() {
			// Save v2.0.0 of "payment"
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptWithVersionAction("payment", paymentScript, "2.0.0")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetSavedNumscript().Info.Version).To(Equal("2.0.0"))

			// version="" returns latest pointer (2.0.0)
			latest, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name: "payment",
			})
			Expect(err).To(Succeed())
			Expect(latest.Version).To(Equal("2.0.0"))
			Expect(latest.Content).To(Equal(paymentScript))

			// version="1.0.0" returns v1 (content unchanged)
			v1, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name:    "payment",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v1.Version).To(Equal("1.0.0"))
			Expect(v1.Content).To(Equal(paymentScript))

			// version="2.0.0" returns v2
			v2, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name:    "payment",
				Version: "2.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v2.Version).To(Equal("2.0.0"))
			Expect(v2.Content).To(Equal(paymentScript))

			// version="latest" still has the content from earlier save
			latestSlot, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name:    "payment",
				Version: "latest",
			})
			Expect(err).To(Succeed())
			Expect(latestSlot.Version).To(Equal("latest"))
			Expect(latestSlot.Content).To(Equal(paymentScriptV2))
		})

		It("Should list numscripts", func() {
			// Save a second numscript
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptWithVersionAction("refund", refundScript, "1.0.0")},
			})
			Expect(err).To(Succeed())

			scripts, err := listNumscripts(ctx, client)
			Expect(err).To(Succeed())
			Expect(scripts).To(HaveLen(2))

			// Build a map by name for easier assertions
			byName := make(map[string]*commonpb.NumscriptInfo)
			for _, s := range scripts {
				byName[s.Name] = s
			}
			Expect(byName).To(HaveKey("payment"))
			Expect(byName).To(HaveKey("refund"))
			// "payment" should return the latest version (2.0.0)
			Expect(byName["payment"].Version).To(Equal("2.0.0"))
		})

		It("Should delete a numscript", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteNumscriptAction("refund")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Payload.GetDeletedNumscript()).NotTo(BeNil())
			Expect(resp.Logs[0].Payload.GetDeletedNumscript().Name).To(Equal("refund"))

			// List should only have "payment"
			scripts, err := listNumscripts(ctx, client)
			Expect(err).To(Succeed())
			Expect(scripts).To(HaveLen(1))
			Expect(scripts[0].Name).To(Equal("payment"))

			// Get "refund" should fail with NOT_FOUND
			_, err = client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{Name: "refund"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should save v1 then save latest in the same batch", func() {
			const batchScript = `
send [EUR/2 1] (
  source = @world
  destination = @users:charlie
)
`
			const batchScriptLatest = `
send [EUR/2 2] (
  source = @world
  destination = @users:charlie
)
`
			// Single batch: create v1.0.0 then save to "latest" slot
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveNumscriptWithVersionAction("batch-script", batchScript, "1.0.0"),
					saveNumscriptAction("batch-script", batchScriptLatest),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			// First log: created v1.0.0
			saved1 := resp.Logs[0].Payload.GetSavedNumscript()
			Expect(saved1).NotTo(BeNil())
			Expect(saved1.Info.Version).To(Equal("1.0.0"))
			Expect(saved1.Info.Content).To(Equal(batchScript))

			// Second log: created "latest" slot
			saved2 := resp.Logs[1].Payload.GetSavedNumscript()
			Expect(saved2).NotTo(BeNil())
			Expect(saved2.Info.Version).To(Equal("latest"))
			Expect(saved2.Info.Content).To(Equal(batchScriptLatest))

			// Verify final state: default Get returns latest pointer
			info, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name: "batch-script",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("latest"))
			Expect(info.Content).To(Equal(batchScriptLatest))

			// v1.0.0 is untouched
			v1, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name:    "batch-script",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v1.Content).To(Equal(batchScript))
		})
	})

	Context("Script reference transactions", Ordered, func() {
		const ledgerName = "numscript-lib-ledger"

		const sendPaymentScript = `
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = $source
  destination = $destination
)
`
		const sendPaymentScriptV2 = `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`

		BeforeAll(func() {
			// Create ledger and save numscript
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
					saveNumscriptWithVersionAction("send_payment", sendPaymentScript, "1.0.0"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should create a transaction using a script reference", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptRefTransactionAction(ledgerName, "send_payment", "", map[string]string{
						"source":      "world",
						"destination": "users:alice",
						"amount":      "USD/2 1000",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("world"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("users:alice"))
			Expect(createdTx.Transaction.Postings[0].Asset).To(Equal("USD/2"))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("1000"))

			// Verify balance
			Eventually(func(g Gomega) {
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:alice",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.Volumes).To(HaveKey("USD/2"))
				g.Expect(account.Volumes["USD/2"].Balance).To(Equal("1000"))
			}).Within(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("Should use a specific version of the script reference", func() {
			// Save v2 of "send_payment" (uses @world as source, no $source var)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveNumscriptWithVersionAction("send_payment", sendPaymentScriptV2, "2.0.0"),
				},
			})
			Expect(err).To(Succeed())

			// Use version="1.0.0" explicitly (the old script that requires $source)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptRefTransactionAction(ledgerName, "send_payment", "1.0.0", map[string]string{
						"source":      "world",
						"destination": "users:bob",
						"amount":      "EUR/2 500",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Source).To(Equal("world"))
			Expect(createdTx.Transaction.Postings[0].Destination).To(Equal("users:bob"))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("500"))
		})
	})

	Context("Error cases", func() {
		It("Should reject saving a numscript with empty name", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptWithVersionAction("", "send [USD/2 1] (source = @world destination = @x)", "1.0.0")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
		})

		It("Should reject saving a numscript with invalid syntax", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptWithVersionAction("bad-script", "this is not valid numscript", "1.0.0")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptParseError))
		})

		It("Should reject deleting a non-existent numscript", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteNumscriptAction("does-not-exist")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptNotFound))
		})

		It("Should reject getting a non-existent numscript", func() {
			_, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name: "does-not-exist",
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should reject script reference with non-existent name", func() {
			// Need a ledger - reuse one from CRUD or create
			const ledgerName = "numscript-lib-err-ledger"
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createScriptRefTransactionAction(ledgerName, "nonexistent-script", "", map[string]string{
						"amount": "USD/2 100",
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptNotFound))
		})

		It("Should reject specifying both script and scriptReference", func() {
			const ledgerName = "numscript-lib-err-ledger"

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_Apply{
							Apply: &servicepb.LedgerApplyRequest{
								Ledger: ledgerName,
								Data: &servicepb.LedgerApplyRequest_CreateTransaction{
									CreateTransaction: &servicepb.CreateTransactionPayload{
										Script: &commonpb.Script{
											Plain: "send [USD/2 1] (source = @world destination = @x)",
										},
										ScriptReference: &servicepb.ScriptReference{
											Name: "some-script",
										},
									},
								},
							},
						},
					},
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
		})

		It("Should create a numscript with version 'latest' when no version exists", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptAction("new-script", "send [USD/2 1] (source = @world destination = @x)")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			saved := resp.Logs[0].Payload.GetSavedNumscript()
			Expect(saved).NotTo(BeNil())
			Expect(saved.Info.Name).To(Equal("new-script"))
			Expect(saved.Info.Version).To(Equal("latest"))

			// Verify we can retrieve it
			info, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Name: "new-script",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("latest"))
		})

		It("Should reject saving with invalid version format", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveNumscriptWithVersionAction("payment", "send [USD/2 1] (source = @world destination = @x)", "invalid")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptInvalidVersion))
		})
	})
})

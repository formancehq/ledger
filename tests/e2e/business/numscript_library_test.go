//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Numscript Library", Ordered, func() {

	Context("CRUD operations", Ordered, func() {
		const ledgerName = "numscript-crud-ledger"

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

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should save a numscript and retrieve it", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptWithVersionAction(ledgerName, "payment", paymentScript, "1.0.0")},
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
			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger: ledgerName,
				Name:   "payment",
			})
			Expect(err).To(Succeed())
			Expect(info.Name).To(Equal("payment"))
			Expect(info.Content).To(Equal(paymentScript))
			Expect(info.Version).To(Equal("1.0.0"))
		})

		It("Should save to the 'latest' version slot", func() {
			// Save with version="" (defaults to "latest") — creates its own version slot
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptAction(ledgerName, "payment", paymentScriptV2)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetSavedNumscript().Info.Version).To(Equal("latest"))

			// version="" returns latest pointer, which is now "latest"
			latest, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger: ledgerName,
				Name:   "payment",
			})
			Expect(err).To(Succeed())
			Expect(latest.Version).To(Equal("latest"))
			Expect(latest.Content).To(Equal(paymentScriptV2))

			// v1.0.0 content is unchanged
			v1, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "payment",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v1.Content).To(Equal(paymentScript))
		})

		It("Should support versioning with semver", func() {
			// Save v2.0.0 of "payment"
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptWithVersionAction(ledgerName, "payment", paymentScript, "2.0.0")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetSavedNumscript().Info.Version).To(Equal("2.0.0"))

			// version="" returns latest pointer (2.0.0)
			latest, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger: ledgerName,
				Name:   "payment",
			})
			Expect(err).To(Succeed())
			Expect(latest.Version).To(Equal("2.0.0"))
			Expect(latest.Content).To(Equal(paymentScript))

			// version="1.0.0" returns v1 (content unchanged)
			v1, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "payment",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v1.Version).To(Equal("1.0.0"))
			Expect(v1.Content).To(Equal(paymentScript))

			// version="2.0.0" returns v2
			v2, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "payment",
				Version: "2.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v2.Version).To(Equal("2.0.0"))
			Expect(v2.Content).To(Equal(paymentScript))

			// version="latest" still has the content from earlier save
			latestSlot, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "payment",
				Version: "latest",
			})
			Expect(err).To(Succeed())
			Expect(latestSlot.Version).To(Equal("latest"))
			Expect(latestSlot.Content).To(Equal(paymentScriptV2))
		})

		It("Should list numscripts", func() {
			// Save a second numscript
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptWithVersionAction(ledgerName, "refund", refundScript, "1.0.0")},
			})
			Expect(err).To(Succeed())

			scripts, err := testutil.ListNumscripts(sharedCtx, sharedClient, ledgerName)
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
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.DeleteNumscriptAction(ledgerName, "refund")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Payload.GetDeletedNumscript()).NotTo(BeNil())
			Expect(resp.Logs[0].Payload.GetDeletedNumscript().Name).To(Equal("refund"))

			// List should only have "payment"
			scripts, err := testutil.ListNumscripts(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())
			Expect(scripts).To(HaveLen(1))
			Expect(scripts[0].Name).To(Equal("payment"))

			// Get "refund" should fail with NOT_FOUND
			_, err = sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{Ledger: ledgerName, Name: "refund"})
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
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.SaveNumscriptWithVersionAction(ledgerName, "batch-script", batchScript, "1.0.0"),
					testutil.SaveNumscriptAction(ledgerName, "batch-script", batchScriptLatest),
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
			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger: ledgerName,
				Name:   "batch-script",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("latest"))
			Expect(info.Content).To(Equal(batchScriptLatest))

			// v1.0.0 is untouched
			v1, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "batch-script",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(v1.Content).To(Equal(batchScript))
		})
	})

	Context("Partial version resolution", Ordered, func() {
		const ledgerName = "numscript-partial-ledger"
		const simpleScript = `send [USD/2 1] (source = @world destination = @x)`

		BeforeAll(func() {
			// Create ledger and save multiple versions of "partial-test" to test range resolution
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.SaveNumscriptWithVersionAction(ledgerName, "partial-test", simpleScript, "1.0.0"),
					testutil.SaveNumscriptWithVersionAction(ledgerName, "partial-test", simpleScript, "1.0.3"),
					testutil.SaveNumscriptWithVersionAction(ledgerName, "partial-test", simpleScript, "1.2.0"),
					testutil.SaveNumscriptWithVersionAction(ledgerName, "partial-test", simpleScript, "2.0.0"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should resolve '1.0' to highest 1.0.x", func() {
			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "partial-test",
				Version: "1.0",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("1.0.3"))
		})

		It("Should resolve '1' to highest 1.x.y", func() {
			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "partial-test",
				Version: "1",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("1.2.0"))
		})

		It("Should still resolve exact semver", func() {
			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "partial-test",
				Version: "1.0.0",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("1.0.0"))
		})

		It("Should return NOT_FOUND for non-matching partial", func() {
			_, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger:  ledgerName,
				Name:    "partial-test",
				Version: "3",
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should resolve partial version in ScriptReference", func() {
			const txLedgerName = "partial-version-ledger"

			const transferScript = `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`
			// Create ledger and save two versions
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(txLedgerName, nil),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "transfer", transferScript, "1.0.0"),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "transfer", transferScript, "1.0.5"),
				},
			})
			Expect(err).To(Succeed())

			// Use partial version "1.0" in script reference — should resolve to 1.0.5
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateScriptRefTransactionAction(txLedgerName, "transfer", "1.0", map[string]string{
						"destination": "users:charlie",
						"amount":      "USD/2 42",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("42"))
		})

		It("Should resolve major-only partial version in ScriptReference", func() {
			const txLedgerName = "partial-major-ledger"

			const transferScript = `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`
			// Create ledger and save versions across minor ranges
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(txLedgerName, nil),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "major-transfer", transferScript, "1.0.0"),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "major-transfer", transferScript, "1.3.0"),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "major-transfer", transferScript, "2.0.0"),
				},
			})
			Expect(err).To(Succeed())

			// Use version "1" — should resolve to 1.3.0 (highest 1.x.y)
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateScriptRefTransactionAction(txLedgerName, "major-transfer", "1", map[string]string{
						"destination": "users:delta",
						"amount":      "USD/2 77",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("77"))
		})

		It("Should resolve latest version in ScriptReference when version is empty", func() {
			const txLedgerName = "latest-ref-ledger"

			const transferScript = `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`
			// Create ledger and save multiple semver versions
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(txLedgerName, nil),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "latest-transfer", transferScript, "1.0.0"),
					testutil.SaveNumscriptWithVersionAction(txLedgerName, "latest-transfer", transferScript, "2.0.0"),
				},
			})
			Expect(err).To(Succeed())

			// Use version "" — should resolve via latest pointer to 2.0.0
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateScriptRefTransactionAction(txLedgerName, "latest-transfer", "", map[string]string{
						"destination": "users:echo",
						"amount":      "USD/2 99",
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			Expect(createdTx.Transaction.Postings).To(HaveLen(1))
			Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().String()).To(Equal("99"))
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.SaveNumscriptWithVersionAction(ledgerName, "send_payment", sendPaymentScript, "1.0.0"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should create a transaction using a script reference", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateScriptRefTransactionAction(ledgerName, "send_payment", "", map[string]string{
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
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.SaveNumscriptWithVersionAction(ledgerName, "send_payment", sendPaymentScriptV2, "2.0.0"),
				},
			})
			Expect(err).To(Succeed())

			// Use version="1.0.0" explicitly (the old script that requires $source)
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateScriptRefTransactionAction(ledgerName, "send_payment", "1.0.0", map[string]string{
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
		const ledgerName = "numscript-err-ledger"

		It("Should reject saving a numscript with empty name", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptWithVersionAction(ledgerName, "", "send [USD/2 1] (source = @world destination = @x)", "1.0.0")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
		})

		It("Should reject saving a numscript with invalid syntax", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptWithVersionAction(ledgerName, "bad-script", "this is not valid numscript", "1.0.0")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptParseError))
		})

		It("Should reject deleting a non-existent numscript", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.DeleteNumscriptAction(ledgerName, "does-not-exist")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptNotFound))
		})

		It("Should reject getting a non-existent numscript", func() {
			_, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger: ledgerName,
				Name:   "does-not-exist",
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should reject script reference with non-existent name", func() {
			const txLedgerName = "numscript-lib-err-ledger"
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(txLedgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateScriptRefTransactionAction(txLedgerName, "nonexistent-script", "", map[string]string{
						"amount": "USD/2 100",
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptNotFound))
		})

		It("Should reject specifying both script and scriptReference", func() {
			const txLedgerName = "numscript-lib-err-ledger"

			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_Apply{
							Apply: &servicepb.LedgerApplyRequest{
								Ledger: txLedgerName,
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
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptAction(ledgerName, "new-script", "send [USD/2 1] (source = @world destination = @x)")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			saved := resp.Logs[0].Payload.GetSavedNumscript()
			Expect(saved).NotTo(BeNil())
			Expect(saved.Info.Name).To(Equal("new-script"))
			Expect(saved.Info.Version).To(Equal("latest"))

			// Verify we can retrieve it
			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{
				Ledger: ledgerName,
				Name:   "new-script",
			})
			Expect(err).To(Succeed())
			Expect(info.Version).To(Equal("latest"))
		})

		It("Should reject saving with invalid version format", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.SaveNumscriptWithVersionAction(ledgerName, "payment", "send [USD/2 1] (source = @world destination = @x)", "invalid")},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptInvalidVersion))
		})
	})
})

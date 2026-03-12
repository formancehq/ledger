//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"context"
	"crypto/ed25519"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// setAuditConfigAction creates a SetAuditConfig request.
func setAuditConfigAction(enabled bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetAuditConfig{
			SetAuditConfig: &servicepb.SetAuditConfigRequest{
				Enabled: enabled,
			},
		},
	}
}

var _ = Describe("Audit Config (SetAuditConfig RPC)", func() {

	Context("Enable and disable audit logging", Ordered, func() {
		const ledgerName = "audit-config-test"

		It("should start with audit disabled by default (no entries)", func() {
			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			Expect(entries).To(BeEmpty(), "no audit entries should exist before audit is enabled")
		})

		It("should enable audit logging via SetAuditConfig", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setAuditConfigAction(true)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should record audit entries after enabling", func() {
			// Create a ledger to generate an audit entry
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			// At least 2 entries: SetAuditConfig(enable) + CreateLedger
			Expect(len(entries)).To(BeNumerically(">=", 2))
		})

		It("should include SetAuditConfig order in audit entry", func() {
			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())

			// The first entry should be the SetAuditConfig(enabled=true)
			first := entries[0]
			Expect(first.Orders).To(HaveLen(1))
			Expect(first.Orders[0].GetSetAuditConfig()).NotTo(BeNil())
			Expect(first.Orders[0].GetSetAuditConfig().Enabled).To(BeTrue())
		})

		It("should disable audit logging via SetAuditConfig", func() {
			// Snapshot current entries count
			entriesBefore, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			countBefore := len(entriesBefore)

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setAuditConfigAction(false)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// The disable itself should have been recorded (audit was still on when it was processed)
			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			Expect(len(entries)).To(Equal(countBefore + 1))

			last := entries[len(entries)-1]
			Expect(last.Orders[0].GetSetAuditConfig()).NotTo(BeNil())
			Expect(last.Orders[0].GetSetAuditConfig().Enabled).To(BeFalse())
		})

		It("should not record new entries after disabling", func() {
			entriesBefore, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			countBefore := len(entriesBefore)

			// Create a transaction — should NOT generate an audit entry
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			Expect(len(entries)).To(Equal(countBefore), "no new audit entry should be recorded when audit is disabled")
		})

		It("should re-enable audit logging", func() {
			entriesBefore, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			countBefore := len(entriesBefore)

			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setAuditConfigAction(true)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Create a transaction — should generate an audit entry again
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			// re-enable + transaction = 2 new entries
			Expect(len(entries)).To(Equal(countBefore + 2))
		})
	})

	Context("SetAuditConfig with request signing", Ordered, func() {
		var (
			privKey   ed25519.PrivateKey
			sigCtx    context.Context
			sigClient servicepb.BucketServiceClient
		)

		const keyID = "audit-config-sign-key"

		BeforeAll(func() {
			var pubKey ed25519.PublicKey
			pubKey, privKey = testutil.GenerateTestKeypair()

			sigCtx, sigClient, _ = testutil.SetupSingleNode(9311, 8311)

			// Bootstrap signing key
			resp, err := sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.RegisterSigningKeyAction(keyID, pubKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept signed SetAuditConfig request", func() {
			req := setAuditConfigAction(true)
			testutil.SignRequest(req, keyID, privKey)

			resp, err := sigClient.Apply(sigCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the audit entry includes the signing key
			entries, err := collectAuditEntries(sigCtx, sigClient, &servicepb.ListAuditEntriesRequest{})
			Expect(err).To(Succeed())
			Expect(entries).NotTo(BeEmpty())

			last := entries[len(entries)-1]
			Expect(last.Orders).To(HaveLen(1))
			sig := last.Orders[0].GetSignature()
			Expect(sig).NotTo(BeNil())
			Expect(sig.KeyId).To(Equal(keyID))
		})
	})
})

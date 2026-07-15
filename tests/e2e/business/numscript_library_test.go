//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Numscript Library", Ordered, func() {

	const script100 = `send [USD/2 100] (
  source = @world
  destination = @users:alice
)`
	const script200 = `send [USD/2 200] (
  source = @world
  destination = @users:alice
)`

	Context("Immutable append-only library", Ordered, func() {
		const ledgerName = "numscript-immutable-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should save an explicit-semver version and retrieve it", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script100, "1.0.0")))
			Expect(err).To(Succeed())
			saved := resp.Logs[0].Payload.GetSavedNumscript()
			Expect(saved).NotTo(BeNil())
			Expect(saved.Info.Version).To(Equal("1.0.0"))

			info, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{Ledger: ledgerName, Name: "pay"})
			Expect(err).To(Succeed())
			Expect(info.GetVersion()).To(Equal("1.0.0"))
			Expect(info.GetContent()).To(Equal(script100))
		})

		It("Should track the greatest semver as latest, even when saved out of order", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script200, "2.0.0")))
			Expect(err).To(Succeed())
			// Save a lower version after the greater one.
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script100, "1.5.0")))
			Expect(err).To(Succeed())

			latest, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{Ledger: ledgerName, Name: "pay"})
			Expect(err).To(Succeed())
			Expect(latest.GetVersion()).To(Equal("2.0.0"))

			// Older versions remain retrievable and immutable.
			v1, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{Ledger: ledgerName, Name: "pay", Version: "1.0.0"})
			Expect(err).To(Succeed())
			Expect(v1.GetContent()).To(Equal(script100))
		})

		It("Should reject re-saving an existing version (immutable)", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script200, "1.0.0")))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptVersionAlreadyExists))
		})

		It("Should reject saving without an explicit full semver", func() {
			for _, v := range []string{"", "latest", "1", "1.2"} {
				_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script100, v)))
				Expect(err).To(HaveOccurred(), "version %q must be rejected", v)
				st, ok := status.FromError(err)
				Expect(ok).To(BeTrue())
				Expect(st.Code()).To(Equal(codes.InvalidArgument))
			}
		})

		It("Should list numscripts at their greatest version", func() {
			scripts, err := actions.ListNumscripts(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())
			byName := map[string]*commonpb.NumscriptInfo{}
			for _, s := range scripts {
				byName[s.GetName()] = s
			}
			Expect(byName).To(HaveKey("pay"))
			Expect(byName["pay"].GetVersion()).To(Equal("2.0.0"))
		})

		It("Should list the version history with the current latest", func() {
			latest, versions, err := actions.ListNumscriptVersions(sharedCtx, sharedClient, ledgerName, "pay")
			Expect(err).To(Succeed())
			Expect(latest).To(Equal("2.0.0"))
			got := make([]string, len(versions))
			for i, v := range versions {
				got[i] = v.GetVersion()
			}
			Expect(got).To(Equal([]string{"2.0.0", "1.5.0", "1.0.0"}))
		})
	})

	Context("Script references", Ordered, func() {
		const ledgerName = "numscript-ref-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script100, "1.0.0")))
			Expect(err).To(Succeed())
		})

		It("Should run a transaction using a latest reference", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptRefTransactionAction(ledgerName, "pay", "", nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetApply()).NotTo(BeNil())
		})

		It("Should run a transaction using an exact-version reference", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptRefTransactionAction(ledgerName, "pay", "1.0.0", nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetApply()).NotTo(BeNil())
		})

		It("Should reject an executable partial-version reference", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptRefTransactionAction(ledgerName, "pay", "1", nil, nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
		})

		It("Should resolve a same-bulk save for a later latest reference", func() {
			// A save of a new greatest version followed by a latest reference in the
			// same bulk must run the just-saved version (read-your-writes).
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script200, "3.0.0"),
				actions.CreateScriptRefTransactionAction(ledgerName, "pay", "", nil, nil),
			))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
			Expect(resp.Logs[1].Payload.GetApply()).NotTo(BeNil())
		})
	})
})

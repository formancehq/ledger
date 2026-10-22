//go:build e2e

package business

import (
	"strings"

	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
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
			byName := map[string]*ledgerpb.NumscriptInfo{}
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

	Context("Version descriptions", Ordered, func() {
		const ledgerName = "numscript-description-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should store a description with each version and return it on reads", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveDescribedNumscriptAction(ledgerName, "pay", script100, "1.0.0", "Credit Alice 1.00 USD"),
				actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script200, "2.0.0"),
			))
			Expect(err).To(Succeed())

			v1, err := sharedClient.GetNumscript(sharedCtx, &servicepb.GetNumscriptRequest{Ledger: ledgerName, Name: "pay", Version: "1.0.0"})
			Expect(err).To(Succeed())
			Expect(v1.GetDescription()).To(Equal("Credit Alice 1.00 USD"))

			scripts, err := actions.ListNumscripts(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())
			Expect(scripts).To(HaveLen(1))
			Expect(scripts[0].GetVersion()).To(Equal("2.0.0"))
			Expect(scripts[0].GetDescription()).To(BeEmpty())

			_, versions, err := actions.ListNumscriptVersions(sharedCtx, sharedClient, ledgerName, "pay")
			Expect(err).To(Succeed())
			got := map[string]string{}
			for _, v := range versions {
				got[v.GetVersion()] = v.GetDescription()
			}
			Expect(got).To(Equal(map[string]string{"2.0.0": "", "1.0.0": "Credit Alice 1.00 USD"}))
		})

		It("Should reject control characters other than newline and tab", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveDescribedNumscriptAction(ledgerName, "pay", script100, "3.0.0", "\x1b]52;c;cHduZWQ=\x07")))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonValidation))

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveDescribedNumscriptAction(ledgerName, "pay", script100, "3.0.0", "Line one\n\tLine two")))
			Expect(err).To(Succeed())
		})

		It("Should reject a description longer than 1024 bytes", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveDescribedNumscriptAction(ledgerName, "pay", script100, "4.0.0", strings.Repeat("a", domain.MaxNumscriptDescriptionLength+1))))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
			Expect(st.Message()).To(ContainSubstring("description"))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonValidation))
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
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptRefTransactionAction(ledgerName, "pay", "latest", nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetApply()).NotTo(BeNil())
		})

		It("Should run a transaction using an exact-version reference", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptRefTransactionAction(ledgerName, "pay", "1.0.0", nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs[0].Payload.GetApply()).NotTo(BeNil())
		})

		It("Should reject an executable reference without an explicit selector", func() {
			for _, v := range []string{"", "1", "1.2", "bogus"} {
				_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateScriptRefTransactionAction(ledgerName, "pay", v, nil, nil)))
				Expect(err).To(HaveOccurred(), "version %q must be rejected", v)
				st, ok := status.FromError(err)
				Expect(ok).To(BeTrue())
				Expect(st.Code()).To(Equal(codes.InvalidArgument))

				info := actions.ExtractGRPCErrorInfo(err)
				Expect(info).NotTo(BeNil())
				Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptInvalidVersion))
			}
		})

		It("Should resolve a same-bulk save for a later latest reference", func() {
			// A save of a new greatest version followed by a latest reference in the
			// same bulk must run the just-saved version (read-your-writes).
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.SaveNumscriptWithVersionAction(ledgerName, "pay", script200, "3.0.0"),
				actions.CreateScriptRefTransactionAction(ledgerName, "pay", "latest", nil, nil),
			))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
			Expect(resp.Logs[1].Payload.GetApply()).NotTo(BeNil())
		})
	})
})

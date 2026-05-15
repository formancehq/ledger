//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/pkg/actions"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ledger Metadata", Ordered, func() {
	var ledgerName = "ledger-metadata-test"

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
		})
		Expect(err).To(Succeed())
	})

	It("Should set metadata and verify it persists via GetLedger", func() {
		metadata := map[string]string{
			"environment": "production",
			"team":        "payments",
			"region":      "eu-west-1",
		}
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.SaveLedgerMetadataAction(ledgerName, metadata)},
		})
		Expect(err).To(Succeed())

		ledger, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
		Expect(err).To(Succeed())
		Expect(ledger.Metadata).NotTo(BeNil())
		metaMap := commonpb.MetadataToGoMap(ledger.Metadata)
		Expect(metaMap["environment"]).To(Equal("production"))
		Expect(metaMap["team"]).To(Equal("payments"))
		Expect(metaMap["region"]).To(Equal("eu-west-1"))
	})

	It("Should update existing metadata (merge behavior)", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.SaveLedgerMetadataAction(ledgerName, map[string]string{
				"team":    "platform",
				"version": "v3",
			})},
		})
		Expect(err).To(Succeed())

		ledger, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
		Expect(err).To(Succeed())
		metaMap := commonpb.MetadataToGoMap(ledger.Metadata)
		Expect(metaMap["environment"]).To(Equal("production"))
		Expect(metaMap["team"]).To(Equal("platform"))
		Expect(metaMap["version"]).To(Equal("v3"))
		Expect(metaMap["region"]).To(Equal("eu-west-1"))
	})

	It("Should delete metadata and verify removal", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.DeleteLedgerMetadataAction(ledgerName, "region")},
		})
		Expect(err).To(Succeed())

		ledger, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
		Expect(err).To(Succeed())
		metaMap := commonpb.MetadataToGoMap(ledger.Metadata)
		Expect(metaMap["environment"]).To(Equal("production"))
		Expect(metaMap["team"]).To(Equal("platform"))
		_, exists := metaMap["region"]
		Expect(exists).To(BeFalse())
	})

	It("Should return error when deleting non-existent key", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.DeleteLedgerMetadataAction(ledgerName, "does-not-exist")},
		})
		Expect(err).To(HaveOccurred())
	})

	It("Should return error when targeting non-existent ledger", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.SaveLedgerMetadataAction("no-such-ledger", map[string]string{
				"key": "value",
			})},
		})
		Expect(err).To(HaveOccurred())
	})

	Context("On a separate ledger", Ordered, func() {
		var otherLedger = "ledger-metadata-isolation"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(otherLedger, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should not leak metadata between ledgers", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.SaveLedgerMetadataAction(otherLedger, map[string]string{
					"isolated": "true",
				})},
			})
			Expect(err).To(Succeed())

			original, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())
			metaMap := commonpb.MetadataToGoMap(original.Metadata)
			_, exists := metaMap["isolated"]
			Expect(exists).To(BeFalse())

			other, err := actions.GetLedger(sharedCtx, sharedClient, otherLedger)
			Expect(err).To(Succeed())
			otherMeta := commonpb.MetadataToGoMap(other.Metadata)
			Expect(otherMeta["isolated"]).To(Equal("true"))
		})
	})
})

//go:build e2e

package business

import (
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("InspectIndex", Ordered, func() {

	// ========================================================================
	// String metadata: summary, distinct values, facets
	// ========================================================================
	Context("String metadata index inspection", Ordered, func() {
		const ledgerName = "inspect-idx-string"

		BeforeAll(func() {
			// Create ledger with a string metadata field + index.
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "category",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category"),
				},
			})
			Expect(err).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category")).To(Succeed())

			// Create accounts with varied metadata values.
			// 3 premium, 2 basic, 1 enterprise, 1 without metadata.
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user1", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user2", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user3", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user4", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user5", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "user6", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "nocat", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "user1", map[string]string{"category": "premium"}),
					actions.SaveAccountMetadataAction(ledgerName, "user2", map[string]string{"category": "premium"}),
					actions.SaveAccountMetadataAction(ledgerName, "user3", map[string]string{"category": "premium"}),
					actions.SaveAccountMetadataAction(ledgerName, "user4", map[string]string{"category": "basic"}),
					actions.SaveAccountMetadataAction(ledgerName, "user5", map[string]string{"category": "basic"}),
					actions.SaveAccountMetadataAction(ledgerName, "user6", map[string]string{"category": "enterprise"}),
				},
			})
			Expect(err).To(Succeed())

			// Wait for index to catch up.
			Eventually(func(g Gomega) {
				accounts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", actions.StringMetadataFilter("category", "premium"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(3))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return correct summary", func() {
			resp, err := sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      ledgerName,
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "category",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
			})
			Expect(err).To(Succeed())

			summary := resp.GetSummary()
			Expect(summary).NotTo(BeNil())
			Expect(summary.GetCardinality()).To(Equal(uint64(3))) // basic, enterprise, premium
			Expect(summary.GetMin().GetStringValue()).To(Equal("basic"))
			Expect(summary.GetMax().GetStringValue()).To(Equal("premium"))
			Expect(summary.GetEntitiesWithKey()).To(Equal(uint64(6)))
		})

		It("Should return all distinct values in sorted order", func() {
			resp, err := sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      ledgerName,
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "category",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
			})
			Expect(err).To(Succeed())

			dv := resp.GetDistinctValues()
			Expect(dv).NotTo(BeNil())
			Expect(dv.GetValues()).To(HaveLen(3))
			Expect(dv.GetValues()[0].GetStringValue()).To(Equal("basic"))
			Expect(dv.GetValues()[1].GetStringValue()).To(Equal("enterprise"))
			Expect(dv.GetValues()[2].GetStringValue()).To(Equal("premium"))
			Expect(dv.GetHasMore()).To(BeFalse())
		})

		It("Should paginate distinct values", func() {
			// Page 1: 2 values.
			resp, err := sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      ledgerName,
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "category",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
				PageSize:    2,
			})
			Expect(err).To(Succeed())

			dv := resp.GetDistinctValues()
			Expect(dv.GetValues()).To(HaveLen(2))
			Expect(dv.GetValues()[0].GetStringValue()).To(Equal("basic"))
			Expect(dv.GetValues()[1].GetStringValue()).To(Equal("enterprise"))
			Expect(dv.GetHasMore()).To(BeTrue())
			Expect(dv.GetNextCursor()).NotTo(BeEmpty())

			// Page 2: remaining value.
			resp, err = sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      ledgerName,
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "category",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
				PageSize:    2,
				Cursor:      dv.GetNextCursor(),
			})
			Expect(err).To(Succeed())

			dv = resp.GetDistinctValues()
			Expect(dv.GetValues()).To(HaveLen(1))
			Expect(dv.GetValues()[0].GetStringValue()).To(Equal("premium"))
			Expect(dv.GetHasMore()).To(BeFalse())
		})

		It("Should return correct facets", func() {
			resp, err := sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      ledgerName,
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "category",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS,
			})
			Expect(err).To(Succeed())

			facets := resp.GetFacets()
			Expect(facets).NotTo(BeNil())
			Expect(facets.GetFacets()).To(HaveLen(3))

			// Facets are in index order (sorted by value).
			Expect(facets.GetFacets()[0].GetValue().GetStringValue()).To(Equal("basic"))
			Expect(facets.GetFacets()[0].GetCount()).To(Equal(uint64(2)))
			Expect(facets.GetFacets()[1].GetValue().GetStringValue()).To(Equal("enterprise"))
			Expect(facets.GetFacets()[1].GetCount()).To(Equal(uint64(1)))
			Expect(facets.GetFacets()[2].GetValue().GetStringValue()).To(Equal("premium"))
			Expect(facets.GetFacets()[2].GetCount()).To(Equal(uint64(3)))
		})
	})

	// ========================================================================
	// Error cases
	// ========================================================================
	Context("Error cases", Ordered, func() {
		const ledgerName = "inspect-idx-errors"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "indexed",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should fail for non-indexed metadata key", func() {
			_, err := sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      ledgerName,
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "indexed",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should fail for non-existent ledger", func() {
			_, err := sharedClient.InspectIndex(sharedCtx, &servicepb.InspectIndexRequest{
				Ledger:      "nonexistent-ledger",
				TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				MetadataKey: "foo",
				Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
			})
			Expect(err).To(HaveOccurred())
		})
	})
})

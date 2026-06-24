//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("AggregateVolumes", Ordered, func() {

	Context("When aggregating volumes on an empty ledger", Ordered, func() {
		const ledgerName = "agg-vol-empty"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should return an empty result", func() {
			result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(result.Volumes).To(BeEmpty())
		})
	})

	Context("When aggregating volumes with no filter", Ordered, func() {
		const ledgerName = "agg-vol-nofilter"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// world→alice 100 USD, world→alice 50 EUR, world→bob 200 USD
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "alice", big.NewInt(50), "EUR"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
				}, nil)))
			Expect(err).To(Succeed())
		})

		It("Should return aggregated volumes across all accounts", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.Volumes).To(HaveLen(2))

				volumesByAsset := make(map[string]*commonpb.AggregatedVolume)
				for _, v := range result.Volumes {
					volumesByAsset[v.Asset] = v
				}

				// All accounts: world(out=300 USD, out=50 EUR), alice(in=100 USD, in=50 EUR), bob(in=200 USD)
				// Totals: USD input=300, output=300; EUR input=50, output=50
				usdVol, ok := volumesByAsset["USD"]
				g.Expect(ok).To(BeTrue(), "expected USD volumes")
				g.Expect(usdVol.Input.ToBigInt().Int64()).To(Equal(int64(300)))
				g.Expect(usdVol.Output.ToBigInt().Int64()).To(Equal(int64(300)))

				eurVol, ok := volumesByAsset["EUR"]
				g.Expect(ok).To(BeTrue(), "expected EUR volumes")
				g.Expect(eurVol.Input.ToBigInt().Int64()).To(Equal(int64(50)))
				g.Expect(eurVol.Output.ToBigInt().Int64()).To(Equal(int64(50)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes with a prefix filter", Ordered, func() {
		const ledgerName = "agg-vol-prefix"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// world→users:alice 100 USD, world→users:bob 200 USD, world→bank:main 500 USD
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "users:bob", big.NewInt(200), "USD"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank:main", big.NewInt(500), "USD"),
				}, nil)))
			Expect(err).To(Succeed())
		})

		It("Should return aggregated volumes only for accounts matching the prefix", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger: ledgerName,
					Filter: actions.AddressPrefixFilter("users:"),
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.Volumes).To(HaveLen(1))

				// users:alice(in=100) + users:bob(in=200) = input=300, output=0
				usdVol := result.Volumes[0]
				g.Expect(usdVol.Asset).To(Equal("USD"))
				g.Expect(usdVol.Input.ToBigInt().Int64()).To(Equal(int64(300)))
				g.Expect(usdVol.Output.ToBigInt().Int64()).To(Equal(int64(0)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes with a metadata filter", Ordered, func() {
		const ledgerName = "agg-vol-meta"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "role",
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
			}),
				actions.CreateAccountMetadataIndexAction(ledgerName, "role")))
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())

			// world→alice 100 USD, world→alice 50 EUR, world→bob 200 USD
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "alice", big.NewInt(50), "EUR"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
				}, nil)))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
				actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user"})))
			Expect(err).To(Succeed())
		})

		It("Should return aggregated volumes only for admin accounts", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger: ledgerName,
					Filter: actions.StringMetadataFilter("role", "admin"),
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.Volumes).To(HaveLen(2))

				volumesByAsset := make(map[string]*commonpb.AggregatedVolume)
				for _, v := range result.Volumes {
					volumesByAsset[v.Asset] = v
				}

				// alice received 100 USD (input=100, output=0)
				usdVol, ok := volumesByAsset["USD"]
				g.Expect(ok).To(BeTrue(), "expected USD volumes")
				g.Expect(usdVol.Input.ToBigInt().Int64()).To(Equal(int64(100)))
				g.Expect(usdVol.Output.ToBigInt().Int64()).To(Equal(int64(0)))

				// alice received 50 EUR (input=50, output=0)
				eurVol, ok := volumesByAsset["EUR"]
				g.Expect(ok).To(BeTrue(), "expected EUR volumes")
				g.Expect(eurVol.Input.ToBigInt().Int64()).To(Equal(int64(50)))
				g.Expect(eurVol.Output.ToBigInt().Int64()).To(Equal(int64(0)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes with use_max_precision across different precisions", Ordered, func() {
		const ledgerName = "agg-vol-max-prec"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// Create postings with different precisions for the same asset base:
			// world→alice: 100 USD/2 (= $1.00)
			// world→bob:   10000 USD/4 (= $1.0000)
			// world→carol: 1000 USD/3 (= $1.000)
			// world→alice: 50 EUR/2
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD/2"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bob", big.NewInt(10000), "USD/4"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "carol", big.NewInt(1000), "USD/3"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "alice", big.NewInt(50), "EUR/2"),
				}, nil)))
			Expect(err).To(Succeed())
		})

		It("Without use_max_precision, should return separate entries per precision", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// USD/2, USD/3, USD/4, EUR/2 = 4 entries
				g.Expect(result.Volumes).To(HaveLen(4))

				volumesByAsset := make(map[string]*commonpb.AggregatedVolume)
				for _, v := range result.Volumes {
					volumesByAsset[v.Asset] = v
				}
				g.Expect(volumesByAsset).To(HaveKey("USD/2"))
				g.Expect(volumesByAsset).To(HaveKey("USD/3"))
				g.Expect(volumesByAsset).To(HaveKey("USD/4"))
				g.Expect(volumesByAsset).To(HaveKey("EUR/2"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("With use_max_precision, should merge USD precisions under USD/4", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger:          ledgerName,
					UseMaxPrecision: true,
				})
				g.Expect(err).To(Succeed())
				// USD/2 + USD/3 + USD/4 → USD/4, EUR/2 stays = 2 entries
				g.Expect(result.Volumes).To(HaveLen(2))

				volumesByAsset := make(map[string]*commonpb.AggregatedVolume)
				for _, v := range result.Volumes {
					volumesByAsset[v.Asset] = v
				}

				// USD/4 merged:
				// USD/2: input=100, rescaled to /4: 100 * 10^(4-2) = 10000
				// USD/3: input=1000, rescaled to /4: 1000 * 10^(4-3) = 10000
				// USD/4: input=10000 (no rescaling)
				// Total input = 30000, same for output (double-entry, world has the output)
				usdVol, ok := volumesByAsset["USD/4"]
				g.Expect(ok).To(BeTrue(), "expected merged USD/4 volumes")
				g.Expect(usdVol.Input.ToBigInt().Int64()).To(Equal(int64(30000)))
				g.Expect(usdVol.Output.ToBigInt().Int64()).To(Equal(int64(30000)))

				// EUR/2 unchanged
				eurVol, ok := volumesByAsset["EUR/2"]
				g.Expect(ok).To(BeTrue(), "expected EUR/2 volumes")
				g.Expect(eurVol.Input.ToBigInt().Int64()).To(Equal(int64(50)))
				g.Expect(eurVol.Output.ToBigInt().Int64()).To(Equal(int64(50)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes with use_max_precision and a prefix filter", Ordered, func() {
		const ledgerName = "agg-vol-max-prec-filter"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// world→users:alice 100 USD/2, world→users:bob 10000 USD/4, world→bank:main 500 USD/2
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "users:alice", big.NewInt(100), "USD/2"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "users:bob", big.NewInt(10000), "USD/4"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank:main", big.NewInt(500), "USD/2"),
				}, nil)))
			Expect(err).To(Succeed())
		})

		It("Should merge precisions only for filtered accounts", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger:          ledgerName,
					Filter:          actions.AddressPrefixFilter("users:"),
					UseMaxPrecision: true,
				})
				g.Expect(err).To(Succeed())
				// users:alice(USD/2) + users:bob(USD/4) merged → USD/4
				g.Expect(result.Volumes).To(HaveLen(1))

				usdVol := result.Volumes[0]
				g.Expect(usdVol.Asset).To(Equal("USD/4"))
				// alice: 100 * 10^2 = 10000, bob: 10000 → total = 20000
				g.Expect(usdVol.Input.ToBigInt().Int64()).To(Equal(int64(20000)))
				g.Expect(usdVol.Output.ToBigInt().Int64()).To(Equal(int64(0)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes with group_by_prefixes", Ordered, func() {
		const ledgerName = "agg-vol-group-prefix"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// world→users:alice 100 USD, world→users:bob 200 USD
			// world→merchants:shop1 500 USD, world→merchants:shop2 300 EUR
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "users:bob", big.NewInt(200), "USD"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "merchants:shop1", big.NewInt(500), "USD"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "merchants:shop2", big.NewInt(300), "EUR"),
				}, nil)))
			Expect(err).To(Succeed())
		})

		It("Should return grouped results by prefix", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger:          ledgerName,
					GroupByPrefixes: []string{"users:", "merchants:"},
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.Volumes).To(BeEmpty(), "flat volumes should be empty for grouped result")
				g.Expect(result.Groups).To(HaveLen(2))

				// Groups ordered by declaration: users: first, merchants: second
				usersGroup := result.Groups[0]
				g.Expect(usersGroup.Prefix).To(Equal("users:"))
				g.Expect(usersGroup.Volumes).To(HaveLen(1))
				g.Expect(usersGroup.Volumes[0].Asset).To(Equal("USD"))
				g.Expect(usersGroup.Volumes[0].Input.ToBigInt().Int64()).To(Equal(int64(300))) // 100+200

				merchantsGroup := result.Groups[1]
				g.Expect(merchantsGroup.Prefix).To(Equal("merchants:"))
				g.Expect(merchantsGroup.Volumes).To(HaveLen(2))

				merchByAsset := make(map[string]*commonpb.AggregatedVolume)
				for _, v := range merchantsGroup.Volumes {
					merchByAsset[v.Asset] = v
				}
				g.Expect(merchByAsset["USD"].Input.ToBigInt().Int64()).To(Equal(int64(500)))
				g.Expect(merchByAsset["EUR"].Input.ToBigInt().Int64()).To(Equal(int64(300)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes with group_by_prefixes and use_max_precision", Ordered, func() {
		const ledgerName = "agg-vol-group-maxprec"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// world→users:alice 100 USD/2, world→users:bob 10000 USD/4
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "users:alice", big.NewInt(100), "USD/2"),
			}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "users:bob", big.NewInt(10000), "USD/4"),
				}, nil)))
			Expect(err).To(Succeed())
		})

		It("Should merge precisions within each group", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger:          ledgerName,
					UseMaxPrecision: true,
					GroupByPrefixes: []string{"users:"},
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.Groups).To(HaveLen(1))

				usersGroup := result.Groups[0]
				g.Expect(usersGroup.Prefix).To(Equal("users:"))
				g.Expect(usersGroup.Volumes).To(HaveLen(1))
				g.Expect(usersGroup.Volumes[0].Asset).To(Equal("USD/4"))
				// alice: 100 * 10^2 = 10000, bob: 10000 → total = 20000
				g.Expect(usersGroup.Volumes[0].Input.ToBigInt().Int64()).To(Equal(int64(20000)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When aggregating volumes for a non-existent ledger", func() {
		It("Should return a NotFound error", func() {
			_, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
				Ledger: "non-existent-ledger",
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})
})

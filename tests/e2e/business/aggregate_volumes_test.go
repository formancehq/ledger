//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("AggregateVolumes", Ordered, func() {

	Context("When aggregating volumes on an empty ledger", Ordered, func() {
		const ledgerName = "agg-vol-empty"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// world→alice 100 USD, world→alice 50 EUR, world→bob 200 USD
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(50), "EUR"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// world→users:alice 100 USD, world→users:bob 200 USD, world→bank:main 500 USD
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "users:bob", big.NewInt(200), "USD"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "bank:main", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return aggregated volumes only for accounts matching the prefix", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger: ledgerName,
					Filter: prefixFilter("users:"),
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
				},
			})
			Expect(err).To(Succeed())

			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")

			// world→alice 100 USD, world→alice 50 EUR, world→bob 200 USD
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(50), "EUR"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
					testutil.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return aggregated volumes only for admin accounts", func() {
			Eventually(func(g Gomega) {
				result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
					Ledger: ledgerName,
					Filter: stringFilter("role", "admin"),
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

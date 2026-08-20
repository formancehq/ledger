//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const forwardedHistoricalBalanceViewTrailerKey = "x-historical-balance-view-bin"

var _ = Describe("Point-in-time balances forwarding", Ordered, Serial, func() {
	const (
		countInstances = 3
		ledgerName     = "pit-forwarded-read"
	)

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
	)

	BeforeAll(func() {
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(countInstances)
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("relays the leader PIT result and immutable view trailer through a follower", func() {
		lid := *leaderID
		followerID := (lid % countInstances) + 1
		Expect(followerID).NotTo(Equal(lid))

		leaderClient := servers[lid-1].Client
		followerClient := servers[followerID-1].Client

		// Exercise proposal forwarding as well: both the ledger and transaction
		// are submitted to a follower, while the PIT read below is explicitly
		// routed back to the leader.
		_, err := followerClient.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
		Expect(testutil.ConfigureHistoricalBalances(ctx, followerClient, ledgerName, true)).To(Succeed())
		for _, server := range servers {
			testutil.WaitForHistoricalBalancesReady(ctx, server.Client, ledgerName)
		}

		effectiveAt := time.Date(2020, time.June, 15, 12, 0, 0, 0, time.UTC)
		applyResponse, err := followerClient.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.WithTimestamp(
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:forwarded", big.NewInt(321), "USD"),
			}, nil),
			effectiveAt,
		)))
		Expect(err).To(Succeed())
		Expect(applyResponse.GetLogs()).To(HaveLen(1))
		minimumLogSequence := applyResponse.GetLogs()[0].GetSequence()

		request := &servicepb.AggregateVolumesRequest{
			Ledger:         ledgerName,
			Filter:         actions.AddressExactFilter("accounts:forwarded"),
			MinLogSequence: minimumLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          &commonpb.Timestamp{Data: uint64(effectiveAt.UnixMicro())},
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			},
		}

		// Establish the direct leader response first. The min-log gate waits for
		// the leader's asynchronous balance-history builder.
		directResult, directView := forwardedPointInTimeAggregateEventually(ctx, leaderClient, request)
		expectForwardedPointInTimeVolume(directResult, 321, 0)

		leaderCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "leader")
		forwardedResult, forwardedView := forwardedPointInTimeAggregateEventually(leaderCtx, followerClient, request)
		expectForwardedPointInTimeVolume(forwardedResult, 321, 0)

		Expect(forwardedView.GetRequestedAt().GetData()).To(Equal(uint64(effectiveAt.UnixMicro())))
		Expect(forwardedView.GetTemporality()).To(Equal(servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE))
		Expect(forwardedView.GetLogWatermark()).To(BeNumerically(">=", minimumLogSequence))
		Expect(forwardedView.GetViewToken()).NotTo(BeEmpty())
		Expect(forwardedView.GetViewToken()).To(Equal(directView.GetViewToken()),
			"the follower must relay the leader's immutable PIT provenance")
		Expect(forwardedView.GetManifestVersion()).To(Equal(directView.GetManifestVersion()))

		// A local follower read uses its own peer store and may temporarily lag,
		// but must eventually serve the same monetary result without forwarding.
		localResult, localView := forwardedPointInTimeAggregateEventually(ctx, followerClient, request)
		expectForwardedPointInTimeVolume(localResult, 321, 0)
		Expect(localView.GetLogWatermark()).To(BeNumerically(">=", minimumLogSequence))
		Expect(localView.GetViewToken()).NotTo(BeEmpty())
	})
})

func forwardedPointInTimeAggregateEventually(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	request *servicepb.AggregateVolumesRequest,
) (*commonpb.AggregateResult, *servicepb.HistoricalBalanceView) {
	var (
		result *commonpb.AggregateResult
		view   *servicepb.HistoricalBalanceView
	)
	Eventually(func(g Gomega) {
		var trailer metadata.MD
		var err error
		result, err = client.AggregateVolumes(ctx, request, grpc.Trailer(&trailer))
		g.Expect(err).To(Succeed())
		g.Expect(result).NotTo(BeNil())

		values := trailer.Get(forwardedHistoricalBalanceViewTrailerKey)
		g.Expect(values).To(HaveLen(1))
		view = &servicepb.HistoricalBalanceView{}
		g.Expect(view.UnmarshalVT([]byte(values[0]))).To(Succeed())
		g.Expect(view.GetViewToken()).NotTo(BeEmpty())
	}).Within(30*time.Second).ProbeEvery(100*time.Millisecond).Should(Succeed(),
		fmt.Sprintf("node must serve PIT through log sequence %d", request.GetMinLogSequence()))

	return result, view
}

func expectForwardedPointInTimeVolume(result *commonpb.AggregateResult, input, output int64) {
	Expect(result).NotTo(BeNil())
	Expect(result.GetVolumes()).To(HaveLen(1))
	volume := result.GetVolumes()[0]
	Expect(volume.GetAsset()).To(Equal("USD"))
	Expect(volume.GetColor()).To(BeEmpty())
	Expect(volume.GetInput().ToBigInt().Int64()).To(Equal(input))
	Expect(volume.GetOutput().ToBigInt().Int64()).To(Equal(output))
}

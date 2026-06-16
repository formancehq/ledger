//go:build e2e

package cluster

import (
	"context"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// setQueryCheckpointScheduleAction creates a SetQueryCheckpointSchedule request.
func setQueryCheckpointScheduleAction(cron string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetQueryCheckpointSchedule{
			SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{
				Cron: cron,
			},
		},
	}
}

// deleteQueryCheckpointScheduleAction creates a DeleteQueryCheckpointSchedule request.
func deleteQueryCheckpointScheduleAction() *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteQueryCheckpointSchedule{
			DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{},
		},
	}
}

var _ = Describe("Query Checkpoint Schedule", func() {

	Context("CRUD operations", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9240
			grpcPort   = 8240
			ledgerName = "qcp-schedule-test"
		)

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			// Create a ledger so the cluster is fully bootstrapped.
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateLedgerAction(ledgerName, nil),
				),
			})
			Expect(err).To(Succeed())
		})

		It("should return an empty cron when no schedule is set", func() {
			resp, err := clusterClient.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCron()).To(BeEmpty())
		})

		It("should accept a valid cron expression", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("0 0 1 * *")),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetLogs()).To(HaveLen(1))
		})

		It("should return the configured cron expression", func() {
			resp, err := clusterClient.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCron()).To(Equal("0 0 1 * *"))
		})

		It("should reject an invalid cron expression", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("not-a-cron")),
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInvalidCronExpression))
		})

		It("should reject an empty cron expression", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("")),
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
		})

		It("should delete the schedule", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(deleteQueryCheckpointScheduleAction()),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetLogs()).To(HaveLen(1))
		})

		It("should return an empty cron after deleting", func() {
			resp, err := clusterClient.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCron()).To(BeEmpty())
		})
	})

	Context("Automatic checkpoint creation", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9241
			grpcPort   = 8241
			ledgerName = "qcp-schedule-auto"
		)

		var initialCheckpointCount int

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			// Create a ledger so there is data to checkpoint.
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateLedgerAction(ledgerName, nil),
				),
			})
			Expect(err).To(Succeed())
		})

		It("should record initial checkpoint count", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			initialCheckpointCount = len(resp.GetCheckpoints())
		})

		It("should set a fast cron schedule", func() {
			// Every 5 seconds (6-field format with leading seconds)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("*/5 * * * * *")),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetLogs()).To(HaveLen(1))
		})

		It("should automatically create a new checkpoint within ~10 seconds", func() {
			Eventually(func(g Gomega) {
				resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
				g.Expect(err).To(Succeed())
				g.Expect(len(resp.GetCheckpoints())).To(BeNumerically(">", initialCheckpointCount))
			}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("should be queryable from the auto-created checkpoint", func() {
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCheckpoints()).NotTo(BeEmpty())

			cpID := resp.GetCheckpoints()[0].GetCheckpointId()

			// The checkpoint should contain the ledger we created.
			stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{
				Options: &commonpb.ListOptions{
					Read: &commonpb.ReadOptions{CheckpointId: cpID},
				},
			})
			Expect(err).To(Succeed())

			ledger, err := stream.Recv()
			Expect(err).To(Succeed())
			Expect(ledger.GetName()).To(Equal(ledgerName))
		})

		It("should disable the schedule after the test", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(deleteQueryCheckpointScheduleAction()),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetLogs()).To(HaveLen(1))
		})
	})

	Context("Schedule update takes effect", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9242
			grpcPort   = 8242
			ledgerName = "qcp-schedule-update"
		)

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateLedgerAction(ledgerName, nil),
				),
			})
			Expect(err).To(Succeed())
		})

		It("should update the schedule from monthly to per-second", func() {
			// Set a monthly schedule (won't fire during test)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("0 0 1 * *")),
			})
			Expect(err).To(Succeed())

			resp, err := clusterClient.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCron()).To(Equal("0 0 1 * *"))

			// Update to a fast schedule
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("*/5 * * * * *")),
			})
			Expect(err).To(Succeed())

			resp, err = clusterClient.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetCron()).To(Equal("*/5 * * * * *"))
		})

		It("should fire under the updated schedule", func() {
			Eventually(func(g Gomega) {
				resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
				g.Expect(err).To(Succeed())
				g.Expect(len(resp.GetCheckpoints())).To(BeNumerically(">=", 1))
			}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("should stop creating checkpoints after deleting the schedule", func() {
			// Delete the schedule
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(deleteQueryCheckpointScheduleAction()),
			})
			Expect(err).To(Succeed())

			// Record the current count
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			countAfterDelete := len(resp.GetCheckpoints())

			// Wait and confirm no more are created
			Consistently(func(g Gomega) {
				resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
				g.Expect(err).To(Succeed())
				g.Expect(len(resp.GetCheckpoints())).To(Equal(countAfterDelete))
			}).Within(3 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Checkpoints created by schedule capture progressive state", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9243
			grpcPort   = 8243
			ledgerName = "qcp-schedule-state"
		)

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateLedgerAction(ledgerName, nil),
				),
			})
			Expect(err).To(Succeed())
		})

		var firstCheckpointMaxSeq uint64

		It("should create a checkpoint, add a transaction, then create another checkpoint", func() {
			// Create first checkpoint via schedule
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(setQueryCheckpointScheduleAction("*/3 * * * * *")),
			})
			Expect(err).To(Succeed())

			// Wait for first checkpoint
			Eventually(func(g Gomega) {
				resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
				g.Expect(err).To(Succeed())
				g.Expect(len(resp.GetCheckpoints())).To(BeNumerically(">=", 1))
			}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

			// Record the first checkpoint's max sequence
			resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			Expect(err).To(Succeed())
			firstCheckpointMaxSeq = resp.GetCheckpoints()[0].GetMaxSequence()

			// Create a transaction (this happens after the first checkpoint)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(1000), "USD"),
					}, nil),
				),
			})
			Expect(err).To(Succeed())

			// Wait for a second checkpoint (has higher max_sequence)
			Eventually(func(g Gomega) {
				resp, err := clusterClient.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
				g.Expect(err).To(Succeed())
				cps := resp.GetCheckpoints()
				g.Expect(len(cps)).To(BeNumerically(">=", 2))
				// Latest checkpoint should have higher sequence than the first
				g.Expect(cps[len(cps)-1].GetMaxSequence()).To(BeNumerically(">", firstCheckpointMaxSeq))
			}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("should disable schedule", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(deleteQueryCheckpointScheduleAction()),
			})
			Expect(err).To(Succeed())
		})
	})
})

//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setPeriodScheduleAction creates a SetPeriodSchedule request.
func setPeriodScheduleAction(cron string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetPeriodSchedule{
			SetPeriodSchedule: &servicepb.SetPeriodScheduleRequest{
				Cron: cron,
			},
		},
	}
}

// deletePeriodScheduleAction creates a DeletePeriodSchedule request.
func deletePeriodScheduleAction() *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeletePeriodSchedule{
			DeletePeriodSchedule: &servicepb.DeletePeriodScheduleRequest{},
		},
	}
}

var _ = Describe("Period Schedule", Ordered, func() {

	BeforeAll(func() {

		// Create a ledger so period auto-bootstrap happens
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction("schedule-test", nil)},
		})
		Expect(err).To(Succeed())
	})

	Context("Get schedule when none is set", func() {
		It("should return an empty cron expression", func() {
			resp, err := sharedClient.GetPeriodSchedule(sharedCtx, &servicepb.GetPeriodScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Cron).To(BeEmpty())
		})
	})

	Context("Set and get schedule", Ordered, func() {
		const cronExpr = "0 0 1 * *" // first day of every month

		It("should accept a valid cron expression", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setPeriodScheduleAction(cronExpr)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should return the configured cron expression", func() {
			resp, err := sharedClient.GetPeriodSchedule(sharedCtx, &servicepb.GetPeriodScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Cron).To(Equal(cronExpr))
		})
	})

	Context("Reject invalid cron expression", func() {
		It("should return InvalidArgument for a bad cron expression", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setPeriodScheduleAction("not-a-cron")},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInvalidCronExpression))
		})
	})

	Context("Reject empty cron in SetPeriodSchedule", func() {
		It("should return InvalidArgument for an empty cron expression", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setPeriodScheduleAction("")},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInvalidCronExpression))
		})
	})

	Context("Delete schedule", Ordered, func() {
		It("should set a schedule first", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setPeriodScheduleAction("0 0 1 * *")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept a delete-schedule request", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deletePeriodScheduleAction()},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should return an empty cron after deleting", func() {
			resp, err := sharedClient.GetPeriodSchedule(sharedCtx, &servicepb.GetPeriodScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Cron).To(BeEmpty())
		})
	})

	Context("Automatic period rotation", Ordered, func() {
		var initialPeriodCount int

		It("should record initial period count", func() {
			periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			initialPeriodCount = len(periods)
			Expect(initialPeriodCount).To(BeNumerically(">=", 1))
		})

		It("should set a per-second cron schedule", func() {
			// Every 5 seconds (6-field format with leading seconds)
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{setPeriodScheduleAction("*/5 * * * * *")},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should automatically create a new period within ~10 seconds", func() {
			// The cron fires every 5 seconds, so within ~10s we should see a new period
			Eventually(func(g Gomega) {
				periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				g.Expect(len(periods)).To(BeNumerically(">", initialPeriodCount))
				// The latest period should be OPEN
				g.Expect(periods[len(periods)-1].Status).To(Equal(commonpb.PeriodStatus_PERIOD_OPEN))
			}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("should disable the schedule after the test", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deletePeriodScheduleAction()},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})
})

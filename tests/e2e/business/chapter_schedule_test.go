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

// setChapterScheduleAction creates a SetChapterSchedule request.
func setChapterScheduleAction(cron string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetChapterSchedule{
			SetChapterSchedule: &servicepb.SetChapterScheduleRequest{
				Cron: cron,
			},
		},
	}
}

// deleteChapterScheduleAction creates a DeleteChapterSchedule request.
func deleteChapterScheduleAction() *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteChapterSchedule{
			DeleteChapterSchedule: &servicepb.DeleteChapterScheduleRequest{},
		},
	}
}

var _ = Describe("Chapter Schedule", Ordered, func() {

	BeforeAll(func() {

		// Create a ledger so chapter auto-bootstrap happens
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("schedule-test", nil)))
		Expect(err).To(Succeed())
	})

	Context("Get schedule when none is set", func() {
		It("should return an empty cron expression", func() {
			resp, err := sharedClient.GetChapterSchedule(sharedCtx, &servicepb.GetChapterScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Cron).To(BeEmpty())
		})
	})

	Context("Set and get schedule", Ordered, func() {
		const cronExpr = "0 0 1 * *" // first day of every month

		It("should accept a valid cron expression", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", setChapterScheduleAction(cronExpr)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should return the configured cron expression", func() {
			resp, err := sharedClient.GetChapterSchedule(sharedCtx, &servicepb.GetChapterScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Cron).To(Equal(cronExpr))
		})
	})

	Context("Reject invalid cron expression", func() {
		It("should return InvalidArgument for a bad cron expression", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", setChapterScheduleAction("not-a-cron")))
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInvalidCronExpression))
		})
	})

	Context("Reject empty cron in SetChapterSchedule", func() {
		It("should return InvalidArgument for an empty cron expression", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", setChapterScheduleAction("")))
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
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", setChapterScheduleAction("0 0 1 * *")))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept a delete-schedule request", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", deleteChapterScheduleAction()))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should return an empty cron after deleting", func() {
			resp, err := sharedClient.GetChapterSchedule(sharedCtx, &servicepb.GetChapterScheduleRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Cron).To(BeEmpty())
		})
	})

	Context("Automatic chapter rotation", Ordered, func() {
		var initialChapterCount int

		It("should record initial chapter count", func() {
			chapters, err := actions.ListAllChapters(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			initialChapterCount = len(chapters)
			Expect(initialChapterCount).To(BeNumerically(">=", 1))
		})

		It("should set a per-second cron schedule", func() {
			// Every 5 seconds (6-field format with leading seconds)
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", setChapterScheduleAction("*/5 * * * * *")))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should automatically create a new chapter within ~10 seconds", func() {
			// The cron fires every 5 seconds, so within ~10s we should see a new chapter
			Eventually(func(g Gomega) {
				chapters, err := actions.ListAllChapters(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				g.Expect(len(chapters)).To(BeNumerically(">", initialChapterCount))
				// The latest chapter should be OPEN
				g.Expect(chapters[len(chapters)-1].Status).To(Equal(commonpb.ChapterStatus_CHAPTER_OPEN))
			}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("should disable the schedule after the test", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", deleteChapterScheduleAction()))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})
})

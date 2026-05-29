//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Transaction Reference Uniqueness", Ordered, func() {

	Context("Within a single ledger", Ordered, func() {
		var ledgerName = "ref-test-ledger"

		BeforeAll(func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should create a transaction with a reference", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"ref-001",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should fail when creating a transaction with a duplicate reference", func() {
			// First transaction with reference succeeds
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"dup-ref",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Second transaction with the same reference fails
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-2", big.NewInt(200), "USD"),
						}, nil, nil),
						"dup-ref",
					),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionReferenceConflict))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["reference"]).To(Equal("dup-ref"))
		})

		It("Should allow transactions without a reference", func() {
			// Multiple transactions without reference should all succeed
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-2", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should allow different references in the same ledger", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"ref-a",
					),
					actions.WithReference(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-2", big.NewInt(200), "USD"),
						}, nil, nil),
						"ref-b",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("Across different ledgers", Ordered, func() {
		var (
			ledgerA = "ref-ledger-a"
			ledgerB = "ref-ledger-b"
		)

		BeforeAll(func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerA, nil),
					actions.CreateLedgerAction(ledgerB, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should allow the same reference in different ledgers", func() {
			// Create transaction with reference in ledger A
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(
						actions.CreateTransactionAction(ledgerA, []*commonpb.Posting{
							actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"shared-ref",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Same reference in ledger B should succeed
			resp, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(
						actions.CreateTransactionAction(ledgerB, []*commonpb.Posting{
							actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"shared-ref",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})
})

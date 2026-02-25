//go:build e2e

package e2e

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Transaction Reference Uniqueness", Ordered, func() {
	var (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("Within a single ledger", Ordered, func() {
		var ledgerName = "ref-test-ledger"

		BeforeAll(func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should create a transaction with a reference", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withReference(
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "account-1", big.NewInt(100), "USD"),
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withReference(
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"dup-ref",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Second transaction with the same reference fails
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withReference(
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "account-2", big.NewInt(200), "USD"),
						}, nil, nil),
						"dup-ref",
					),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionReferenceConflict))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["reference"]).To(Equal("dup-ref"))
		})

		It("Should allow transactions without a reference", func() {
			// Multiple transactions without reference should all succeed
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-2", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should allow different references in the same ledger", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withReference(
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"ref-a",
					),
					withReference(
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "account-2", big.NewInt(200), "USD"),
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
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerA, nil),
					createLedgerAction(ledgerB, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should allow the same reference in different ledgers", func() {
			// Create transaction with reference in ledger A
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withReference(
						createTransactionAction(ledgerA, []*commonpb.Posting{
							newPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"shared-ref",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Same reference in ledger B should succeed
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withReference(
						createTransactionAction(ledgerB, []*commonpb.Posting{
							newPosting("world", "account-1", big.NewInt(100), "USD"),
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

// withReference adds a reference to a create transaction request
func withReference(req *servicepb.Request, reference string) *servicepb.Request {
	switch t := req.Type.(type) {
	case *servicepb.Request_Apply:
		switch d := t.Apply.Data.(type) {
		case *servicepb.LedgerApplyRequest_CreateTransaction:
			d.CreateTransaction.Reference = reference
		}
	}
	return req
}

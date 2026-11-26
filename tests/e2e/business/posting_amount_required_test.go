//go:build e2e

package business

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("posting amount presence", func() {
	countLogs := func(ledger string) int {
		stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{Ledger: ledger})
		Expect(err).To(Succeed())
		count := 0
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return count
			}
			Expect(err).To(Succeed())
			count++
		}
	}

	DescribeTable("validates presence before committing", func(name, transport, amountJSON string, valid, secondPosting bool) {
		ledger := "amount-" + name
		idempotencyKey := "posting-amount-" + name
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())
		logsBefore := countLogs(ledger)

		if transport == "http" {
			posting := `{"source":"world","destination":"users:a","asset":"USD/2"` + amountJSON + `}`
			if secondPosting {
				posting = `{"source":"world","destination":"users:b","asset":"USD/2","amount":1},` + posting
			}
			request, err := http.NewRequestWithContext(sharedCtx, http.MethodPost,
				fmt.Sprintf("http://localhost:%d/v3/%s/transactions", sharedHTTPPort, ledger),
				strings.NewReader(`{"postings":[`+posting+`]}`))
			Expect(err).To(Succeed())
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", idempotencyKey)
			response, err := http.DefaultClient.Do(request)
			Expect(err).To(Succeed())
			body, err := io.ReadAll(response.Body)
			Expect(err).To(Succeed())
			Expect(response.Body.Close()).To(Succeed())
			if valid {
				Expect(response.StatusCode).To(Equal(http.StatusCreated), string(body))
			} else {
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest), string(body))
				Expect(string(body)).To(ContainSubstring(`"errorCode":"VALIDATION"`))
				Expect(string(body)).To(ContainSubstring("posting amount is required"))
			}
		} else {
			posting := &commonpb.Posting{Source: "world", Destination: "users:a", Asset: "USD/2"}
			if valid {
				posting.Amount = &commonpb.Uint256{}
			}
			request := servicepb.UnsignedApplyRequest(idempotencyKey, actions.CreateTransactionAction(ledger, []*commonpb.Posting{posting}, nil, nil))
			if secondPosting {
				request = servicepb.UnsignedApplyRequest(idempotencyKey,
					actions.CreateTransactionAction(ledger, []*commonpb.Posting{{Source: "world", Destination: "users:b", Asset: "USD/2", Amount: commonpb.NewUint256FromUint64(1)}}, nil, nil),
					actions.CreateTransactionAction(ledger, []*commonpb.Posting{posting}, nil, nil))
			}
			_, err := sharedClient.Apply(sharedCtx, request)
			if valid {
				Expect(err).To(Succeed())
			} else {
				Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
				Expect(status.Convert(err).Message()).To(Equal("posting amount is required"))
				details := status.Convert(err).Details()
				Expect(details).To(HaveLen(1))
				info, ok := details[0].(*errdetails.ErrorInfo)
				Expect(ok).To(BeTrue())
				Expect(info.GetReason()).To(Equal("VALIDATION"))
				Expect(info.GetDomain()).To(Equal("ledger"))
			}
		}

		stored, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: 1})
		if !valid {
			Expect(status.Code(err)).To(Equal(codes.NotFound))
			Expect(countLogs(ledger)).To(Equal(logsBefore))
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest(idempotencyKey, actions.CreateTransactionAction(ledger,
				[]*commonpb.Posting{{Source: "world", Destination: "users:a", Asset: "USD/2", Amount: &commonpb.Uint256{}}}, nil, nil)))
			Expect(err).To(Succeed())
			stored, err = sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: 1})
		}
		Expect(err).To(Succeed())
		Expect(stored.GetTransaction().GetPostings()).To(HaveLen(1))
		Expect(stored.GetTransaction().GetPostings()[0].GetAmount()).NotTo(BeNil())
		Expect(stored.GetTransaction().GetPostings()[0].GetAmount().Dec()).To(Equal("0"))
		Expect(countLogs(ledger)).To(Equal(logsBefore + 1))
	},
		Entry("HTTP omitted amount", "http-omitted", "http", "", false, false),
		Entry("HTTP null amount", "http-null", "http", `,"amount":null`, false, false),
		Entry("HTTP second posting omitted amount", "http-second", "http", "", false, true),
		Entry("HTTP explicit numeric zero", "http-zero", "http", `,"amount":0`, true, false),
		Entry("HTTP explicit string zero", "http-string-zero", "http", `,"amount":"0"`, true, false),
		Entry("gRPC omitted amount", "grpc-omitted", "grpc", "", false, false),
		Entry("gRPC invalid second envelope rejects whole batch", "grpc-batch", "grpc", "", false, true),
		Entry("gRPC present empty Uint256 is zero", "grpc-zero", "grpc", "", true, false),
	)
})

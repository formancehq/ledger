//go:build e2e

package business

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// EN-2945: admission rejects an explicit posting without an amount. The
// rejection must render as a client validation error on both public
// boundaries. An explicit zero amount stays valid.
var _ = Describe("Posting amount required (EN-2945)", Ordered, func() {
	const ledgerName = "posting-amount-required"

	postTransaction := func(body string) (int, string) {
		url := fmt.Sprintf("http://localhost:%d/v3/%s/transactions", sharedHTTPPort, ledgerName)
		req, err := http.NewRequestWithContext(sharedCtx, http.MethodPost, url, bytes.NewBufferString(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())

		return resp.StatusCode, string(raw)
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
	})

	DescribeTable("HTTP rejects a posting without an amount",
		func(posting string) {
			code, body := postTransaction(`{"postings":[` + posting + `]}`)
			Expect(code).To(Equal(http.StatusBadRequest), "body=%s", body)
			Expect(body).To(ContainSubstring(`"errorCode":"VALIDATION"`))
			Expect(body).To(ContainSubstring("posting amount required"))
		},
		Entry("omitted amount", `{"source":"world","destination":"users:omitted","asset":"USD"}`),
		Entry("null amount", `{"source":"world","destination":"users:null","amount":null,"asset":"USD"}`),
	)

	It("HTTP accepts an explicit zero amount", func() {
		code, body := postTransaction(`{"postings":[{"source":"world","destination":"users:zero","amount":0,"asset":"USD"}]}`)
		Expect(code).To(Equal(http.StatusCreated), "body=%s", body)
	})

	It("gRPC rejects a posting without an amount with InvalidArgument", func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{{
			Source: "world", Destination: "users:grpc-nil", Asset: "USD",
		}}, nil, nil)))
		Expect(err).To(HaveOccurred())
		st := status.Convert(err)
		Expect(st.Code()).To(Equal(codes.InvalidArgument), "status=%v", st)
		Expect(st.Message()).To(Equal("posting amount required"))

		var info *errdetails.ErrorInfo
		for _, detail := range st.Details() {
			if candidate, ok := detail.(*errdetails.ErrorInfo); ok {
				info = candidate
			}
		}
		Expect(info).NotTo(BeNil())
		Expect(info.GetReason()).To(Equal("VALIDATION"))
	})

	It("gRPC accepts an explicit zero amount", func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{{
			Source: "world", Destination: "users:grpc-zero", Amount: &commonpb.Uint256{}, Asset: "USD",
		}}, nil, nil)))
		Expect(err).To(Succeed())
	})
})

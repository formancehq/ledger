package suite

import (
	"net/http"

	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/numary/ledger/it/internal/otlpinterceptor"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeServerExecute("Server API", func() {
	When("reading server configuration", func() {
		var (
			response     ledgerclient.ConfigInfoResponse
			httpResponse *http.Response
		)
		BeforeEach(func() {
			response, httpResponse = MustExecute[ledgerclient.ConfigInfoResponse](GetInfo())
		})
		It("should respond with the correct configuration", func() {
			Expect(response.Data).To(Equal(ledgerclient.ConfigInfo{
				Config: ledgerclient.Config{
					Storage: ledgerclient.LedgerStorage{
						Driver:  "postgres",
						Ledgers: []string{},
					},
				},
				Server:  "numary-ledger",
				Version: "develop",
			}))
		})
		It("should register a trace", func() {
			Expect(httpResponse).To(HaveTrace(NewTrace("/_info").
				WithAttributes(HTTPStandardAttributes(http.MethodGet, "/_info", "/_info"))))
		})
	})
})

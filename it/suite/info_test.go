package suite

import (
	"context"
	"net/http"

	"github.com/numary/ledger/it/internal"
	. "github.com/numary/ledger/it/internal"
	. "github.com/numary/ledger/it/internal/otlpinterceptor"
	ledgerclient "github.com/numary/numary-sdk-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Scenario("Server API", func(env *internal.Environment) {
	When("reading server configuration", func() {
		var (
			response     ledgerclient.ConfigInfoResponse
			httpResponse *http.Response
			err          error
		)
		BeforeEach(func() {
			response, httpResponse, err = env.ServerApi.
				GetInfo(context.Background()).
				Execute()
			Expect(err).To(BeNil())
		})
		It("should respond with the correct configuration", func() {
			Expect(response.Data).To(Equal(env.ServerConfig()))
		})
		It("should register a trace", func() {
			Expect(httpResponse).To(HaveTrace(NewTrace("/_info").
				WithAttributes(HTTPStandardAttributes(http.MethodGet, "/_info", "/_info"))))
		})
	})
})

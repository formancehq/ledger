package internal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	_ "github.com/getkin/kin-openapi/openapi3"
	"github.com/numary/ledger/cmd"
	"github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/it/internal/openapi3"
	"github.com/numary/ledger/it/internal/otlpinterceptor"
	"github.com/numary/ledger/it/internal/pgserver"
	"github.com/numary/numary-sdk-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/pborman/uuid"
)

func init() {
	format.UseStringerRepresentation = true
}

func boolFlag(flag string) string {
	return fmt.Sprintf("--%s", flag)
}

func flag(flag, value string) string {
	return fmt.Sprintf("--%s=%s", flag, value)
}

func Scenario(text string, callback func(env *Environment)) bool {
	return Describe(text, func() {
		var (
			env = &Environment{}
		)
		BeforeEach(func() {
			ctx := context.Background()
			ctx = cmd.NewContext(ctx)

			appId := uuid.New()
			connString := pgserver.CreateDatabase(appId)

			rootCommand := cmd.NewRootCommand()
			rootCommand.SetArgs([]string{"server", "start",
				flag(cmd.StorageDriverFlag, "postgres"),
				flag(cmd.StoragePostgresConnectionStringFlag, connString),
				flag(cmd.StorageDirFlag, os.TempDir()),
				flag(cmd.StorageSQLiteDBNameFlag, uuid.New()),
				boolFlag(cmd.OtelTracesFlag),
				flag(cmd.OtelTracesExporterFlag, "otlp"),
				flag(cmd.OtelTracesExporterOTLPEndpointFlag, fmt.Sprintf("127.0.0.1:%d", otlpinterceptor.HTTPPort)),
				boolFlag(cmd.OtelTracesExporterOTLPInsecureFlag),
				flag(cmd.OtelTracesExporterOTLPModeFlag, "http"),
				flag(cmd.ServerHttpBindAddressFlag, ":0"),
				boolFlag(cmd.PublisherHttpEnabledFlag),
				flag(cmd.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", httplistener.URL())),
			})
			rootCommand.SetOut(io.Discard)
			//rootCommand.SetErr(os.Stderr)
			go func() {
				//defer GinkgoRecover()
				Expect(rootCommand.ExecuteContext(ctx)).To(BeNil())
			}()

			Eventually(func() any {
				return cmd.Port(ctx)
			}).Should(BeNumerically(">", 0))

			ledgerUrl := fmt.Sprintf("http://localhost:%d", cmd.Port(ctx))

			httpClient := &(*http.DefaultClient)
			httpClient.Transport = openapi3.NewTransport(ledgerUrl)

			clientConfiguration := ledgerclient.NewConfiguration()
			clientConfiguration.HTTPClient = httpClient
			clientConfiguration.Servers[0].URL = ledgerUrl
			client := ledgerclient.NewAPIClient(clientConfiguration)

			Eventually(func() error {
				_, _, err := client.ServerApi.GetInfo(ctx).Execute()
				return err
			}).Should(BeNil())

			*env = *NewEnvironment(client)
		})
		callback(env)
	})
}

func WithNewLedger(text string, callback func(ledger *string)) {
	Describe(text, func() {
		emptyString := ""
		ledger := &emptyString
		BeforeEach(func() {
			*ledger = uuid.New()
		})
		callback(ledger)
	})
}

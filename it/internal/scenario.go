package internal

import (
	"context"
	"fmt"
	"io"
	"os"

	_ "github.com/getkin/kin-openapi/openapi3"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlptraces"
	"github.com/numary/ledger/cmd"
	"github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/it/internal/ledgerclient"
	"github.com/numary/ledger/it/internal/otlpinterceptor"
	"github.com/numary/ledger/it/internal/pgserver"
	ledgerclient2 "github.com/numary/numary-sdk-go"
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

func Scenario(text string, callback func()) bool {
	return Describe(text, func() {
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
				boolFlag(sharedotlptraces.OtelTracesFlag),
				flag(sharedotlptraces.OtelTracesExporterFlag, "otlp"),
				flag(sharedotlptraces.OtelTracesExporterOTLPEndpointFlag, fmt.Sprintf("127.0.0.1:%d", otlpinterceptor.HTTPPort)),
				boolFlag(sharedotlptraces.OtelTracesExporterOTLPInsecureFlag),
				flag(sharedotlptraces.OtelTracesExporterOTLPModeFlag, "http"),
				flag(cmd.ServerHttpBindAddressFlag, ":0"),
				boolFlag(cmd.PublisherHttpEnabledFlag),
				flag(cmd.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", httplistener.URL())),
			})
			rootCommand.SetOut(io.Discard)
			rootCommand.SetErr(io.Discard)
			go func() {
				Expect(rootCommand.ExecuteContext(ctx)).To(BeNil())
			}()

			Eventually(func() any {
				return cmd.Port(ctx)
			}).Should(BeNumerically(">", 0))

			ledgerUrl := fmt.Sprintf("http://localhost:%d", cmd.Port(ctx))

			ledgerclient.Init(ledgerUrl)

			Eventually(func() error {
				_, _, err := ledgerclient.Client().ServerApi.GetInfo(ctx).Execute()
				return err
			}).Should(BeNil())
		})
		callback()
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

func Client() *ledgerclient2.APIClient {
	return ledgerclient.Client()
}

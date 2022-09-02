package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	_ "github.com/getkin/kin-openapi/openapi3"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlptraces"
	"github.com/numary/ledger/cmd"
	"github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/it/internal/otlpinterceptor"
	"github.com/numary/ledger/it/internal/pgserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/pborman/uuid"
	"github.com/spf13/cobra"
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

var (
	currentLedger string
)

func CurrentLedger() string {
	return currentLedger
}

func WithNewLedger(callback func()) {
	var oldLedger string

	BeforeEach(func() {
		oldLedger = currentLedger
		currentLedger = uuid.New()
	})
	AfterEach(func() {
		currentLedger = oldLedger
	})
	callback()
}

var (
	args          []string
	actualCommand *cobra.Command
)

func AppendArgs(newArgs ...string) {
	args = append(args, newArgs...)
}

func WithCommand(callback func()) {
	var (
		oldCommand *cobra.Command
	)
	BeforeEach(func() {
		oldCommand = actualCommand
		actualCommand = cmd.NewRootCommand()
	})
	AfterEach(func() {
		actualCommand = oldCommand
	})
	callback()
}

func Execute(callback func()) {
	BeforeEach(func() {
		ctx := context.Background()
		ctx = cmd.NewContext(ctx)

		appId := uuid.New()
		connString := pgserver.CreateDatabase(appId)

		actualCommand.SetArgs(append(args,
			flag(cmd.StorageDriverFlag, "postgres"),
			flag(cmd.StoragePostgresConnectionStringFlag, connString),
			flag(cmd.StorageDirFlag, os.TempDir()),
			flag(cmd.StorageSQLiteDBNameFlag, uuid.New()),
		))
		if !testing.Verbose() {
			actualCommand.SetOut(io.Discard)
			actualCommand.SetErr(io.Discard)
		} else {
			actualCommand.SetOut(os.Stdout)
			actualCommand.SetErr(os.Stderr)
		}

		actualCommand.SetContext(ctx)

		go func() {
			Expect(actualCommand.Execute()).To(BeNil())
		}()
	})
	callback()
}

func ServerExecute(callback func()) {
	WithCommand(func() {
		BeforeEach(func() {
			AppendArgs(
				"server", "start",
				boolFlag(sharedotlptraces.OtelTracesFlag),
				flag(sharedotlptraces.OtelTracesExporterFlag, "otlp"),
				flag(sharedotlptraces.OtelTracesExporterOTLPEndpointFlag, fmt.Sprintf("127.0.0.1:%d", otlpinterceptor.HTTPPort)),
				boolFlag(sharedotlptraces.OtelTracesExporterOTLPInsecureFlag),
				flag(sharedotlptraces.OtelTracesExporterOTLPModeFlag, "http"),
				flag(cmd.ServerHttpBindAddressFlag, ":0"),
				boolFlag(cmd.PublisherHttpEnabledFlag),
				flag(cmd.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", httplistener.URL())),
			)
		})
		Execute(func() {
			BeforeEach(func() {
				Eventually(func() any {
					return cmd.Port(actualCommand.Context())
				}).Should(BeNumerically(">", 0))

				Init(fmt.Sprintf("http://localhost:%d", cmd.Port(actualCommand.Context())))
				Eventually(func() error {
					_, _, err := GetClient().GetInfo().Execute()
					return err
				}).Should(BeNil())
			})
			callback()
		})
	})
}

func DescribeServerExecute(text string, callback func()) bool {
	return Describe(text, func() {
		ServerExecute(callback)
	})
}

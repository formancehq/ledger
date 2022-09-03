package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
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

func BoolFlag(flag string) string {
	return fmt.Sprintf("--%s", flag)
}

func Flag(flag, value string) string {
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

func Debug(format string, args ...interface{}) {
	if testing.Verbose() {
		fmt.Printf(format+"\r\n", args...)
	}
}

var (
	actualArgs    []string
	actualCommand *cobra.Command
	terminated    chan struct{}
	err           error
)

func Terminated() bool {
	select {
	case <-terminated:
		return true
	default:
		return false
	}
}

func Error() error {
	return err
}

func ActualCommand() *cobra.Command {
	return actualCommand
}

func AppendArgs(newArgs ...string) {
	actualArgs = append(actualArgs, newArgs...)
}

func PrepareCommand(callback func()) {
	var (
		oldCommand *cobra.Command
		oldArgs    []string
	)
	BeforeEach(func() {
		oldCommand = actualCommand
		oldArgs = actualArgs
		actualArgs = make([]string, 0)
		actualCommand = cmd.NewRootCommand()
	})
	AfterEach(func() {
		actualCommand = oldCommand
		actualArgs = oldArgs
	})
	callback()
}

func ExecuteCommand(callback func()) {
	var (
		ctx           context.Context
		cancel        func()
		oldTerminated chan struct{}
	)
	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		ctx = cmd.NewContext(ctx)

		Debug("Execute command with args: %s", strings.Join(actualArgs, " "))
		actualCommand.SetArgs(actualArgs)
		if !testing.Verbose() {
			actualCommand.SetOut(io.Discard)
			actualCommand.SetErr(io.Discard)
		} else {
			actualCommand.SetOut(os.Stdout)
			actualCommand.SetErr(os.Stderr)
		}

		actualCommand.SetContext(ctx)
		oldTerminated = terminated
		terminated = make(chan struct{})

		go func() {
			err = actualCommand.Execute()
			close(terminated)
		}()

		DeferCleanup(func() {
			cancel()
			<-terminated
			terminated = oldTerminated
		})
	})
	callback()
}

func ServerExecute(callback func()) {
	PrepareCommand(func() {
		BeforeEach(func() {

			appId := uuid.New()
			connString := pgserver.CreateDatabase(appId)
			SetDatabase(appId)

			AppendArgs(
				"server", "start",
				Flag(cmd.StorageDriverFlag, "postgres"),
				Flag(cmd.StoragePostgresConnectionStringFlag, connString),
				Flag(cmd.StorageDirFlag, os.TempDir()),
				Flag(cmd.StorageSQLiteDBNameFlag, uuid.New()),
				BoolFlag(sharedotlptraces.OtelTracesFlag),
				Flag(sharedotlptraces.OtelTracesExporterFlag, "otlp"),
				Flag(sharedotlptraces.OtelTracesExporterOTLPEndpointFlag, fmt.Sprintf("127.0.0.1:%d", otlpinterceptor.HTTPPort)),
				BoolFlag(sharedotlptraces.OtelTracesExporterOTLPInsecureFlag),
				Flag(sharedotlptraces.OtelTracesExporterOTLPModeFlag, "http"),
				Flag(cmd.ServerHttpBindAddressFlag, ":0"),
				BoolFlag(cmd.PublisherHttpEnabledFlag),
				Flag(cmd.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", httplistener.URL())),
			)
		})
		ExecuteCommand(func() {
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

package internal

import (
	"bytes"
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
	actualArgs           []string
	actualCommand        *cobra.Command
	currentCommandStdout *bytes.Buffer
	currentCommandStderr *bytes.Buffer
	terminated           chan struct{}
	commandError         error
)

func CommandTerminated() bool {
	select {
	case <-terminated:
		return true
	default:
		return false
	}
}

func CommandError() error {
	return commandError
}

func CommandStdout() *bytes.Buffer {
	return currentCommandStdout
}

func CommandStderr() *bytes.Buffer {
	return currentCommandStderr
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
		oldStdout     *bytes.Buffer
		oldStderr     *bytes.Buffer
	)
	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		ctx = cmd.NewContext(ctx)

		Debug("Execute command with args: %s", strings.Join(actualArgs, " "))
		actualCommand.SetArgs(actualArgs)

		oldStdout = currentCommandStdout
		oldStderr = currentCommandStderr

		stdout := io.Discard
		stderr := io.Discard
		if testing.Verbose() {
			stdout = os.Stdout
			stderr = os.Stderr
		}

		currentCommandStdout = bytes.NewBuffer(make([]byte, 0))
		currentCommandStderr = bytes.NewBuffer(make([]byte, 0))

		actualCommand.SetOut(io.MultiWriter(currentCommandStdout, stdout))
		actualCommand.SetErr(io.MultiWriter(currentCommandStderr, stderr))

		actualCommand.SetContext(ctx)
		oldTerminated = terminated
		terminated = make(chan struct{})

		go func() {
			commandError = actualCommand.Execute()
			close(terminated)
		}()

		DeferCleanup(func() {
			cancel()
			<-terminated
			terminated = oldTerminated
		})
	})
	AfterEach(func() {
		currentCommandStdout = oldStdout
		currentCommandStderr = oldStderr
	})
	callback()
}

func WhenExecuteCommand(text string, callback func()) bool {
	return Describe(text, func() {
		ExecuteCommand(callback)
	})
}

var (
	actualDatabaseName string
)

func WithNewDatabase(callback func()) {
	BeforeEach(func() {
		actualDatabaseName = uuid.New()
		_ = pgserver.CreateDatabase(actualDatabaseName)
		SetDatabase(actualDatabaseName)
	})
	callback()
}

func ActualDatabaseName() string {
	return actualDatabaseName
}

func ServerExecute(callback func()) {
	PrepareCommand(func() {
		WithNewDatabase(func() {
			BeforeEach(func() {
				AppendArgs(
					"server", "start",
					Flag(cmd.StorageDriverFlag, "postgres"),
					Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
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
	})
}

func DescribeServerExecute(text string, callback func()) bool {
	return Describe(text, func() {
		ServerExecute(callback)
	})
}

package command

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/numary/ledger/cmd"
	"github.com/numary/ledger/it/internal/debug"
	"github.com/onsi/ginkgo/v2"
	"github.com/spf13/cobra"
)

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

func NewCommand(callback func()) {
	var (
		oldCommand *cobra.Command
		oldArgs    []string
	)
	ginkgo.BeforeEach(func() {
		oldCommand = actualCommand
		oldArgs = actualArgs
		actualArgs = make([]string, 0)
		actualCommand = cmd.NewRootCommand()
	})
	ginkgo.AfterEach(func() {
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
	ginkgo.BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		ctx = cmd.NewContext(ctx)

		debug.Debug("Execute command with args: %s", strings.Join(actualArgs, " "))
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

		ginkgo.DeferCleanup(func() {
			cancel()
			<-terminated
			terminated = oldTerminated
		})
	})
	ginkgo.AfterEach(func() {
		currentCommandStdout = oldStdout
		currentCommandStderr = oldStderr
	})
	callback()
}

func WhenExecuteCommand(text string, callback func()) bool {
	return ginkgo.Describe(text, func() {
		ExecuteCommand(callback)
	})
}

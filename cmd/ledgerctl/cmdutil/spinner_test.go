package cmdutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestStartSpinnerNonInteractiveSpawnsNoGoroutine is the EN-1781 regression
// test. pterm's SpinnerPrinter.Start always launches an animation goroutine
// that its Stop cannot terminate while RawOutput is set, so the only safe
// option in a long-lived non-interactive process is to never start one.
//
// Deliberately not parallel: goleak inspects the whole goroutine dump.
func TestStartSpinnerNonInteractiveSpawnsNoGoroutine(t *testing.T) {
	ignore := goleak.IgnoreCurrent()

	spinner := startSpinner("working", false, io.Discard)
	spinner.UpdateText("still working")
	spinner.Success("done")

	goleak.VerifyNone(t, ignore)
}

// TestSpinnerNonInteractiveMatchesPterm pins the non-interactive output to what
// pterm itself emits. The reference spinner is never Started, so it spawns no
// goroutine: its terminal methods print and then hit Stop's early return.
func TestSpinnerNonInteractiveMatchesPterm(t *testing.T) {
	t.Parallel()

	require.True(t, pterm.RawOutput,
		"cmdutil.init must have put pterm in raw mode: stdout is not a terminal under go test")

	tests := []struct {
		name    string
		message string
		invoke  func(s *Spinner, message string)
		refer   func(s *pterm.SpinnerPrinter, message string)
	}{
		{
			name:    "success",
			message: "finished",
			invoke:  func(s *Spinner, m string) { s.Success(m) },
			refer:   func(s *pterm.SpinnerPrinter, m string) { s.Success(m) },
		},
		{
			name:    "fail",
			message: "broke",
			invoke:  func(s *Spinner, m string) { s.Fail(m) },
			refer:   func(s *pterm.SpinnerPrinter, m string) { s.Fail(m) },
		},
		{
			name:    "warning",
			message: "careful",
			invoke:  func(s *Spinner, m string) { s.Warning(m) },
			refer:   func(s *pterm.SpinnerPrinter, m string) { s.Warning(m) },
		},
		{
			name:    "info",
			message: "noted",
			invoke:  func(s *Spinner, m string) { s.Info(m) },
			refer:   func(s *pterm.SpinnerPrinter, m string) { s.Info(m) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var actual bytes.Buffer
			test.invoke(startSpinner("working", false, &actual), test.message)

			var expected bytes.Buffer
			reference := pterm.DefaultSpinner
			reference.Writer = &expected
			reference.Text = "working"
			test.refer(&reference, test.message)

			assert.Equal(t, expected.String(), actual.String())
		})
	}
}

// TestSpinnerNonInteractiveUpdateTextMatchesPterm pins UpdateText to pterm's
// raw-mode behaviour. pterm overwrites the current line with Fprinto only while
// styling is enabled; in raw mode it prints the new text on its own line with
// Fprintln, and Fprintln is not suppressed by RawOutput.
func TestSpinnerNonInteractiveUpdateTextMatchesPterm(t *testing.T) {
	t.Parallel()

	var actual bytes.Buffer
	startSpinner("working", false, &actual).UpdateText("still working")

	var expected bytes.Buffer
	reference := pterm.DefaultSpinner
	reference.Writer = &expected
	reference.Text = "working"
	reference.UpdateText("still working")

	assert.Equal(t, expected.String(), actual.String())
}

// TestSpinnerNonInteractiveUpdateTextRetargetsDefaultMessage checks that the
// text a message-less terminal method falls back to follows UpdateText, as it
// does in pterm, where UpdateText assigns SpinnerPrinter.Text.
func TestSpinnerNonInteractiveUpdateTextRetargetsDefaultMessage(t *testing.T) {
	t.Parallel()

	var actual bytes.Buffer

	spinner := startSpinner("working", false, &actual)
	spinner.UpdateText("still working")
	spinner.Success()

	var expected bytes.Buffer
	reference := pterm.DefaultSpinner
	reference.Writer = &expected
	reference.Text = "working"
	reference.UpdateText("still working")
	reference.Success()

	assert.Equal(t, expected.String(), actual.String())
}

// TestSpinnerNonInteractiveDefaultsMessageToText mirrors pterm, which reuses the
// spinner text when a terminal method is called with no message.
func TestSpinnerNonInteractiveDefaultsMessageToText(t *testing.T) {
	t.Parallel()

	var actual bytes.Buffer
	startSpinner("working", false, &actual).Success()

	assert.Contains(t, actual.String(), "working")
}

// TestSpinnerNonInteractiveStopIsQuiet mirrors pterm, whose Stop prints nothing
// and returns nil while RawOutput is set.
func TestSpinnerNonInteractiveStopIsQuiet(t *testing.T) {
	t.Parallel()

	var actual bytes.Buffer

	require.NoError(t, startSpinner("working", false, &actual).Stop())
	assert.Empty(t, actual.String())
}

package cmdutil

import (
	"io"

	"github.com/pterm/pterm"
)

// Spinner reports progress for a long-running command step.
//
// When stdout is a terminal it wraps a real pterm spinner and behaves exactly
// as pterm does. When stdout is not a terminal it wraps nothing: no pterm
// spinner is created, so no animation goroutine exists.
//
// That distinction is the fix for EN-1781. pterm's SpinnerPrinter.Start always
// launches a goroutine that reads pterm's package-level configuration on every
// tick, and its Stop returns early while pterm.RawOutput is set, leaving
// IsActive true so the goroutine outlives the command. In a long-lived
// non-interactive process — notably the in-process E2E suite, which runs many
// ledgerctl commands in one binary — those goroutines accumulate and race with
// any later write to pterm's configuration. Because pterm reads IsActive
// without synchronisation, such a goroutine cannot be stopped race-free; it can
// only be never started.
//
// The non-interactive path emits the same bytes pterm would: in raw mode Start
// prints nothing, fClearLine is a no-op, and the terminal methods print through
// pterm.Fprinto with the same prefix printers.
type Spinner struct {
	inner  *pterm.SpinnerPrinter
	writer io.Writer
	text   string
}

// StartSpinner starts a spinner showing text.
//
// This is the only sanctioned way to create a spinner in ledgerctl; direct use
// of pterm.DefaultSpinner is rejected by the forbidigo rule in .golangci.yaml.
func StartSpinner(text string) *Spinner {
	return startSpinner(text, interactiveOutput, pterm.DefaultSpinner.Writer)
}

// startSpinner is the injectable form used by tests.
func startSpinner(text string, interactive bool, writer io.Writer) *Spinner {
	if !interactive {
		return &Spinner{writer: writer, text: text}
	}

	inner, _ := pterm.DefaultSpinner.Start(text)

	return &Spinner{inner: inner, writer: writer, text: text}
}

// UpdateText replaces the message shown by the spinner.
//
// The non-interactive branch prints a line. That is what pterm itself does:
// SpinnerPrinter.UpdateText overwrites the current line with Fprinto only while
// styling is enabled, and falls back to Fprintln in raw mode.
func (s *Spinner) UpdateText(text string) {
	s.text = text

	if s.inner != nil {
		s.inner.UpdateText(text)

		return
	}

	pterm.Fprintln(s.writer, text)
}

// Success terminates the spinner with the success prefix.
func (s *Spinner) Success(message ...any) {
	if s.inner != nil {
		s.inner.Success(message...)

		return
	}

	s.finish(&pterm.Success, message)
}

// Fail terminates the spinner with the error prefix.
func (s *Spinner) Fail(message ...any) {
	if s.inner != nil {
		s.inner.Fail(message...)

		return
	}

	s.finish(&pterm.Error, message)
}

// Warning terminates the spinner with the warning prefix.
func (s *Spinner) Warning(message ...any) {
	if s.inner != nil {
		s.inner.Warning(message...)

		return
	}

	s.finish(&pterm.Warning, message)
}

// Info terminates the spinner with the info prefix.
func (s *Spinner) Info(message ...any) {
	if s.inner != nil {
		s.inner.Info(message...)

		return
	}

	s.finish(&pterm.Info, message)
}

// Stop terminates the spinner without printing a result.
func (s *Spinner) Stop() error {
	if s.inner != nil {
		return s.inner.Stop()
	}

	return nil
}

// finish reproduces pterm's terminal-method behaviour for the non-interactive
// path: default the message to the spinner text, then print through Fprinto.
func (s *Spinner) finish(printer *pterm.PrefixPrinter, message []any) {
	if len(message) == 0 {
		message = []any{s.text}
	}

	pterm.Fprinto(s.writer, printer.Sprint(message...))
}

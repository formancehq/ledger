package cmdutil

import (
	"os"

	"github.com/pterm/pterm"
	"golang.org/x/term"
)

func init() {
	// Enable raw output mode when stdout is not a terminal (e.g. tests, CI,
	// piped output). This disables the spinner animation goroutine, avoiding a
	// known data race in pterm's SpinnerPrinter on the IsActive field.
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		pterm.RawOutput = true
	}
}

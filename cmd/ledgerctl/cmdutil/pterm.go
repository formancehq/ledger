package cmdutil

import (
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// interactiveOutput reports whether stdout is a terminal.
//
// It is evaluated exactly once, during package initialisation and therefore
// before any command or test spec runs, and is never written again. pterm keeps
// its configuration in package-level variables that its printers read without
// synchronisation, so every later write to that configuration is a potential
// data race (EN-1781).
var interactiveOutput = term.IsTerminal(int(os.Stdout.Fd()))

func init() {
	if interactiveOutput {
		return
	}

	// Non-interactive output (tests, CI, piped output): drop styling entirely,
	// so nothing downstream has to strip escape codes.
	//
	// This does NOT prevent pterm from spawning spinner goroutines.
	// SpinnerPrinter.Start launches its animation goroutine unconditionally
	// (spinner_printer.go:157) and SpinnerPrinter.Stop returns early while
	// RawOutput is set (spinner_printer.go:184), leaving IsActive true so the
	// goroutine never exits and keeps reading pterm globals forever. That is
	// why spinners must be created through StartSpinner, which creates no
	// pterm spinner at all when output is non-interactive.
	pterm.DisableStyling()
}

// RoutePtermForStructuredOutput inspects --json / --yaml on the command and,
// when either is set, redirects every pterm printer (Info, Success, Warning,
// Error, spinners, banners, tables, ...) to stderr. The point: when the user
// asked for machine-readable output, stdout must carry only the encoded
// payload — any incidental log line on stdout breaks consumers that pipe
// `ledgerctl --json` into `jq`, parse it from K8s pod logs, etc.
//
// Human (non --json/--yaml) invocations keep pterm on its default writer
// (stdout) so tables and progress remain visible.
func RoutePtermForStructuredOutput(cmd *cobra.Command) {
	if jsonOutput, _ := cmd.Flags().GetBool("json"); jsonOutput {
		pterm.SetDefaultOutput(os.Stderr)

		return
	}

	if yamlOutput, _ := cmd.Flags().GetBool("yaml"); yamlOutput {
		pterm.SetDefaultOutput(os.Stderr)
	}
}

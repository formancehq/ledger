package cmdutil

import (
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
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

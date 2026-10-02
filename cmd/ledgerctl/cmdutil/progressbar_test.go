package cmdutil

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestStartProgressBarSpawnsNoGoroutine checks both modes while the bar is
// still active, which is when pterm's elapsed-time redraw goroutine would be
// running and racing with Update.
//
// Deliberately not parallel: goleak inspects the whole goroutine dump, and the
// interactive bar registers itself in pterm's unsynchronised
// ActiveProgressBarPrinters.
func TestStartProgressBarSpawnsNoGoroutine(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		ignore := goleak.IgnoreCurrent()

		bar := startProgressBar("working", 10, interactive, io.Discard)
		bar.Update("still working", 5, 10)

		goleak.VerifyNone(t, ignore)

		bar.Stop()
	}
}

// TestProgressBarNonInteractiveMatchesPterm pins the non-interactive output to
// what the interactive path's pterm progress bar emits in raw mode.
//
// Deliberately not parallel: the reference bar registers itself in pterm's
// unsynchronised ActiveProgressBarPrinters, which every pterm print iterates.
func TestProgressBarNonInteractiveMatchesPterm(t *testing.T) {
	require.True(t, pterm.RawOutput,
		"cmdutil.init must have put pterm in raw mode: stdout is not a terminal under go test")

	drive := func(bar *ProgressBar) {
		bar.Update("third", 1, 3)
		bar.Update("two thirds", 2, 3)
		bar.Update("no progress", 0, 3)
		bar.Update("done", 3, 3)
		bar.Update("téléchargement", 7, 13)
		bar.Update(strings.Repeat("too long for the terminal ", 4), 1, 2)
		bar.Update("empty", 0, 0)
		bar.Stop()
		bar.Update("after stop", 1, 2)
		bar.Stop()
	}

	var expected bytes.Buffer
	drive(startProgressBar("starting", 1, true, &expected))

	var actual bytes.Buffer
	drive(startProgressBar("starting", 1, false, &actual))

	assert.Equal(t, expected.String(), actual.String())
}

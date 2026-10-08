package cmdutil

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/pterm/pterm"
)

// ProgressBar reports the progress of a long-running command step as a bar
// with a title and a percentage.
//
// It never runs a goroutine of its own, in either mode: every render happens
// synchronously on the goroutine that calls Update, so nothing else reads the
// state Update writes.
//
// pterm's ProgressbarPrinter.Start launches a redraw goroutine whenever
// ShowElapsedTime is set, which is its default. That goroutine reads Title,
// Total and Current without synchronisation, so a caller that updates the bar
// races with it. pterm offers no lock to share with that goroutine, so as
// with Spinner (EN-1781) the only race-free option is to never start it.
//
// When stdout is a terminal it wraps a real pterm progress bar with the
// elapsed-time display turned off. When stdout is not a terminal it wraps
// nothing: no pterm progress bar is created, so none is registered in pterm's
// package-level ActiveProgressBarPrinters, which pterm reads on every print
// from any goroutine.
//
// The non-interactive path emits the same bytes pterm would on the bar's writer
// in raw mode: Start prints the title on its own line, each render overwrites
// the line with Fprinto, and Stop ends the line. It omits the cursor
// show/hide escape codes pterm writes to stdout, which is not a terminal.
type ProgressBar struct {
	inner   *pterm.ProgressbarPrinter
	writer  io.Writer
	title   string
	total   int
	current int
	active  bool
}

// StartProgressBar starts a progress bar showing title at zero out of total.
//
// This is the only sanctioned way to create a progress bar in ledgerctl;
// direct use of pterm.DefaultProgressbar is rejected by the forbidigo rule in
// .golangci.yaml.
func StartProgressBar(title string, total int) *ProgressBar {
	return startProgressBar(title, total, interactiveOutput, pterm.DefaultProgressbar.Writer)
}

// startProgressBar is the injectable form used by tests.
func startProgressBar(title string, total int, interactive bool, writer io.Writer) *ProgressBar {
	bar := &ProgressBar{writer: writer, title: title, total: total, active: true}

	if interactive {
		bar.inner, _ = pterm.DefaultProgressbar.
			WithTitle(title).
			WithTotal(total).
			WithShowCount(false).
			WithShowElapsedTime(false).
			WithWriter(writer).
			Start()

		return bar
	}

	pterm.Fprintln(writer, title)
	bar.render()

	return bar
}

// Update sets the title and progress, then redraws the bar.
func (b *ProgressBar) Update(title string, current, total int) {
	if b.inner != nil {
		b.inner.Total = total
		b.inner.Current = current
		b.inner.UpdateTitle(title)

		return
	}

	b.title = title
	b.total = total
	b.current = current
	b.render()
}

// Stop ends the progress bar, leaving its last render on screen.
func (b *ProgressBar) Stop() {
	if b.inner != nil {
		_, _ = b.inner.Stop()

		return
	}

	if !b.active {
		return
	}

	b.active = false
	pterm.Fprintln(b.writer)
}

func (b *ProgressBar) render() {
	pterm.Fprinto(b.writer, b.line())
}

// line reproduces pterm's ProgressbarPrinter.getString for the configuration
// the interactive path uses. It renders the percentage without pterm's
// red-to-green fade because the non-interactive path only runs with color
// disabled.
func (b *ProgressBar) line() string {
	if !b.active || b.total == 0 {
		return ""
	}

	defaults := pterm.DefaultProgressbar

	width := pterm.GetTerminalWidth()
	if defaults.MaxWidth > 0 && width >= defaults.MaxWidth {
		width = defaults.MaxWidth
	}

	before := defaults.TitleStyle.Sprint(b.title) + " "

	percentage := int(math.Round(float64(b.current) / float64(b.total) * 100))
	after := " " + fmt.Sprintf("%3d%%", percentage) + " "

	// Byte lengths, as pterm measures them.
	barMaxLength := width - len(pterm.RemoveColorFromString(before)) - len(pterm.RemoveColorFromString(after)) - 1
	barCurrentLength := (b.current * barMaxLength) / b.total

	var bar string
	if barMaxLength-barCurrentLength > 0 {
		bar = strings.Repeat(defaults.BarFiller, barMaxLength-barCurrentLength)
	}

	if barCurrentLength > 0 {
		bar = defaults.BarStyle.Sprint(strings.Repeat(defaults.BarCharacter, barCurrentLength)+defaults.LastCharacter) + bar
	}

	return before + bar + after
}

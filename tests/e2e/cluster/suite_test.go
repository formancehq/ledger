//go:build e2e

package cluster

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pterm/pterm"
)

func TestCluster(t *testing.T) {
	// pterm keeps its output settings in package-level globals. Set them once,
	// here, before any spec can start a printer. DisableOutput is preferred over
	// DisableStyling because DisableStyling also sets RawOutput, which makes
	// SpinnerPrinter.Stop return without stopping its goroutine. This only
	// suppresses test noise; it does not by itself fix the spinner lifecycle,
	// because cmd/ledgerctl/cmdutil is linked into this binary and its init sets
	// RawOutput whenever stdout is not a terminal, which is always so under
	// go test.
	pterm.DisableColor()
	pterm.DisableOutput()

	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	SetDefaultEventuallyTimeout(5 * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Cluster Suite")
}

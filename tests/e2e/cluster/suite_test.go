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
	// before any spec can start a printer. Keep styling enabled: RawOutput makes
	// SpinnerPrinter.Stop return without stopping its goroutine. Disabling output
	// suppresses test noise while preserving the normal spinner lifecycle.
	pterm.DisableColor()
	pterm.DisableOutput()

	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	SetDefaultEventuallyTimeout(5 * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Cluster Suite")
}

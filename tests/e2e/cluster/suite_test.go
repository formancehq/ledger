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
	// pterm keeps its styling settings in package-level globals, and any
	// ledgerctl command the suite invokes reads them from a spinner goroutine
	// that outlives the call. Setting them from inside a spec therefore races
	// against a spinner started by an earlier spec. Do it once, before any
	// spec runs, so no goroutine can be reading them concurrently.
	pterm.DisableColor()
	pterm.DisableStyling()

	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	SetDefaultEventuallyTimeout(5 * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Cluster Suite")
}

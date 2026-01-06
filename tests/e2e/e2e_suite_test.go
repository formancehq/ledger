//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// serviceWithClient is a shared type used across e2e tests to hold a test service instance
// along with its SDK client and Raft data directory path.
type serviceWithClient struct {
	service     *testservice.Service
	client      *client.Formance
	raftDataDir string
}

func TestE2E(t *testing.T) {
	SetDefaultEventuallyPollingInterval(100*time.Millisecond)
	SetDefaultEventuallyTimeout(10*time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

//go:build e2e

package e2e

import (
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
)

// serviceWithClient is a shared type used across e2e tests to hold a test service instance
// along with its SDK client and directory paths.
type serviceWithClient struct {
	service *testservice.Service
	client  *client.Formance
	walDir  string
	dataDir string
}



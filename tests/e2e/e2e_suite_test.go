//go:build e2e

package e2e

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestE2E(t *testing.T) {
	SetDefaultEventuallyPollingInterval(100*time.Millisecond)
	SetDefaultEventuallyTimeout(10*time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

//go:build e2e && s3

package coldstorage

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestColdStorage(t *testing.T) {
	SetDefaultEventuallyPollingInterval(200 * time.Millisecond)
	SetDefaultEventuallyTimeout(30 * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Cold Storage S3 Suite")
}

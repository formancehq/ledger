package suite_test

import (
	"testing"

	"github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/it/internal/otlpinterceptor"
	"github.com/numary/ledger/it/internal/pgserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "It Suite")
}

var _ = BeforeSuite(func() {
	otlpinterceptor.StartCollector()
	httplistener.StartServer()
	pgserver.StartServer()
})

var _ = AfterSuite(func() {
	pgserver.StopServer()
	httplistener.StopServer()
	otlpinterceptor.StopCollector()
})

func Then(text string, args ...interface{}) bool {
	return Describe(text, args...)
}

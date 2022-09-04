package tests

import (
	"testing"

	"github.com/numary/ledger/tests/internal/httplistener"
	"github.com/numary/ledger/tests/internal/otlpinterceptor"
	"github.com/numary/ledger/tests/internal/pgserver"
	"github.com/numary/ledger/tests/internal/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "It Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	DeferCleanup(pgserver.StopServer)
	return []byte(pgserver.StartServer())
}, func(url []byte) {
	pgserver.SetUrl(string(url))
	otlpinterceptor.StartCollector()
	httplistener.StartServer()
})

var _ = AfterSuite(func() {
	httplistener.StopServer()
	otlpinterceptor.StopCollector()
})

func Then(text string, args ...interface{}) bool {
	return Describe("then "+text, args...)
}

func With(text string, args ...interface{}) bool {
	return Describe("with "+text, args...)
}

func Given(text string, args ...interface{}) bool {
	return Describe("given "+text, args...)
}

func WithNewLedger(callback func()) bool {
	return With("new ledger", func() {
		server.NewLedger(callback)
	})
}

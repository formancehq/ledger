package server

import (
	"fmt"
	"os"
	"time"

	_ "github.com/getkin/kin-openapi/openapi3"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlptraces"
	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/tests/internal/command"
	. "github.com/numary/ledger/tests/internal/database"
	"github.com/numary/ledger/tests/internal/httplistener"
	"github.com/numary/ledger/tests/internal/otlpinterceptor"
	"github.com/numary/ledger/tests/internal/pgserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
)

var (
	currentLedger string
)

func CurrentLedger() string {
	return currentLedger
}

func NewLedger(callback func()) bool {
	var oldLedger string

	BeforeEach(func() {
		oldLedger = currentLedger
		currentLedger = uuid.New()
	})
	AfterEach(func() {
		currentLedger = oldLedger
	})
	callback()
	return true
}

func WithNodes(fns ...interface{}) {
	switch first := fns[0].(type) {
	case func(callback func()):
		first(func() {
			WithNodes(fns[1:]...)
		})
	case func():
		first()
	}
}

func ServerExecute(callback func()) {
	WithNodes(NewCommand, NewDatabase, func() {
		BeforeEach(func() {
			AppendArgs(
				"server", "start",
				Flag(cmd.StorageDriverFlag, "postgres"),
				Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
				Flag(cmd.StorageDirFlag, os.TempDir()),
				Flag(cmd.StorageSQLiteDBNameFlag, uuid.New()),
				BoolFlag(sharedotlptraces.OtelTracesFlag),
				Flag(sharedotlptraces.OtelTracesExporterFlag, "otlp"),
				Flag(sharedotlptraces.OtelTracesExporterOTLPEndpointFlag, fmt.Sprintf("127.0.0.1:%d", otlpinterceptor.HTTPPort+GinkgoParallelProcess())),
				BoolFlag(sharedotlptraces.OtelTracesExporterOTLPInsecureFlag),
				Flag(sharedotlptraces.OtelTracesExporterOTLPModeFlag, "http"),
				Flag(cmd.ServerHttpBindAddressFlag, ":0"),
				BoolFlag(cmd.PublisherHttpEnabledFlag),
				Flag(cmd.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", httplistener.URL())),
			)
		})
		ExecuteCommand(func() {
			BeforeEach(func() {
				Eventually(func() any {
					return cmd.Port(ActualCommand().Context())
				}).Should(BeNumerically(">", 0))

				<-time.After(time.Second)
				Init(fmt.Sprintf("http://localhost:%d", cmd.Port(ActualCommand().Context())))
				Eventually(func() error {
					_, _, err := GetInfo().Execute()
					return err
				}).Should(BeNil())
			})
			callback()
		})
	})
}

func DescribeServerExecute(text string, callback func()) bool {
	return Describe(text, func() {
		ServerExecute(callback)
	})
}

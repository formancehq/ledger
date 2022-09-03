package server

import (
	"fmt"
	"os"

	_ "github.com/getkin/kin-openapi/openapi3"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlptraces"
	"github.com/numary/ledger/cmd"
	. "github.com/numary/ledger/it/internal/command"
	. "github.com/numary/ledger/it/internal/database"
	"github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/it/internal/otlpinterceptor"
	"github.com/numary/ledger/it/internal/pgserver"
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

func WithNewLedger(callback func()) {
	var oldLedger string

	BeforeEach(func() {
		oldLedger = currentLedger
		currentLedger = uuid.New()
	})
	AfterEach(func() {
		currentLedger = oldLedger
	})
	callback()
}

func ServerExecute(callback func()) {
	PrepareCommand(func() {
		WithNewDatabase(func() {
			BeforeEach(func() {
				AppendArgs(
					"server", "start",
					Flag(cmd.StorageDriverFlag, "postgres"),
					Flag(cmd.StoragePostgresConnectionStringFlag, pgserver.ConnString(ActualDatabaseName())),
					Flag(cmd.StorageDirFlag, os.TempDir()),
					Flag(cmd.StorageSQLiteDBNameFlag, uuid.New()),
					BoolFlag(sharedotlptraces.OtelTracesFlag),
					Flag(sharedotlptraces.OtelTracesExporterFlag, "otlp"),
					Flag(sharedotlptraces.OtelTracesExporterOTLPEndpointFlag, fmt.Sprintf("127.0.0.1:%d", otlpinterceptor.HTTPPort)),
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

					Init(fmt.Sprintf("http://localhost:%d", cmd.Port(ActualCommand().Context())))
					Eventually(func() error {
						_, _, err := GetClient().GetInfo().Execute()
						return err
					}).Should(BeNil())
				})
				callback()
			})
		})
	})
}

func DescribeServerExecute(text string, callback func()) bool {
	return Describe(text, func() {
		ServerExecute(callback)
	})
}

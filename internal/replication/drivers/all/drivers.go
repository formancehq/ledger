package all

import (
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/clickhouse"
	"github.com/formancehq/ledger/internal/replication/drivers/elasticsearch"
	"github.com/formancehq/ledger/internal/replication/drivers/http"
	"github.com/formancehq/ledger/internal/replication/drivers/noop"
	"github.com/formancehq/ledger/internal/replication/drivers/stdout"
)

func Register(driversRegistry *drivers.Registry) {
	driversRegistry.RegisterConnector("elasticsearch", elasticsearch.NewConnector)
	driversRegistry.RegisterConnector("clickhouse", clickhouse.NewConnector)
	driversRegistry.RegisterConnector("stdout", stdout.NewConnector)
	driversRegistry.RegisterConnector("http", http.NewConnector)
	driversRegistry.RegisterConnector("noop", noop.NewConnector)
}

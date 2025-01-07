package all

import (
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/clickhouse"
	"github.com/formancehq/ledger/internal/replication/drivers/elasticsearch"
	"github.com/formancehq/ledger/internal/replication/drivers/http"
	"github.com/formancehq/ledger/internal/replication/drivers/noop"
	"github.com/formancehq/ledger/internal/replication/drivers/stdout"
)

func Register(connectorRegistry *drivers.Registry) {
	connectorRegistry.RegisterConnector("elasticsearch", elasticsearch.NewConnector)
	connectorRegistry.RegisterConnector("clickhouse", clickhouse.NewConnector)
	connectorRegistry.RegisterConnector("stdout", stdout.NewConnector)
	connectorRegistry.RegisterConnector("http", http.NewConnector)
	connectorRegistry.RegisterConnector("noop", noop.NewConnector)
}

package admission

import (
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/connectionurl"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// validateOrderSinkURL checks new configurations before consensus, independent
// of optional sink build tags. Existing committed entries are not revalidated.
func validateOrderSinkURL(order *raftcmdpb.Order) domain.Describable {
	add := order.GetSystemScoped().GetAddEventsSink()
	cfg := add.GetConfig()
	if nats := cfg.GetNats(); nats != nil {
		for server := range strings.SplitSeq(nats.GetUrl(), ",") {
			if _, ok := connectionurl.Parse(strings.TrimSpace(server), true, "nats", "tls", "ws", "wss"); !ok {
				return ErrSinkNATSURLInvalid
			}
		}
	}
	if clickhouse := cfg.GetClickhouse(); clickhouse != nil {
		if _, ok := connectionurl.Parse(clickhouse.GetDsn(), false, "clickhouse", "http", "https"); !ok {
			return ErrSinkClickHouseURLInvalid
		}
	}

	return nil
}

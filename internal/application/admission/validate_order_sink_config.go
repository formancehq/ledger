package admission

import (
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/connectionurl"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// validateOrderSinkConfig checks new configurations before consensus, independent
// of optional sink build tags. Existing committed entries are not revalidated.
func validateOrderSinkConfig(order *raftcmdpb.Order) domain.Describable {
	add := order.GetSystemScoped().GetAddEventsSink()
	if add == nil {
		return nil
	}
	cfg := add.GetConfig()
	if cfg.GetName() == "" || cfg.GetType() == nil {
		return ErrSinkConfigIncomplete
	}

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
	if grpcLog := cfg.GetGrpcLog(); grpcLog != nil {
		owner := grpcLog.GetOwner()
		hasSecret := len(grpcLog.GetSecretSha256()) != 0
		if hasSecret == (owner != nil) ||
			(hasSecret && len(grpcLog.GetSecretSha256()) != 32) ||
			(owner != nil && (owner.GetSource() == nil ||
				(owner.GetIssuer() == "" && owner.GetKeyId() == "") ||
				(owner.GetIssuer() != "" && owner.GetSubject() == ""))) ||
			grpcLog.GetClientId() == "" || len(grpcLog.GetClientId()) > 128 || grpcLog.GetIncarnation() != 0 ||
			cfg.GetFormat() != "" || cfg.GetBatchSize() != 0 || cfg.GetBatchDelayMs() != 0 || len(cfg.GetEventTypes()) != 0 {
			return ErrGrpcLogSinkConfigInvalid
		}
	}

	return nil
}

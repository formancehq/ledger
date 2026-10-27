package bootstrap

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newCommittedBloomFilterSet uses only replicated state. Node-local BloomConfig
// is reserved for the leader's cluster-config proposal.
func newCommittedBloomFilterSet(store *dal.Store, meterProvider metric.MeterProvider) (*bloom.FilterSet, error) {
	clusterState, err := query.ReadClusterState(store)
	if err != nil {
		return nil, fmt.Errorf("reading committed bloom configuration: %w", err)
	}

	return bloom.NewFilterSet(clusterState.GetConfig(), meterProvider.Meter("bloom")), nil
}

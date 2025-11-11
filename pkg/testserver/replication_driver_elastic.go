package testserver

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/olivere/elastic/v7"

	"github.com/formancehq/go-libs/v3/collectionutils"

	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/elasticsearch"
)

type ElasticDriver struct {
	mu       sync.Mutex
	endpoint string
	client   *elastic.Client
}

func (h *ElasticDriver) Clear(ctx context.Context) error {
	_, err := h.client.Delete().Index(elasticsearch.DefaultIndex).Do(ctx)
	return err
}

func (h *ElasticDriver) ReadMessages(ctx context.Context) ([]drivers.LogWithLedger, error) {

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.client == nil {
		var err error
		h.client, err = elastic.NewClient(elastic.SetURL(h.endpoint))
		if err != nil {
			return nil, err
		}
	}

	response, err := h.client.
		Search(elasticsearch.DefaultIndex).
		Size(1000).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return collectionutils.Map(response.Hits.Hits, func(from *elastic.SearchHit) drivers.LogWithLedger {
		ret := drivers.LogWithLedger{}
		if err := json.Unmarshal(from.Source, &ret); err != nil {
			panic(err)
		}

		return ret
	}), nil
}

func (h *ElasticDriver) Config() map[string]any {
	return map[string]any{
		"endpoint": h.endpoint,
	}
}

func (h *ElasticDriver) Name() string {
	return "elasticsearch"
}

var _ Driver = &ElasticDriver{}

func NewElasticDriver(endpoint string) Driver {
	return &ElasticDriver{
		endpoint: endpoint,
	}
}

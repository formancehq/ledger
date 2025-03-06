package elasticsearch

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

type Connector struct {
	config Config
	client *elastic.Client
	logger logging.Logger
}

func (connector *Connector) Stop(_ context.Context) error {
	connector.client.Stop()
	return nil
}

func (connector *Connector) Start(_ context.Context) error {
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(connector.config.Endpoint),
	}
	if connector.config.Authentication != nil {
		options = append(options, elastic.SetBasicAuth(connector.config.Authentication.Username, connector.config.Authentication.Password))
	}

	var err error
	connector.client, err = elastic.NewClient(options...)
	if err != nil {
		return errors.Wrap(err, "building es client")
	}

	return nil
}

func (connector *Connector) Client() *elastic.Client {
	return connector.client
}

func (connector *Connector) Accept(ctx context.Context, logs ...drivers.LogWithLedger) ([]error, error) {

	bulk := connector.client.Bulk().Refresh("true")
	for _, log := range logs {

		data, err := json.Marshal(log.Data)
		if err != nil {
			return nil, errors.Wrap(err, "marshalling data")
		}

		doc := struct {
			ID      string          `json:"id"`
			Payload json.RawMessage `json:"payload"`
			Module  string          `json:"module"`
		}{
			ID: DocID{
				Ledger: log.Ledger,
				LogID:  *log.ID,
			}.String(),
			Payload: json.RawMessage(data),
			Module:  log.Ledger,
		}

		bulk.Add(
			elastic.NewBulkIndexRequest().
				Index(connector.config.Index).
				Id(doc.ID).
				Doc(doc),
		)
	}

	rsp, err := bulk.Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query es")
	}

	ret := make([]error, len(logs))
	for index, item := range rsp.Items {
		errorDetails := item["index"].Error
		if errorDetails == nil {
			ret[index] = nil
		} else {
			ret[index] = errors.New(errorDetails.Reason)
		}
	}

	return ret, nil
}

func NewConnector(config Config, logger logging.Logger) (*Connector, error) {
	return &Connector{
		config: config,
		logger: logger,
	}, nil
}

var _ drivers.Driver = (*Connector)(nil)

type DocID struct {
	LogID  uint64 `json:"logID"`
	Ledger string `json:"ledger,omitempty"`
}

func (docID DocID) String() string {
	rawID, _ := json.Marshal(docID)
	return base64.URLEncoding.EncodeToString(rawID)
}

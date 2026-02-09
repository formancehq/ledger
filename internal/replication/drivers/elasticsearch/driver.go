package elasticsearch

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/replication/drivers"
)

type Driver struct {
	config Config
	client *elastic.Client
	logger logging.Logger
}

func (driver *Driver) Stop(_ context.Context) error {
	driver.client.Stop()
	return nil
}

func (driver *Driver) Start(_ context.Context) error {
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(driver.config.Endpoint),
	}
	if driver.config.Authentication != nil {
		options = append(options, elastic.SetBasicAuth(driver.config.Authentication.Username, driver.config.Authentication.Password))
	}

	var err error
	driver.client, err = elastic.NewClient(options...)
	if err != nil {
		return errors.Wrap(err, "building es client")
	}

	return nil
}

func (driver *Driver) Client() *elastic.Client {
	return driver.client
}

func (driver *Driver) Accept(ctx context.Context, logs ...drivers.LogWithLedger) ([]error, error) {

	bulk := driver.client.Bulk().Refresh("true")
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
				Index(driver.config.Index).
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

func NewDriver(config Config, logger logging.Logger) (*Driver, error) {
	return &Driver{
		config: config,
		logger: logger,
	}, nil
}

var _ drivers.Driver = (*Driver)(nil)

type DocID struct {
	LogID  uint64 `json:"logID"`
	Ledger string `json:"ledger,omitempty"`
}

func (docID DocID) String() string {
	rawID, err := json.Marshal(docID)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(rawID)
}

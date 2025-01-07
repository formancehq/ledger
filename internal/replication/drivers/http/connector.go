package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	ingester "github.com/formancehq/ledger/internal/replication"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"

	"github.com/formancehq/ledger/internal/replication/config"
	"github.com/formancehq/ledger/internal/replication/httpclient"
	"github.com/pkg/errors"
)

type Connector struct {
	config        Config
	serviceConfig drivers.ServiceConfig
	httpClient    *http.Client
}

func (c *Connector) Stop(_ context.Context) error {
	return nil
}

func (c *Connector) Start(_ context.Context) error {
	return nil
}

func (c *Connector) Accept(ctx context.Context, logs ...ingester.LogWithModule) ([]error, error) {
	buffer := bytes.NewBufferString("")
	err := json.NewEncoder(buffer).Encode(logs)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, c.config.URL, buffer)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	rsp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if rsp.StatusCode < 200 || rsp.StatusCode > 299 {
		return nil, fmt.Errorf("invalid status code, expect something between 200 and 299, got %d", rsp.StatusCode)
	}

	return make([]error, len(logs)), nil
}

func NewConnector(serviceConfig drivers.ServiceConfig, config Config, _ logging.Logger) (*Connector, error) {
	return &Connector{
		serviceConfig: serviceConfig,
		config:        config,
		httpClient:    httpclient.NewClient(serviceConfig),
	}, nil
}

var _ drivers.Driver = (*Connector)(nil)

type Config struct {
	URL string `json:"url"`
}

func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("empty url")
	}
	parsedURL, err := url.Parse(c.URL)
	if err != nil {
		return errors.Wrap(err, "failed to parse url")
	}
	if parsedURL.Host == "" {
		return errors.New("invalid url, host, must be defined")
	}

	return nil
}

var _ config.Validator = (*Config)(nil)

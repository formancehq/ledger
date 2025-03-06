package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/replication/config"
	"net/http"
	"net/url"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"

	"github.com/pkg/errors"
)

type Connector struct {
	config     Config
	httpClient *http.Client
}

func (c *Connector) Stop(_ context.Context) error {
	return nil
}

func (c *Connector) Start(_ context.Context) error {
	return nil
}

func (c *Connector) Accept(ctx context.Context, logs ...drivers.LogWithLedger) ([]error, error) {
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

func NewConnector(config Config, _ logging.Logger) (*Connector, error) {
	return &Connector{
		config:     config,
		httpClient: http.DefaultClient,
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

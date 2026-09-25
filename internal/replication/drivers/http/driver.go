package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/internal/replication/config"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

const (
	// maxResponseDrainBytes caps how much of an exporter's response body Accept reads
	// before closing it; ingest endpoints answer with a few bytes at most.
	maxResponseDrainBytes = 64 << 10
	// responseDrainTimeout caps how long Accept waits for those bytes. A stalled body
	// costs the connection, not the worker.
	responseDrainTimeout = 5 * time.Second
)

type Driver struct {
	config       Config
	httpClient   *http.Client
	drainTimeout time.Duration
}

func (c *Driver) Stop(_ context.Context) error {
	return nil
}

func (c *Driver) Start(_ context.Context) error {
	return nil
}

func (c *Driver) Accept(ctx context.Context, logs ...drivers.LogWithLedger) ([]error, error) {
	buffer := bytes.NewBufferString("")
	err := json.NewEncoder(buffer).Encode(logs)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, c.config.URL, buffer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req = req.WithContext(ctx)

	rsp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drain and close so the underlying connection is released back to the
		// transport instead of leaking one socket per push. The drain is bounded in
		// bytes and in time: cancelling the request context interrupts a stalled body
		// read, and the transport then drops that connection, which is preferable to
		// blocking the worker on a misbehaving exporter.
		stop := time.AfterFunc(c.drainTimeout, cancel)
		defer stop.Stop()
		_, _ = io.Copy(io.Discard, io.LimitReader(rsp.Body, maxResponseDrainBytes))
		_ = rsp.Body.Close()
	}()

	if rsp.StatusCode < 200 || rsp.StatusCode > 299 {
		return nil, fmt.Errorf("invalid status code, expect something between 200 and 299, got %d", rsp.StatusCode)
	}

	return make([]error, len(logs)), nil
}

func NewDriver(config Config, _ logging.Logger) (*Driver, error) {
	return &Driver{
		config:       config,
		httpClient:   http.DefaultClient,
		drainTimeout: responseDrainTimeout,
	}, nil
}

var _ drivers.Driver = (*Driver)(nil)

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

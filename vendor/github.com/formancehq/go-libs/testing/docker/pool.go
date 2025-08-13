package docker

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

type T interface {
	require.TestingT
	Helper()
	Cleanup(func())
}

type Pool struct {
	t      T
	pool   *dockertest.Pool
	logger logging.Logger
}

type Configuration struct {
	RunOptions         *dockertest.RunOptions
	HostConfigOptions  []func(config *docker.HostConfig)
	CheckFn            func(ctx context.Context, resource *dockertest.Resource) error
	Timeout            time.Duration
	RetryCheckInterval time.Duration
}

func (p *Pool) T() T {
	return p.t
}

func (p *Pool) streamContainerLogs(containerID string) {
	r, w, err := os.Pipe()
	if err != nil {
		p.logger.Errorf("error opening pipe: %s", err)
		return
	}

	if err := p.pool.Client.Logs(docker.LogsOptions{
		Container:    containerID,
		OutputStream: w,
		Stdout:       true,
		Stderr:       true,
		RawTerminal:  true,
		Timestamps:   true,
	}); err != nil {
		p.logger.Errorf("error reading container logs: %s", err)
		return
	}

	logging.StreamReader(p.logger, r, logging.Logger.Debug)
}

func (p *Pool) Run(cfg Configuration) *dockertest.Resource {
	p.t.Helper()

	resource, err := p.pool.RunWithOptions(cfg.RunOptions, cfg.HostConfigOptions...)
	require.NoError(p.t, err)
	if os.Getenv("NO_CLEANUP") != "true" {
		p.t.Cleanup(func() {
			require.NoError(p.t, p.pool.Purge(resource))
		})
	}

	go p.streamContainerLogs(resource.Container.ID)

	if cfg.Timeout == 0 {
		cfg.Timeout = 20 * time.Second
	}
	if cfg.RetryCheckInterval == 0 {
		cfg.RetryCheckInterval = 500 * time.Millisecond
	}

	if cfg.CheckFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		p.t.Cleanup(cancel)

		var err error
	l:
		for {
			select {
			case <-ctx.Done():
				require.Fail(p.t, fmt.Sprintf("Timeout waiting for service ready, last error was: %s", err))
			default:
				err = cfg.CheckFn(ctx, resource)
				if err != nil {
					p.logger.Debugf("check fail: %s", err)
					<-time.After(cfg.RetryCheckInterval)
					continue l
				}
				break l
			}
		}
	}

	return resource
}

func NewPool(t T, logger logging.Logger) *Pool {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	return &Pool{
		t:      t,
		pool:   pool,
		logger: logger,
	}
}

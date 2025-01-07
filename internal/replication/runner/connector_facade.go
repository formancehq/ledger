package runner

import (
	"context"
	"time"

	ingester "github.com/formancehq/ledger/internal/replication"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/pkg/errors"
)

type DriverFacade struct {
	drivers.Driver
	readyChan     chan struct{}
	logger        logging.Logger
	retryInterval time.Duration
}

func (c *DriverFacade) Ready() chan struct{} {
	return c.readyChan
}

func (c *DriverFacade) Run(ctx context.Context) {
	go func() {
		defer close(c.readyChan)
		for {
			if err := c.Driver.Start(ctx); err != nil {
				c.logger.Errorf("unable to start connector: %s", err)
				<-time.After(c.retryInterval)
				continue
			}
			return
		}
	}()
}

func (c *DriverFacade) Accept(ctx context.Context, logs ...ingester.LogWithModule) ([]error, error) {
	select {
	case <-c.readyChan:
		return c.Driver.Accept(ctx, logs...)
	default:
		return nil, errors.New("not ready connector")
	}
}

var _ drivers.Driver = (*DriverFacade)(nil)

func newDriverFacade(driver drivers.Driver, logger logging.Logger, retryInterval time.Duration) *DriverFacade {
	return &DriverFacade{
		Driver:        driver,
		readyChan:     make(chan struct{}),
		logger:        logger,
		retryInterval: retryInterval,
	}
}

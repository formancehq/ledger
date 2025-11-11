package replication

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger/internal/replication/drivers"
)

type DriverFacade struct {
	drivers.Driver
	readyChan     chan struct{}
	logger        logging.Logger
	retryInterval time.Duration

	startContext context.Context
	cancelStart  func()
	startingChan chan struct{}
}

func (c *DriverFacade) Ready() chan struct{} {
	return c.readyChan
}

func (c *DriverFacade) Run(ctx context.Context) {

	c.startContext, c.cancelStart = context.WithCancel(ctx)

	go func() {
		defer close(c.startingChan)
		for {
			if err := c.Start(c.startContext); err != nil {
				c.logger.Errorf("unable to start exporter: %s", err)
				if errors.Is(err, context.Canceled) {
					return
				}
				select {
				case <-c.startContext.Done():
					return
				case <-time.After(c.retryInterval):
				}
				continue
			}

			close(c.readyChan)
			return
		}
	}()
}

func (c *DriverFacade) Stop(ctx context.Context) error {
	select {
	case <-c.startingChan: // running phase
		// not in starting phase
	default:
		// Cancel start
		c.cancelStart()

		// Wait for the termination of the routine starting the driver
		select {
		case <-c.startingChan:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Check if driver has been started
	select {
	case <-c.readyChan:
		return c.Driver.Stop(ctx)
	default:
		return nil
	}
}

func (c *DriverFacade) Accept(ctx context.Context, logs ...drivers.LogWithLedger) ([]error, error) {
	select {
	case <-c.readyChan:
		return c.Driver.Accept(ctx, logs...)
	default:
		return nil, errors.New("not ready exporter")
	}
}

var _ drivers.Driver = (*DriverFacade)(nil)

func newDriverFacade(driver drivers.Driver, logger logging.Logger, retryInterval time.Duration) *DriverFacade {
	return &DriverFacade{
		Driver:        driver,
		readyChan:     make(chan struct{}),
		startingChan:  make(chan struct{}),
		logger:        logger,
		retryInterval: retryInterval,
	}
}

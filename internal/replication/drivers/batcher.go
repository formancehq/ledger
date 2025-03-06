package drivers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/pkg/errors"
	"go.vallahaye.net/batcher"
)

type Batcher struct {
	Driver
	mu       sync.Mutex
	batcher  *batcher.Batcher[LogWithLedger, error]
	cancel   context.CancelFunc
	stopped  chan struct{}
	batching Batching
	logger   logging.Logger
}

func (b *Batcher) Accept(ctx context.Context, logs ...LogWithLedger) ([]error, error) {
	itemsErrors := make([]error, len(logs))
	operations := make(batcher.Operations[LogWithLedger, error], len(logs))
	for ind, log := range logs {
		ret, err := b.batcher.Send(ctx, log)
		if err != nil {
			itemsErrors[ind] = fmt.Errorf("failed to send log to the batcher: %w", err)
			continue
		}
		operations[ind] = ret
	}

	for ind, operation := range operations {
		if _, err := operation.Wait(ctx); err != nil {
			itemsErrors[ind] = fmt.Errorf("failure while waiting for operation completion: %w", err)
			continue
		}
	}

	for _, err := range itemsErrors {
		if err != nil {
			return itemsErrors, fmt.Errorf("some logs failed to be sent to the batcher: %w", err)
		}
	}

	return itemsErrors, nil
}

func (b *Batcher) commit(ctx context.Context, logs batcher.Operations[LogWithLedger, error]) {
	b.logger.WithFields(map[string]any{
		"len": len(logs),
	}).Debugf("commit batch")
	itemsErrors, err := b.Driver.Accept(ctx, collectionutils.Map(logs, func(from *batcher.Operation[LogWithLedger, error]) LogWithLedger {
		return from.Value
	})...)
	if err != nil {
		for _, log := range logs {
			log.SetError(err)
		}
		return
	}
	for index, log := range logs {
		if itemsErrors[index] != nil {
			log.SetError(itemsErrors[index])
		} else {
			log.SetResult(nil)
		}
	}
}

func (b *Batcher) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.logger.Infof("starting batching with parameters: maxItems=%d, flushInterval=%s", b.batching.MaxItems, b.batching.FlushInterval)

	if err := b.Driver.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start connector")
	}

	ctx, b.cancel = context.WithCancel(ctx)
	b.stopped = make(chan struct{})
	go func() {
		defer close(b.stopped)
		b.batcher.Batch(ctx)
	}()
	return nil
}

func (b *Batcher) Stop(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.cancel == nil {
		return nil
	}

	b.logger.Infof("stopping batching")
	b.cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stopped:
		return b.Driver.Stop(ctx)
	}
}

func newBatcher(connector Driver, batching Batching, logger logging.Logger) *Batcher {
	ret := &Batcher{
		Driver:   connector,
		batching: batching,
		logger: logger.WithFields(map[string]any{
			"component": "batcher",
		}),
	}
	ret.batcher = batcher.New(
		ret.commit,
		batcher.WithTimeout[LogWithLedger, error](batching.FlushInterval),
		batcher.WithMaxSize[LogWithLedger, error](batching.MaxItems),
	)
	return ret
}

type Batching struct {
	MaxItems      int           `json:"maxItems"`
	FlushInterval time.Duration `json:"flushInterval"`
}

func (b Batching) MarshalJSON() ([]byte, error) {
	type Aux Batching
	return json.Marshal(struct {
		Aux
		FlushInterval string `json:"flushInterval,omitempty"`
	}{
		Aux:           Aux(b),
		FlushInterval: b.FlushInterval.String(),
	})
}

func (b *Batching) UnmarshalJSON(data []byte) error {
	type Aux Batching
	x := struct {
		Aux
		FlushInterval string `json:"flushInterval,omitempty"`
	}{}
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}

	*b = Batching{
		MaxItems: x.MaxItems,
	}

	if x.FlushInterval != "" {
		var err error
		b.FlushInterval, err = time.ParseDuration(x.FlushInterval)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Batching) Validate() error {
	if b.MaxItems < 0 {
		return errors.New("flushBytes must be greater than 0")
	}

	if b.MaxItems == 0 && b.FlushInterval == 0 {
		return errors.New("while configuring the batcher with unlimited size, you must configure the flush interval")
	}

	return nil
}

func (b *Batching) SetDefaults() {
	if b.MaxItems == 0 && b.FlushInterval == 0 {
		b.FlushInterval = time.Second
	}
}

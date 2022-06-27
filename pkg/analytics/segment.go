package analytics

import (
	"context"
	"time"

	"github.com/numary/go-libs/sharedlogging"
	"go.uber.org/fx"
	"gopkg.in/segmentio/analytics-go.v3"
)

const (
	ApplicationStartedEvent = "Application started"

	VersionProperty = "version"
)

type heartbeat struct {
	version  string
	interval time.Duration
	client   analytics.Client
	id       string
	stopChan chan chan struct{}
}

func (m *heartbeat) Run(ctx context.Context) error {

	enqueue := func() {
		err := m.enqueue()
		if err != nil {
			sharedlogging.GetLogger(ctx).WithFields(map[string]interface{}{
				"error": err,
			}).Error("enqueuing analytics")
		}
	}

	enqueue()
	for {
		select {
		case ch := <-m.stopChan:
			ch <- struct{}{}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.interval):
			enqueue()
		}
	}
}

func (m *heartbeat) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	m.stopChan <- ch
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (m *heartbeat) enqueue() error {
	return m.client.Enqueue(&analytics.Track{
		AnonymousId: m.id,
		Event:       ApplicationStartedEvent,
		Properties: analytics.NewProperties().
			Set(VersionProperty, m.version),
	})
}

func newHeartbeat(applicationId string, client analytics.Client, version string, interval time.Duration) *heartbeat {
	return &heartbeat{
		version:  version,
		interval: interval,
		client:   client,
		id:       applicationId,
		stopChan: make(chan chan struct{}),
	}
}

func NewHeartbeatModule(applicationId, version, writeKey string, interval time.Duration) fx.Option {
	return fx.Options(
		fx.Supply(analytics.Config{}), // Provide empty config to be able to replace (use fx.Replace) if necessary
		fx.Provide(func(cfg analytics.Config) (analytics.Client, error) {
			return analytics.NewWithConfig(writeKey, cfg)
		}),
		fx.Provide(func(client analytics.Client) *heartbeat {
			return newHeartbeat(applicationId, client, version, interval)
		}),
		fx.Invoke(func(m *heartbeat, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						err := m.Run(context.Background())
						if err != nil {
							panic(err)
						}
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return m.Stop(ctx)
				},
			})
		}),
		fx.Invoke(func(lc fx.Lifecycle, client analytics.Client) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return client.Close()
				},
			})
		}),
	)
}

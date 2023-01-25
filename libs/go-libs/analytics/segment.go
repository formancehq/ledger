package analytics

import (
	"context"
	"runtime"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/pbnjay/memory"
	"github.com/segmentio/analytics-go"
)

const (
	ApplicationStats = "Application stats"

	VersionProperty     = "version"
	OSProperty          = "os"
	ArchProperty        = "arch"
	TimeZoneProperty    = "tz"
	CPUCountProperty    = "cpuCount"
	TotalMemoryProperty = "totalMemory"
)

type AppIdProvider interface {
	AppID(ctx context.Context) (string, error)
}
type AppIdProviderFn func(ctx context.Context) (string, error)

func (fn AppIdProviderFn) AppID(ctx context.Context) (string, error) {
	return fn(ctx)
}

type PropertiesEnricher interface {
	Enrich(ctx context.Context, p analytics.Properties) error
}
type PropertiesEnricherFn func(ctx context.Context, p analytics.Properties) error

func (fn PropertiesEnricherFn) Enrich(ctx context.Context, p analytics.Properties) error {
	return fn(ctx, p)
}

type heartbeat struct {
	version       string
	interval      time.Duration
	client        analytics.Client
	stopChan      chan chan struct{}
	appIdProvider AppIdProvider
	enrichers     []PropertiesEnricher
}

func (m *heartbeat) Run(ctx context.Context) error {

	enqueue := func() {
		err := m.enqueue(ctx)
		if err != nil {
			logging.GetLogger(ctx).WithFields(map[string]interface{}{
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

func (m *heartbeat) enqueue(ctx context.Context) error {

	appId, err := m.appIdProvider.AppID(ctx)
	if err != nil {
		return err
	}

	tz, _ := time.Now().Local().Zone()

	properties := analytics.NewProperties().
		Set(VersionProperty, m.version).
		Set(OSProperty, runtime.GOOS).
		Set(ArchProperty, runtime.GOARCH).
		Set(TimeZoneProperty, tz).
		Set(CPUCountProperty, runtime.NumCPU()).
		Set(TotalMemoryProperty, memory.TotalMemory()/1024/1024)

	for _, enricher := range m.enrichers {
		if err := enricher.Enrich(ctx, properties); err != nil {
			logging.GetLogger(ctx).Errorf("Enricher return error: %s", err)
		}
	}

	return m.client.Enqueue(&analytics.Track{
		AnonymousId: appId,
		Event:       ApplicationStats,
		Properties:  properties,
	})
}

func newHeartbeat(appIdProvider AppIdProvider, client analytics.Client, version string, interval time.Duration, enrichers ...PropertiesEnricher) *heartbeat {
	return &heartbeat{
		version:       version,
		interval:      interval,
		client:        client,
		appIdProvider: appIdProvider,
		stopChan:      make(chan chan struct{}, 1),
		enrichers:     enrichers,
	}
}

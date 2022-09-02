package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"runtime"
	"time"

	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pbnjay/memory"
	"github.com/pborman/uuid"
	"go.uber.org/fx"
	"gopkg.in/segmentio/analytics-go.v3"
)

const (
	ApplicationStats = "Application stats"

	VersionProperty      = "version"
	AccountsProperty     = "accounts"
	TransactionsProperty = "transactions"
	LedgersProperty      = "ledgers"
	OSProperty           = "os"
	ArchProperty         = "arch"
	TimeZoneProperty     = "tz"
	CPUCountProperty     = "cpuCount"
	TotalMemoryProperty  = "totalMemory"
)

type AppIdProvider interface {
	AppID(ctx context.Context) (string, error)
}
type AppIdProviderFn func(ctx context.Context) (string, error)

func (fn AppIdProviderFn) AppID(ctx context.Context) (string, error) {
	return fn(ctx)
}

func FromStorageAppIdProvider(driver storage.Driver[ledger.Store]) AppIdProvider {
	var appId string
	return AppIdProviderFn(func(ctx context.Context) (string, error) {
		var err error
		if appId == "" {
			appId, err = driver.GetSystemStore().GetConfiguration(ctx, "appId")
			if err != nil && err != storage.ErrConfigurationNotFound {
				return "", err
			}
			if err == storage.ErrConfigurationNotFound {
				appId = uuid.New()
				if err := driver.GetSystemStore().InsertConfiguration(ctx, "appId", appId); err != nil {
					return "", err
				}
			}
		}
		return appId, nil
	})
}

type heartbeat struct {
	version       string
	interval      time.Duration
	client        analytics.Client
	stopChan      chan chan struct{}
	appIdProvider AppIdProvider
	driver        storage.Driver[ledger.Store]
}

func (m *heartbeat) Run(ctx context.Context) error {

	enqueue := func() {
		err := m.enqueue(ctx)
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

	ledgers, err := m.driver.GetSystemStore().ListLedgers(ctx)
	if err != nil {
		return err
	}

	ledgersProperty := map[string]any{}

	for _, l := range ledgers {
		stats := map[string]any{}
		if err := func() error {
			store, _, err := m.driver.GetLedgerStore(ctx, l, false)
			if err != nil {
				return err
			}
			transactions, err := store.CountTransactions(ctx, ledger.TransactionsQuery{})
			if err != nil {
				return err
			}
			accounts, err := store.CountAccounts(ctx, ledger.AccountsQuery{})
			if err != nil {
				return err
			}
			stats[TransactionsProperty] = transactions
			stats[AccountsProperty] = accounts

			return nil
		}(); err != nil {
			return err
		}

		digest := sha256.New()
		digest.Write([]byte(l))
		ledgerHash := base64.RawURLEncoding.EncodeToString(digest.Sum(nil))

		ledgersProperty[ledgerHash] = stats
	}
	if len(ledgersProperty) > 0 {
		properties.Set(LedgersProperty, ledgersProperty)
	}

	return m.client.Enqueue(&analytics.Track{
		AnonymousId: appId,
		Event:       ApplicationStats,
		Properties:  properties,
	})
}

func newHeartbeat(appIdProvider AppIdProvider, driver storage.Driver[ledger.Store], client analytics.Client, version string, interval time.Duration) *heartbeat {
	return &heartbeat{
		version:       version,
		interval:      interval,
		client:        client,
		driver:        driver,
		appIdProvider: appIdProvider,
		stopChan:      make(chan chan struct{}, 1),
	}
}

func NewHeartbeatModule(version, writeKey string, interval time.Duration) fx.Option {
	return fx.Options(
		fx.Supply(analytics.Config{}), // Provide empty config to be able to replace (use fx.Replace) if necessary
		fx.Provide(func(cfg analytics.Config) (analytics.Client, error) {
			return analytics.NewWithConfig(writeKey, cfg)
		}),
		fx.Provide(func(client analytics.Client, provider AppIdProvider, driver storage.Driver[ledger.Store]) *heartbeat {
			return newHeartbeat(provider, driver, client, version, interval)
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

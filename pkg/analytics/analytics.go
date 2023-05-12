package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"runtime"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pbnjay/memory"
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

type heartbeat struct {
	version  string
	interval time.Duration
	client   analytics.Client
	stopChan chan chan struct{}
	backend  Backend
}

func (m *heartbeat) Run(ctx context.Context) error {

	enqueue := func() {
		err := m.enqueue(ctx)
		if err != nil {
			logging.FromContext(ctx).WithFields(map[string]interface{}{
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

	appID, err := m.backend.AppID(ctx)
	if err != nil {
		return err
	}

	tz, _ := core.Now().Local().Zone()

	properties := analytics.NewProperties().
		Set(VersionProperty, m.version).
		Set(OSProperty, runtime.GOOS).
		Set(ArchProperty, runtime.GOARCH).
		Set(TimeZoneProperty, tz).
		Set(CPUCountProperty, runtime.NumCPU()).
		Set(TotalMemoryProperty, memory.TotalMemory()/1024/1024)

	ledgers, err := m.backend.ListLedgers(ctx)
	if err != nil {
		return err
	}

	ledgersProperty := map[string]any{}

	for _, l := range ledgers {
		stats := map[string]any{}
		if err := func() error {
			store, _, err := m.backend.GetLedgerStore(ctx, l, false)
			if err != nil {
				return err
			}

			transactions, err := store.CountTransactions(ctx)
			if err != nil {
				return err
			}

			accounts, err := store.CountAccounts(ctx)
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
		AnonymousId: appID,
		Event:       ApplicationStats,
		Properties:  properties,
	})
}

func newHeartbeat(backend Backend, client analytics.Client, version string, interval time.Duration) *heartbeat {
	return &heartbeat{
		version:  version,
		interval: interval,
		client:   client,
		backend:  backend,
		stopChan: make(chan chan struct{}, 1),
	}
}

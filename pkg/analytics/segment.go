package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/base64"

	sharedanalytics "github.com/numary/go-libs/sharedanalytics/pkg"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	analytics2 "github.com/segmentio/analytics-go"
)

const (
	AccountsProperty     = "accounts"
	TransactionsProperty = "transactions"
	LedgersProperty      = "ledgers"
)

func FromStorageAppIdProvider(driver storage.Driver) sharedanalytics.AppIdProvider {
	var appId string
	return sharedanalytics.AppIdProviderFn(func(ctx context.Context) (string, error) {
		var err error
		if appId == "" {
			appId, err = driver.GetConfiguration(ctx, "appId")
			if err != nil && err != storage.ErrConfigurationNotFound {
				return "", err
			}
			if err == storage.ErrConfigurationNotFound {
				appId = uuid.New()
				if err := driver.InsertConfiguration(ctx, "appId", appId); err != nil {
					return "", err
				}
			}
		}
		return appId, nil
	})
}

type LedgerStatsPropertiesEnricher struct {
	driver storage.Driver
}

func (l LedgerStatsPropertiesEnricher) Enrich(ctx context.Context, p analytics2.Properties) error {
	ledgers, err := l.driver.List(ctx)
	if err != nil {
		return err
	}

	ledgersProperty := map[string]any{}

	for _, ledger := range ledgers {
		stats := map[string]any{}
		if err := func() error {
			store, _, err := l.driver.GetStore(ctx, ledger, false)
			if err != nil {
				return err
			}
			transactions, err := store.CountTransactions(ctx, storage.TransactionsQuery{})
			if err != nil {
				return err
			}
			accounts, err := store.CountAccounts(ctx, storage.AccountsQuery{})
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
		digest.Write([]byte(ledger))
		ledgerHash := base64.RawURLEncoding.EncodeToString(digest.Sum(nil))

		ledgersProperty[ledgerHash] = stats
	}
	p.Set(LedgersProperty, ledgersProperty)
	return nil
}

var _ sharedanalytics.PropertiesEnricher = &LedgerStatsPropertiesEnricher{}

func NewLedgerStatsPropertiesProvider(driver storage.Driver) *LedgerStatsPropertiesEnricher {
	return &LedgerStatsPropertiesEnricher{
		driver: driver,
	}
}

package bootstrap

import (
	"fmt"
	"time"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const (
	DefaultBalanceHistoryMaintenanceInterval   = time.Second
	DefaultBalanceHistoryMaxCompactionsPerPass = 2
	MaxBalanceHistoryCompactionsPerPass        = 1_000
)

// BalanceHistoryConfig contains only physical, replica-local resource tuning.
// Whether a ledger is projected is client configuration recorded in the audit,
// never a deployment flag.
type BalanceHistoryConfig struct {
	Dir                        string        `json:"dir"                        yaml:"dir"`
	BuilderBatchSize           int           `json:"builderBatchSize"           yaml:"builderBatchSize"`
	SegmentCompactionThreshold int           `json:"segmentCompactionThreshold" yaml:"segmentCompactionThreshold"`
	MaintenanceInterval        time.Duration `json:"maintenanceInterval"        yaml:"maintenanceInterval"`
	MaxCompactionsPerPass      int           `json:"maxCompactionsPerPass"      yaml:"maxCompactionsPerPass"`
	BackfillYield              time.Duration `json:"backfillYield"              yaml:"backfillYield"`
	DurabilityInterval         time.Duration `json:"durabilityInterval"         yaml:"durabilityInterval"`
}

func DefaultBalanceHistoryConfig() BalanceHistoryConfig {
	return BalanceHistoryConfig{
		BuilderBatchSize:           appbalancehistory.DefaultBatchSize,
		SegmentCompactionThreshold: balancehistorystore.DefaultSegmentCompactionThreshold,
		MaintenanceInterval:        DefaultBalanceHistoryMaintenanceInterval,
		MaxCompactionsPerPass:      DefaultBalanceHistoryMaxCompactionsPerPass,
		BackfillYield:              appbalancehistory.DefaultBackfillYield,
		DurabilityInterval:         appbalancehistory.DefaultDurabilityInterval,
	}
}

func (c BalanceHistoryConfig) Effective() BalanceHistoryConfig {
	defaults := DefaultBalanceHistoryConfig()
	if c.BuilderBatchSize == 0 {
		c.BuilderBatchSize = defaults.BuilderBatchSize
	}
	if c.SegmentCompactionThreshold == 0 {
		c.SegmentCompactionThreshold = defaults.SegmentCompactionThreshold
	}
	if c.MaintenanceInterval == 0 {
		c.MaintenanceInterval = defaults.MaintenanceInterval
	}
	if c.MaxCompactionsPerPass == 0 {
		c.MaxCompactionsPerPass = defaults.MaxCompactionsPerPass
	}
	if c.BackfillYield == 0 {
		c.BackfillYield = defaults.BackfillYield
	}
	if c.DurabilityInterval == 0 {
		c.DurabilityInterval = defaults.DurabilityInterval
	}

	return c
}

func (c BalanceHistoryConfig) Validate() error {
	effective := c.Effective()
	if effective.BuilderBatchSize < 1 {
		return fmt.Errorf("--balance-history-builder-batch-size must be > 0 (got %d)", effective.BuilderBatchSize)
	}
	if effective.SegmentCompactionThreshold < 2 {
		return fmt.Errorf("--balance-history-segment-compaction-threshold must be >= 2 (got %d)", effective.SegmentCompactionThreshold)
	}
	if effective.MaintenanceInterval <= 0 {
		return fmt.Errorf("--balance-history-maintenance-interval must be > 0 (got %s)", effective.MaintenanceInterval)
	}
	if effective.MaxCompactionsPerPass < 1 || effective.MaxCompactionsPerPass > MaxBalanceHistoryCompactionsPerPass {
		return fmt.Errorf("--balance-history-max-compactions-per-pass must be in [1,%d] (got %d)", MaxBalanceHistoryCompactionsPerPass, effective.MaxCompactionsPerPass)
	}
	if effective.BackfillYield < 0 {
		return fmt.Errorf("--balance-history-backfill-yield must be >= 0 (got %s)", effective.BackfillYield)
	}
	if effective.DurabilityInterval < 0 {
		return fmt.Errorf("--balance-history-wal-sync-interval must be >= 0 (got %s)", effective.DurabilityInterval)
	}

	return nil
}

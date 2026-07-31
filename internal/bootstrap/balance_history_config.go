package bootstrap

import (
	"errors"
	"fmt"
	"math/bits"
	"time"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const (
	// DefaultBalanceHistoryRetainLocalRuns keeps a small warm working set after
	// immutable runs have been certified in cold storage.
	DefaultBalanceHistoryRetainLocalRuns = 8
	// DefaultBalanceHistoryArchiveCacheMaxBytes bounds hydrated cold history on
	// the replica independently from the primary Pebble store.
	DefaultBalanceHistoryArchiveCacheMaxBytes int64 = 1 << 30
	// DefaultBalanceHistoryMaxSegmentBytes bounds one cold fetch unit. The run
	// archive writer splits larger immutable runs without changing their
	// logical manifest coverage.
	DefaultBalanceHistoryMaxSegmentBytes int64 = balancehistorystore.DefaultMaxSegmentBytes
	// DefaultBalanceHistoryMaintenanceInterval bounds how long eligible local
	// runs wait before a background compaction pass.
	DefaultBalanceHistoryMaintenanceInterval = time.Second
	// DefaultBalanceHistoryMaxCompactionsPerPass drains a modest backlog while
	// keeping each pass bounded. With the default threshold four, two merges per
	// second retire up to six runs against at most five ticker publications.
	DefaultBalanceHistoryMaxCompactionsPerPass = 2
	// DefaultBalanceHistoryTierInterval limits how often one replica attempts
	// bounded cold uploads outside the builder.
	DefaultBalanceHistoryTierInterval = 5 * time.Minute
	// DefaultBalanceHistoryRemoteGCInterval keeps full owned-namespace scans
	// infrequent while the collector persists its cursor between passes.
	DefaultBalanceHistoryRemoteGCInterval = time.Hour
	// MaxBalanceHistoryRunsPerTierPass prevents a flag typo from turning one
	// asynchronous pass into an effectively unbounded upload burst.
	MaxBalanceHistoryRunsPerTierPass = 1_000
	// MaxBalanceHistoryCompactionsPerPass prevents a maintenance tick from
	// monopolizing local I/O while still allowing substantial backlog drain.
	MaxBalanceHistoryCompactionsPerPass = 1_000
	// MaxBalanceHistoryRemoteGCScanLimit caps remote enumeration work while
	// remaining large enough for operators to drain substantial inventories.
	MaxBalanceHistoryRemoteGCScanLimit = 100_000
	// MaxBalanceHistoryRemoteGCDeleteLimit caps destructive work more tightly
	// than listing so one pass cannot erase an unexpectedly large namespace.
	MaxBalanceHistoryRemoteGCDeleteLimit = 10_000
)

// BalanceHistoryConfig controls the asynchronous, rebuildable PIT projection.
// Its zero value intentionally keeps the feature disabled: retaining exact
// arbitrary history is O(monetary changes), so existing installations must
// opt in after capacity and performance planning.
type BalanceHistoryConfig struct {
	Dir                    string        `json:"dir"                    yaml:"dir"`
	Enabled                bool          `json:"enabled"                yaml:"enabled"`
	Ledgers                []string      `json:"ledgers"                yaml:"ledgers"`
	BuilderBatchSize       int           `json:"builderBatchSize"       yaml:"builderBatchSize"`
	RunCompactionThreshold int           `json:"runCompactionThreshold" yaml:"runCompactionThreshold"`
	MaintenanceInterval    time.Duration `json:"maintenanceInterval"    yaml:"maintenanceInterval"`
	MaxCompactionsPerPass  int           `json:"maxCompactionsPerPass"  yaml:"maxCompactionsPerPass"`
	BackfillYield          time.Duration `json:"backfillYield"          yaml:"backfillYield"`
	DurabilityInterval     time.Duration `json:"durabilityInterval"     yaml:"durabilityInterval"`
	VerifierInterval       time.Duration `json:"verifierInterval"       yaml:"verifierInterval"`
	VerifierReplayEvery    uint64        `json:"verifierReplayEvery"    yaml:"verifierReplayEvery"`
	ColdTierEnabled        bool          `json:"coldTierEnabled"        yaml:"coldTierEnabled"`
	RetainLocalRuns        int           `json:"retainLocalRuns"        yaml:"retainLocalRuns"`
	ArchiveCacheMaxBytes   int64         `json:"archiveCacheMaxBytes"   yaml:"archiveCacheMaxBytes"`
	MaxSegmentBytes        int64         `json:"maxSegmentBytes"        yaml:"maxSegmentBytes"`
	MaxRunsPerTierPass     int           `json:"maxRunsPerTierPass"     yaml:"maxRunsPerTierPass"`
	TierInterval           time.Duration `json:"tierInterval"           yaml:"tierInterval"`
	RemoteGCInterval       time.Duration `json:"remoteGcInterval"       yaml:"remoteGcInterval"`
	RemoteGCGracePeriod    time.Duration `json:"remoteGcGracePeriod"    yaml:"remoteGcGracePeriod"`
	RemoteGCScanLimit      int           `json:"remoteGcScanLimit"      yaml:"remoteGcScanLimit"`
	RemoteGCDeleteLimit    int           `json:"remoteGcDeleteLimit"    yaml:"remoteGcDeleteLimit"`
}

// DefaultBalanceHistoryConfig returns the server defaults in one place for
// CLI registration, config loading, bootstrap wiring, and config display.
func DefaultBalanceHistoryConfig() BalanceHistoryConfig {
	verifier := appbalancehistory.DefaultVerifierConfig()

	return BalanceHistoryConfig{
		BuilderBatchSize:       appbalancehistory.DefaultBatchSize,
		RunCompactionThreshold: balancehistorystore.DefaultRunCompactionThreshold,
		MaintenanceInterval:    DefaultBalanceHistoryMaintenanceInterval,
		MaxCompactionsPerPass:  DefaultBalanceHistoryMaxCompactionsPerPass,
		BackfillYield:          appbalancehistory.DefaultBackfillYield,
		DurabilityInterval:     appbalancehistory.DefaultDurabilityInterval,
		VerifierInterval:       verifier.Interval,
		VerifierReplayEvery:    verifier.ReplayEvery,
		RetainLocalRuns:        DefaultBalanceHistoryRetainLocalRuns,
		ArchiveCacheMaxBytes:   DefaultBalanceHistoryArchiveCacheMaxBytes,
		MaxSegmentBytes:        DefaultBalanceHistoryMaxSegmentBytes,
		MaxRunsPerTierPass:     balancehistorystore.DefaultMaxRunsPerTierPass,
		TierInterval:           DefaultBalanceHistoryTierInterval,
		RemoteGCInterval:       DefaultBalanceHistoryRemoteGCInterval,
		RemoteGCGracePeriod:    balancehistorystore.DefaultRemoteGCGracePeriod,
		RemoteGCScanLimit:      balancehistorystore.DefaultRemoteGCScanLimit,
		RemoteGCDeleteLimit:    balancehistorystore.DefaultRemoteGCDeleteLimit,
	}
}

// Effective fills zero-valued optional tuning fields from the production
// defaults. Negative values remain untouched so Validate can reject them.
func (c BalanceHistoryConfig) Effective() BalanceHistoryConfig {
	defaults := DefaultBalanceHistoryConfig()
	if c.BuilderBatchSize == 0 {
		c.BuilderBatchSize = defaults.BuilderBatchSize
	}
	if c.RunCompactionThreshold == 0 {
		c.RunCompactionThreshold = defaults.RunCompactionThreshold
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
	if c.VerifierInterval == 0 {
		c.VerifierInterval = defaults.VerifierInterval
	}
	if c.VerifierReplayEvery == 0 {
		c.VerifierReplayEvery = defaults.VerifierReplayEvery
	}
	if c.RetainLocalRuns == 0 {
		c.RetainLocalRuns = defaults.RetainLocalRuns
	}
	if c.ArchiveCacheMaxBytes == 0 {
		c.ArchiveCacheMaxBytes = defaults.ArchiveCacheMaxBytes
	}
	if c.MaxSegmentBytes == 0 {
		c.MaxSegmentBytes = defaults.MaxSegmentBytes
	}
	if c.MaxRunsPerTierPass == 0 {
		c.MaxRunsPerTierPass = defaults.MaxRunsPerTierPass
	}
	if c.TierInterval == 0 {
		c.TierInterval = defaults.TierInterval
	}
	if c.RemoteGCInterval == 0 {
		c.RemoteGCInterval = defaults.RemoteGCInterval
	}
	if c.RemoteGCGracePeriod == 0 {
		c.RemoteGCGracePeriod = defaults.RemoteGCGracePeriod
	}
	if c.RemoteGCScanLimit == 0 {
		c.RemoteGCScanLimit = defaults.RemoteGCScanLimit
	}
	if c.RemoteGCDeleteLimit == 0 {
		c.RemoteGCDeleteLimit = defaults.RemoteGCDeleteLimit
	}

	return c
}

// Validate rejects settings which would make PIT silently stop progressing or
// advertise a cold tier without a durable cold-storage backend.
func (c BalanceHistoryConfig) Validate(cold coldstorage.Config) error {
	effective := c.Effective()
	seenLedgers := make(map[string]struct{}, len(effective.Ledgers))
	for index, ledger := range effective.Ledgers {
		if ledger == "" {
			return fmt.Errorf("--balance-history-ledgers entry %d must not be empty", index+1)
		}
		if _, exists := seenLedgers[ledger]; exists {
			return fmt.Errorf("--balance-history-ledgers contains duplicate ledger %q", ledger)
		}
		seenLedgers[ledger] = struct{}{}
	}

	if effective.BuilderBatchSize < 1 {
		return fmt.Errorf("--balance-history-builder-batch-size must be > 0 (got %d)", effective.BuilderBatchSize)
	}
	if effective.RunCompactionThreshold < 2 {
		return fmt.Errorf("--balance-history-compaction-threshold must be >= 2 (got %d)", effective.RunCompactionThreshold)
	}
	if effective.MaintenanceInterval <= 0 {
		return fmt.Errorf("--balance-history-maintenance-interval must be > 0 (got %s)", effective.MaintenanceInterval)
	}
	if effective.MaxCompactionsPerPass < 1 || effective.MaxCompactionsPerPass > MaxBalanceHistoryCompactionsPerPass {
		return fmt.Errorf(
			"--balance-history-max-compactions-per-pass must be in [1,%d] (got %d)",
			MaxBalanceHistoryCompactionsPerPass,
			effective.MaxCompactionsPerPass,
		)
	}
	// Ticker-only publication can create at most one immutable run per builder
	// tick. One threshold-N merge retires N-1 runs. Reject configurations whose
	// integer retirement capacity per maintenance interval is lower than the
	// maximum number of publications in that same interval:
	//
	//   maxCompactionsPerPass * (threshold - 1) * TickInterval
	//     >= MaintenanceInterval
	//
	// Mul64 avoids overflow for intentionally large but otherwise valid
	// thresholds; a non-zero high word necessarily exceeds the duration bound.
	retirementHigh, retirementLow := bits.Mul64(
		uint64(effective.MaxCompactionsPerPass),
		uint64(effective.RunCompactionThreshold-1),
	)
	requiredRetirements := uint64(effective.MaintenanceInterval / appbalancehistory.TickInterval)
	if effective.MaintenanceInterval%appbalancehistory.TickInterval != 0 {
		requiredRetirements++
	}
	if retirementHigh == 0 && retirementLow < requiredRetirements {
		return fmt.Errorf(
			"balance history maintenance is unstable: --balance-history-max-compactions-per-pass=%d with --balance-history-compaction-threshold=%d retires at most %d runs per %s, but the %s builder ticker can publish %d; require maxCompactionsPerPass*(threshold-1)*TickInterval >= MaintenanceInterval",
			effective.MaxCompactionsPerPass,
			effective.RunCompactionThreshold,
			retirementLow,
			effective.MaintenanceInterval,
			appbalancehistory.TickInterval,
			requiredRetirements,
		)
	}
	if effective.BackfillYield < 0 {
		return fmt.Errorf("--balance-history-backfill-yield must be >= 0 (got %s)", effective.BackfillYield)
	}
	if effective.DurabilityInterval < 0 {
		return fmt.Errorf("--balance-history-wal-sync-interval must be >= 0 (got %s)", effective.DurabilityInterval)
	}
	if effective.VerifierInterval < 0 {
		return fmt.Errorf("--balance-history-verifier-interval must be >= 0 (got %s)", effective.VerifierInterval)
	}
	if effective.VerifierReplayEvery < 1 {
		return fmt.Errorf("--balance-history-verifier-replay-every must be > 0 (got %d)", effective.VerifierReplayEvery)
	}
	if effective.RetainLocalRuns < 1 {
		return fmt.Errorf("--balance-history-retain-local-runs must be > 0 (got %d)", effective.RetainLocalRuns)
	}
	if effective.ArchiveCacheMaxBytes < 1 {
		return fmt.Errorf("--balance-history-archive-cache-max-bytes must be > 0 (got %d)", effective.ArchiveCacheMaxBytes)
	}
	if effective.MaxSegmentBytes <= int64(balancehistoryarchive.EmptyEncodedSize) {
		return fmt.Errorf(
			"--balance-history-max-segment-bytes must exceed archive overhead %d (got %d)",
			balancehistoryarchive.EmptyEncodedSize,
			effective.MaxSegmentBytes,
		)
	}
	if effective.MaxRunsPerTierPass < 1 || effective.MaxRunsPerTierPass > MaxBalanceHistoryRunsPerTierPass {
		return fmt.Errorf(
			"--balance-history-max-runs-per-tier-pass must be in [1,%d] (got %d)",
			MaxBalanceHistoryRunsPerTierPass,
			effective.MaxRunsPerTierPass,
		)
	}
	if effective.TierInterval <= 0 {
		return fmt.Errorf("--balance-history-tier-interval must be > 0 (got %s)", effective.TierInterval)
	}
	if effective.RemoteGCInterval <= 0 {
		return fmt.Errorf("--balance-history-remote-gc-interval must be > 0 (got %s)", effective.RemoteGCInterval)
	}
	if effective.RemoteGCGracePeriod <= 0 {
		return fmt.Errorf("--balance-history-remote-gc-grace-period must be > 0 (got %s)", effective.RemoteGCGracePeriod)
	}
	if effective.RemoteGCScanLimit < 1 || effective.RemoteGCScanLimit > MaxBalanceHistoryRemoteGCScanLimit {
		return fmt.Errorf(
			"--balance-history-remote-gc-scan-limit must be in [1,%d] (got %d)",
			MaxBalanceHistoryRemoteGCScanLimit,
			effective.RemoteGCScanLimit,
		)
	}
	if effective.RemoteGCDeleteLimit < 1 || effective.RemoteGCDeleteLimit > MaxBalanceHistoryRemoteGCDeleteLimit {
		return fmt.Errorf(
			"--balance-history-remote-gc-delete-limit must be in [1,%d] (got %d)",
			MaxBalanceHistoryRemoteGCDeleteLimit,
			effective.RemoteGCDeleteLimit,
		)
	}
	if !effective.Enabled && effective.ColdTierEnabled {
		return errors.New("--balance-history-cold-tier requires --balance-history-enabled")
	}
	if effective.ColdTierEnabled {
		switch cold.Driver {
		case "filesystem", "s3":
		default:
			return errors.New("--balance-history-cold-tier requires --cold-storage-driver=filesystem or s3")
		}
	}

	return nil
}

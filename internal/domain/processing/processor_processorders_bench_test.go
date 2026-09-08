package processing

import (
	"fmt"
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// This file contains the ProcessOrders benchmarks requested by EN-1968: CPU
// and allocations on the FSM apply hot path, split across
//
//   - batch sizes 1 and 100;
//   - empty / small / rich ledger configurations (account types + metadata);
//   - a pure read-only transaction batch (the regime where the clone was
//     removed from processApply), and
//   - configuration changes within a batch (the regime where the redundant
//     `info = info.CloneVT()` after loadLedger was removed).
//
// The benchmark drives the real RequestProcessor.ProcessOrders dispatch (real
// handlers, real LedgerInfo CloneVT cost) against a minimal in-memory Scope
// double. The clone being measured lives in the processor/loadLedger path, not
// in the storage layer, so the double isolates the exact allocation the PR
// changes while keeping the benchmark self-contained in this package.
//
// To compare base vs. candidate, run the same benchmark on both revisions and
// diff the results:
//
//	go test ./internal/domain/processing -run '^$' -bench 'BenchmarkProcessOrders' -benchmem -count 5
//
// and compare the two runs with benchstat. The benchmark only depends on the
// stable RequestProcessor/Scope test surface, so it compiles unchanged on the
// base revision.
//
// Benchmarks over the same scope instance must reset the mutable state between
// iterations because ProcessOrders consumes and advances it (log/transaction
// IDs, ledger writes for configuration changes). The reset is pointer-only and
// runs inside the timed loop so steady-state behaviour is what gets measured.

// benchScope is the minimal Scope surface the benchmarked orders touch. Ledger,
// boundary, volume and transaction-state reads/writes go through kindStub, the
// shared no-op/hook stub; everything else stays on the nil embedded Scope so an
// unexpected call fails loudly instead of being silently accepted.
type benchScope struct {
	Scope

	ledgers      *kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	boundaries   *kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	volumes      *kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	transactions *kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]

	date         commonpb.TimestampReader
	logSequence  uint64
	nextSequence uint64

	baseInfo          *commonpb.LedgerInfo
	info              *commonpb.LedgerInfo
	baseBoundaries    *raftcmdpb.LedgerBoundaries
	currentBoundaries *raftcmdpb.LedgerBoundaries
}

func (s *benchScope) Ledgers() Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	return s.ledgers
}

func (s *benchScope) Boundaries() Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	return s.boundaries
}

func (s *benchScope) Volumes() Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	return s.volumes
}

func (s *benchScope) TransactionStates() Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader] {
	return s.transactions
}

func (s *benchScope) GetDate() commonpb.TimestampReader {
	return s.date
}

func (s *benchScope) GetNextSequenceID() uint64 {
	return s.nextSequence
}

func (s *benchScope) IncrementNextSequenceID() uint64 {
	s.logSequence++

	return s.logSequence
}

// reset restores the pre-batch ledger and boundary snapshots for the next
// benchmark iteration. It only reassigns pointers; no allocation happens here.
func (s *benchScope) reset() {
	s.info = s.baseInfo
	s.currentBoundaries = s.baseBoundaries
	s.logSequence = 0
}

// benchLedgerConfig describes the ledger the scope exposes to ProcessOrders.
type benchLedgerConfig struct {
	name         string
	accountTypes map[string]*commonpb.AccountType
	metadata     map[string]*commonpb.MetadataValue
}

func emptyLedgerConfig() benchLedgerConfig {
	return benchLedgerConfig{name: "bench-ledger"}
}

func smallLedgerConfig() benchLedgerConfig {
	return benchLedgerConfig{
		name: "bench-ledger",
		accountTypes: map[string]*commonpb.AccountType{
			"user":     {Name: "user", Pattern: "users:{id}"},
			"merchant": {Name: "merchant", Pattern: "merchants:{id}"},
		},
		metadata: map[string]*commonpb.MetadataValue{
			"region":   stringMetadata("eu-west"),
			"platform": stringMetadata("checkout"),
		},
	}
}

func richLedgerConfig() benchLedgerConfig {
	accountTypes := make(map[string]*commonpb.AccountType, 50)
	metadata := make(map[string]*commonpb.MetadataValue, 50)

	for i := range 50 {
		name := fmt.Sprintf("type-%02d", i)
		accountTypes[name] = &commonpb.AccountType{
			Name:    name,
			Pattern: fmt.Sprintf("t%02d:{id}", i),
		}
		metadata[fmt.Sprintf("key-%02d", i)] = stringMetadata(fmt.Sprintf("value-%02d", i))
	}

	// The transaction benchmark addresses users:{id}; keep that type present so
	// the rich configuration still exercises the account-type matching path.
	accountTypes["user"] = &commonpb.AccountType{Name: "user", Pattern: "users:{id}"}

	return benchLedgerConfig{
		name:         "bench-ledger",
		accountTypes: accountTypes,
		metadata:     metadata,
	}
}

func stringMetadata(value string) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{
		Type: &commonpb.MetadataValue_StringValue{StringValue: value},
	}
}

func newBenchScope(cfg benchLedgerConfig) *benchScope {
	baseInfo := &commonpb.LedgerInfo{
		Name:                   cfg.name,
		Id:                     1,
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes:           cfg.accountTypes,
		Metadata:               cfg.metadata,
	}
	baseBoundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	s := &benchScope{
		date:              (&commonpb.Timestamp{Data: 1700000000}).AsReader(),
		nextSequence:      1,
		baseInfo:          baseInfo,
		info:              baseInfo,
		baseBoundaries:    baseBoundaries,
		currentBoundaries: baseBoundaries,
	}

	s.ledgers = &kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]{}
	s.ledgers.onGet(func(domain.LedgerKey) (commonpb.LedgerInfoReader, error) {
		return s.info.AsReader(), nil
	})
	s.ledgers.onPut(func(_ domain.LedgerKey, info *commonpb.LedgerInfo) {
		s.info = info
	})

	s.boundaries = &kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]{}
	s.boundaries.onGet(func(domain.LedgerKey) (raftcmdpb.LedgerBoundariesReader, error) {
		return s.currentBoundaries.AsReader(), nil
	})
	s.boundaries.onPut(func(_ domain.LedgerKey, boundaries *raftcmdpb.LedgerBoundaries) {
		s.currentBoundaries = boundaries
	})

	s.volumes = &kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]{}
	s.volumes.onGet(func(domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
		return zero.AsReader(), nil
	})

	s.transactions = &kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{}

	return s
}

func benchCreateTransactionOrder(ledger string, i int) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{
								{
									Source:      "world",
									Destination: fmt.Sprintf("users:%06d", i),
									Amount:      commonpb.NewUint256FromUint64(1),
									Asset:       "USD",
								},
							},
							Force: true,
						},
					},
					},
				},
			},
		},
	}
}

func benchAddAccountTypeOrder(ledger string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
						AddAccountType: &raftcmdpb.AddAccountTypeOrder{
							AccountType: &commonpb.AccountType{Name: "new-type", Pattern: "bench:{id}"},
						},
					},
					},
				},
			},
		},
	}
}

func benchConfigs() []struct {
	name string
	cfg  benchLedgerConfig
} {
	return []struct {
		name string
		cfg  benchLedgerConfig
	}{
		{name: "empty", cfg: emptyLedgerConfig()},
		{name: "small", cfg: smallLedgerConfig()},
		{name: "rich", cfg: richLedgerConfig()},
	}
}

// BenchmarkProcessOrders_CreateTransaction measures a batch of read-only
// CreateTransaction orders. This is the regime fixed by no longer cloning the
// ledger in processApply for every order.
func BenchmarkProcessOrders_CreateTransaction(b *testing.B) {
	for _, tc := range benchConfigs() {
		for _, n := range []int{1, 100} {
			b.Run(fmt.Sprintf("%s/n=%d", tc.name, n), func(b *testing.B) {
				processor, err := NewRequestProcessor(nil, 0)
				if err != nil {
					b.Fatal(err)
				}

				scope := newBenchScope(tc.cfg)
				orders := make([]*raftcmdpb.Order, n)
				for i := range orders {
					orders[i] = benchCreateTransactionOrder(tc.cfg.name, i)
				}

				factory := mockFactory(scope)
				sink := noopSink{}

				b.ReportAllocs()
				for b.Loop() {
					scope.reset()

					if _, derr := processor.ProcessOrders(orders, factory, sink); derr != nil {
						b.Fatal(derr)
					}
				}
			})
		}
	}
}

// BenchmarkProcessOrders_ConfigurationChanges measures a batch that edits
// ledger configuration (one AddAccountType) followed by read-only
// transactions. This is the regime fixed by removing the redundant
// `info = info.CloneVT()` after loadLedger in configuration-mutating handlers.
func BenchmarkProcessOrders_ConfigurationChanges(b *testing.B) {
	for _, tc := range benchConfigs() {
		for _, n := range []int{1, 100} {
			b.Run(fmt.Sprintf("%s/n=%d", tc.name, n), func(b *testing.B) {
				processor, err := NewRequestProcessor(nil, 0)
				if err != nil {
					b.Fatal(err)
				}

				scope := newBenchScope(tc.cfg)
				orders := make([]*raftcmdpb.Order, 0, n)
				orders = append(orders, benchAddAccountTypeOrder(tc.cfg.name))
				for i := 1; i < n; i++ {
					orders = append(orders, benchCreateTransactionOrder(tc.cfg.name, i))
				}

				factory := mockFactory(scope)
				sink := noopSink{}

				b.ReportAllocs()
				for b.Loop() {
					scope.reset()

					if _, derr := processor.ProcessOrders(orders, factory, sink); derr != nil {
						b.Fatal(derr)
					}
				}
			})
		}
	}
}

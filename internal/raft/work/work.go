package work

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/ledger-v3-poc/internal/store/pebble"
)

type Generation struct {
	BaseIndex uint64
	Balances  map[BalanceKey]*raftcmdpb.BalanceHolder
	Metadata  map[AccountKey]*commonpb.Metadata
}

type Pinned struct {
	Infos          map[string]*commonpb.LedgerInfo
	Ledgers        map[string]*raftcmdpb.LedgerBoundaries
	NextLedgerID   uint32
	NextSequenceID uint64
}

type Memory struct {
	Volatile DualGen[*Generation]
	Pinned   Pinned
}

func (m *Memory) Preload(preloadSet *raftcmdpb.PreloadSet) error {
	// The preloads must be for the gen0 or the gen1
	// This is the role of the admission to ensure this invariant
	var generation *Generation
	if m.Volatile.Gen0 != nil && m.Volatile.Gen0.BaseIndex == preloadSet.LastPersistedIndex {
		generation = m.Volatile.Gen0
	} else if m.Volatile.Gen1 != nil && m.Volatile.Gen1.BaseIndex == preloadSet.LastPersistedIndex {
		generation = m.Volatile.Gen1
	}

	if generation == nil {
		var gen1BaseIndex uint64
		if m.Volatile.Gen1 != nil {
			gen1BaseIndex = m.Volatile.Gen1.BaseIndex
		}
		return fmt.Errorf(
			"generation not matching, expected %d or %d, got %d",
			m.Volatile.Gen0.BaseIndex,
			gen1BaseIndex,
			preloadSet.LastPersistedIndex,
		)
	}

	for _, preload := range preloadSet.GetPreloads() {
		switch preloadType := preload.Type.(type) {
		case *raftcmdpb.Preload_Balances:
			for ledgerName, balancesForLedger := range preloadType.Balances.GetByLedger() {
				for account, balancesByAsset := range balancesForLedger.ByAccount {
					for asset, balance := range balancesByAsset.ByAsset {
						finalBalance := balance.Value()

						balanceKey := BalanceKey{
							AccountKey: AccountKey{
								LedgerName: ledgerName,
								Account:    account,
							},
							Asset: asset,
						}

						if m.Volatile.Gen1 != nil && preloadSet.LastPersistedIndex == m.Volatile.Gen1.BaseIndex {
							h, ok := m.Volatile.Gen1.Balances[balanceKey]
							if ok && h.DiffSinceBaseIndex != nil {
								finalBalance = finalBalance.Add(finalBalance, h.DiffSinceBaseIndex.Value())
							}
						}

						h := m.Volatile.Gen0.Balances[balanceKey]
						if h == nil {
							h = &raftcmdpb.BalanceHolder{}
							m.Volatile.Gen0.Balances[balanceKey] = h
						}
						if h.Known != nil {
							// Can receive redundant preload
							continue
						}
						if h.DiffSinceBaseIndex != nil {
							finalBalance = finalBalance.Add(finalBalance, h.DiffSinceBaseIndex.Value())
						}
						h.Known = commonpb.NewBigInt(finalBalance)
						h.DiffSinceBaseIndex = nil
					}
				}
			}
		}
	}

	return nil
}

func NewMemory() *Memory {
	return &Memory{
		Volatile: DualGen[*Generation]{
			Gen0: &Generation{
				BaseIndex: 1,
				Balances:  make(map[BalanceKey]*raftcmdpb.BalanceHolder),
				Metadata:  make(map[AccountKey]*commonpb.Metadata),
			},
			Gen1: &Generation{
				BaseIndex: 0,
				Balances:  make(map[BalanceKey]*raftcmdpb.BalanceHolder),
				Metadata:  make(map[AccountKey]*commonpb.Metadata),
			},
		},
		Pinned: Pinned{
			Infos:          make(map[string]*commonpb.LedgerInfo),
			Ledgers:        make(map[string]*raftcmdpb.LedgerBoundaries),
			NextLedgerID:   1,
			NextSequenceID: 1,
		},
	}
}

type Buffered struct {
	WorkMemory       *Memory
	Date             *commonpb.Timestamp
	NextLedgerID     uint32
	NextSequenceID   uint64
	Infos            CopyOnAccessMap[string, *commonpb.LedgerInfo]
	Boundaries       CopyOnAccessMap[string, *raftcmdpb.LedgerBoundaries]
	Balances         CopyOnAccessMap[BalanceKey, *raftcmdpb.BalanceHolder]
	AccumulatedDiffs map[BalanceKey]*big.Int
	AccountMetadata  CopyOnAccessMap[AccountKey, *commonpb.Metadata]
	PendingLogs      []*commonpb.Log
}

func (m *Buffered) Merge(index uint64, s *pebble.Store) error {

	batch := s.NewBatch(index)
	defer func() {
		_ = batch.Cancel()
	}()

	for _, update := range m.Infos.Updates() {
		if err := batch.SaveLedger(update); err != nil {
			return err
		}
	}

	infos := m.Infos.Merge()
	for balanceKey, diff := range m.AccumulatedDiffs {
		err := batch.AppendBalanceDiff(store.BalanceDiff{
			LedgerID:  infos[balanceKey.LedgerName].Id,
			Account:   balanceKey.Account,
			Asset:     balanceKey.Asset,
			Diff:      commonpb.NewBigInt(diff),
			RaftIndex: index,
		})
		if err != nil {
			return err
		}
	}

	for accountKey, metadata := range m.AccountMetadata.Updates() {
		err := batch.SaveAccountMetadata(infos[accountKey.LedgerName].Id, accountKey.Account, metadata)
		if err != nil {
			return err
		}
	}

	err := batch.AppendLogs(m.PendingLogs...)
	if err != nil {
		return err
	}
	m.PendingLogs = nil

	m.WorkMemory.Pinned = Pinned{
		Infos:          infos,
		Ledgers:        m.Boundaries.Merge(),
		NextLedgerID:   m.NextLedgerID,
		NextSequenceID: m.NextSequenceID,
	}
	m.WorkMemory.Volatile.Gen0.Balances = m.Balances.Merge()
	m.WorkMemory.Volatile.Gen0.Metadata = m.AccountMetadata.Merge()

	return batch.Commit()
}

func (m *Buffered) AppendDiff(key BalanceKey, delta *big.Int) {
	m.AccumulatedDiffs[key] = delta.Add(m.AccumulatedDiffs[key], delta)
	value := m.Balances.LoadOrInitWithZeroValue(key)
	ApplyDiff(value, delta)
}

func NewBuffer(at *commonpb.Timestamp, m *Memory) *Buffered {
	return &Buffered{
		WorkMemory:      m,
		Date:            at,
		Boundaries:      NewCopyOnAccessMap(m.Pinned.Ledgers),
		NextLedgerID:    m.Pinned.NextLedgerID,
		NextSequenceID:  m.Pinned.NextSequenceID,
		Balances:        NewCopyOnAccessMap(m.Volatile.Gen0.Balances, m.Volatile.Gen1.Balances),
		AccountMetadata: NewCopyOnAccessMap(m.Volatile.Gen0.Metadata, m.Volatile.Gen1.Metadata),
	}
}

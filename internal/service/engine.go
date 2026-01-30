package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

type Applier interface {
	Apply(ctx context.Context, actions ...*raftcmdpb.Action) ([]*commonpb.Log, error)
}

//type balance struct {
//	sync.Mutex
//	amount     *commonpb.BigInt
//	references uint
//}
//
//type cacheBalancesByAsset = utils.SyncMap[string, *balance]
//
//type cacheBalancesByAccount = utils.SyncMap[string, *cacheBalancesByAsset]
//
//type cacheBalances = utils.SyncMap[uint32, *cacheBalancesByAccount]
//
//type inflightDiff struct {
//	sync.Mutex
//	diffs []*big.Int
//}
//
//type inflightDiffsByAsset = utils.SyncMap[string, *inflightDiff]
//
//type inflightDiffsByAccount = utils.SyncMap[string, *inflightDiffsByAsset]
//
//type inflightDiffs = utils.SyncMap[uint32, *inflightDiffsByAccount]
//
//type DefaultEngine struct {
//	store store.Store
//	applier Applier
//	cache cacheBalances
//	// inflightDiffs represents the diffs that are currently being applied on the raft system
//	// it can contain diffs for accounts which are not in the cache, since we track all movements
//	// including those where no reservation has been made
//	inflightDiffs inflightDiffs
//	// reservations represent the reservations that are currently being hold by the application
//	//reservations utils.SyncMap[uint32, utils.SyncMap[string, utils.SyncMap[string, *big.Int]]]
//}
//
//func (engine *DefaultEngine) GetBalance(ctx context.Context, ledgerID uint32, account, asset string) (*commonpb.BigInt, error) {
//	cacheBalancesByAccount, _ := engine.cache.LoadOrStore(ledgerID, &cacheBalancesByAccount{})
//	cacheBalancesByAsset, _ := cacheBalancesByAccount.LoadOrStore(account, &cacheBalancesByAsset{})
//	cacheBalance, _ := cacheBalancesByAsset.LoadOrStore(asset, &balance{})
//	if cacheBalance.amount == nil {
//		balance, err := engine.store.GetBalance(ctx, ledgerID, account, asset)
//		if err != nil {
//			return nil, err
//		}
//		cacheBalance.amount = balance
//	}
//
//	cacheBalance.Lock()
//	cacheBalance.references++
//	inflightDiffsByAccount, _ := engine.inflightDiffs.LoadOrStore(ledgerID, &inflightDiffsByAccount{})
//	inflightDiffsByAsset, _ := inflightDiffsByAccount.LoadOrStore(account, &inflightDiffsByAsset{})
//	inflightDiff, _ := inflightDiffsByAsset.Load(asset)
//	ret := new(big.Int).Set(cacheBalance.amount.Value())
//	for _, diff := range inflightDiff.diffs {
//		ret = ret.Add(ret, diff)
//	}
//	cacheBalance.Unlock()
//
//	return commonpb.NewBigInt(ret), nil
//}
//
//func (engine *DefaultEngine) Apply(ctx context.Context, actions ...*raftcmdpb.Action) ([]*commonpb.Log, error) {
//	for _, action := range actions {
//		switch cmd := action.Command.GetCommand().(type) {
//		case *raftcmdpb.AnyCommand_CreateLedgerLog:
//			switch createLedgerLog := cmd.CreateLedgerLog.GetCommand().(type) {
//			case *raftcmdpb.CreateLedgerLogCommand_AppendTransaction:
//				inflightDiffsByAccount, _ := engine.inflightDiffs.LoadOrStore(
//					cmd.CreateLedgerLog.LedgerId,
//					&inflightDiffsByAccount{},
//				)
//
//				appendDiff := func(account, asset string, amount *big.Int) {
//					inflightDiffsByAccount, _ := inflightDiffsByAccount.LoadOrStore(account, &inflightDiffsByAsset{})
//					inflightDiff, _ := inflightDiffsByAccount.LoadOrStore(asset, &inflightDiff{})
//					inflightDiff.Lock()
//					inflightDiff.diffs = append(inflightDiff.diffs, new(big.Int).Neg(amount))
//					inflightDiff.Unlock()
//				}
//
//				for _, posting := range createLedgerLog.AppendTransaction.Postings {
//					appendDiff(posting.Source, posting.Asset, posting.Amount.Value())
//					appendDiff(posting.Destination, posting.Asset, posting.Amount.Value())
//				}
//			default:
//				// Nothing to do actually
//			}
//		default:
//			// Nothing to do actually
//		}
//	}
//
//	ret, err := engine.applier.Apply(ctx, actions...)
//	if err != nil {
//		return nil, err
//	}
//
//
//}

//func (b *Balances) ReserveBalance(ledgerID uint32, account, asset string, amount *big.Int) (uint64, error) {
//	balancesLedgerCache, _ := b.cache.LoadOrStore(ledgerID, &balancesLedgersCache{})
//	accountBalances, _ := balancesLedgerCache.LoadOrStore(account, &balancesAccount{})
//	accountBalance, _ := accountBalances.LoadOrStore(asset, &balance{})
//	if accountBalance.amount == nil {
//		balance, err := b.store.GetBalance(ledgerID, account, asset)
//		if err != nil {
//			return 0, err
//		}
//		accountBalance.amount = balance
//	}
//
//	return 0, nil
//}
//
//type cachedBalances struct {
//	sync.Mutex
//	// references register the number of active references to this balance
//	references int
//	// actualBalance fetched from store
//	// If nil, it indicates a not loaded balance
//	actualBalance *big.Int
//}
//
//type Handle[V any] struct {
//	mu         sync.Mutex
//	references int
//	value      V
//	onRelease  func()
//}
//
//func (h *Handle[V]) release() {
//	h.mu.Lock()
//	defer h.mu.Unlock()
//	h.references--
//}
//
//type Cache[K comparable, V any] struct {
//	mu      sync.Mutex
//	cache   map[K]*Handle[V]
//	initFn func(ctx context.Context) (V, error)
//}
//
//func (b *Cache[K, V]) Get(ctx context.Context, key K) (*Handle[V], error) {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//
//	handle, ok := b.cache[key]
//	if !ok {
//		value, err := b.initFn(ctx)
//		if err != nil {
//			return nil, err
//		}
//		handle = &Handle[V]{
//			references: 1,
//			value: value,
//			onRelease: func() {
//				b.mu.Lock()
//				defer b.mu.Unlock()
//
//				if handle.references == 0 {
//					delete(b.cache, key)
//				}
//			},
//		}
//		b.cache[key] = handle
//	}
//
//	return handle, nil
//}
//
//func (b *Cache) Apply(ctx context.Context, actions ...*raftcmdpb.Action) ([]*commonpb.Log, error) {
//
//	var usedBalances = make(map[*cachedBalances]int)
//	for _, action := range actions {
//		switch cmd := action.Command.GetCommand().(type) {
//		case *raftcmdpb.AnyCommand_CreateLedgerLog:
//			addDiff := func(account, asset string, amount *big.Int) error {
//				accountKey := b.computeAccountKey(cmd.CreateLedgerLog.LedgerId, account, asset)
//				cachedBalances, _ := b.cache.LoadOrStore(accountKey, &cachedBalances{})
//				cachedBalances.Lock()
//				if cachedBalances.actualBalance == nil {
//					balance, err := b.store.GetBalance(ctx, cmd.CreateLedgerLog.LedgerId, account, asset)
//					if err != nil {
//						return err
//					}
//
//					cachedBalances.actualBalance = balance.Value()
//				}
//				cachedBalances.actualBalance.Add(cachedBalances.actualBalance, amount)
//				cachedBalances.references++
//				cachedBalances.Unlock()
//
//				usedBalances[cachedBalances]++
//
//				return nil
//			}
//			processTx := func(cmd *raftcmdpb.AppendTransactionCommand) error {
//				for _, posting := range cmd.Postings {
//					if err := addDiff(posting.Source, posting.Asset, posting.Amount.Value()); err != nil {
//						return err
//					}
//					if err := addDiff(posting.Destination, posting.Asset, posting.Amount.Value()); err != nil {
//						return err
//					}
//				}
//				return nil
//			}
//
//			switch createLedgerLog := cmd.CreateLedgerLog.GetCommand().(type) {
//			case *raftcmdpb.CreateLedgerLogCommand_AppendTransaction:
//				if err := processTx(createLedgerLog.AppendTransaction); err != nil {
//					return nil, err
//				}
//			case *raftcmdpb.CreateLedgerLogCommand_RevertTransaction:
//				if err := processTx(createLedgerLog.RevertTransaction.RevertTransaction); err != nil {
//					return nil, err
//				}
//			default:
//				// Nothing to do actually
//			}
//		default:
//			// Nothing to do actually
//		}
//	}
//
//	ret, err := b.applier.Apply(ctx, actions...)
//	if err != nil {
//		return nil, err
//	}
//
//	for cachedBalances, count := range usedBalances {
//		cachedBalances.Lock()
//		cachedBalances.references -= count
//		cachedBalances.Unlock()
//	}
//
//	return ret, nil
//}
//
//func (b *Cache) cleanUnusedEntries() {
//
//}

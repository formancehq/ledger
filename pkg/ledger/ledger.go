package ledger

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

var DefaultContracts = []core.Contract{
	{
		Expr: &core.ExprGte{
			Op1: core.VariableExpr{
				Name: "balance",
			},
			Op2: core.ConstantExpr{
				Value: float64(0),
			},
		},
		Account: "*", // world still an exception
	},
}

type Ledger struct {
	locker Locker
	// TODO: We could remove this field since it is present in store
	name    string
	store   storage.Store
	monitor Monitor
}

func NewLedger(name string, store storage.Store, locker Locker, monitor Monitor) (*Ledger, error) {
	return &Ledger{
		store:   store,
		name:    name,
		locker:  locker,
		monitor: monitor,
	}, nil
}

func (l *Ledger) Close(ctx context.Context) error {
	if err := l.store.Close(ctx); err != nil {
		return errors.Wrap(err, "closing store")
	}
	return nil
}

type CommitResult struct {
	PreCommitVolumes      core.AggregatedVolumes
	PostCommitVolumes     core.AggregatedVolumes
	GeneratedTransactions []core.Transaction
	GeneratedLogs         []core.Log
}

func (l *Ledger) processTx(ctx context.Context, ts []core.TransactionData) (*CommitResult, error) {
	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "loading mapping")
	}

	lastLog, err := l.store.LastLog(ctx)
	if err != nil {
		return nil, err
	}

	var nextTxId uint64
	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return nil, err
	}
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}

	volumeAggregator := newVolumeAggregator(l.store)

	generatedTxs := make([]core.Transaction, 0)
	accounts := make(map[string]core.Account, 0)
	generatedLogs := make([]core.Log, 0)
	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	for i, t := range ts {
		if len(t.Postings) == 0 {
			return nil, NewTransactionCommitError(i, NewValidationError("transaction has no postings"))
		}

		txVolumeAggregator := volumeAggregator.nextTx()

		for _, p := range t.Postings {
			if p.Amount < 0 {
				return nil, NewTransactionCommitError(i, NewValidationError("negative amount"))
			}
			if !core.ValidateAddress(p.Source) {
				return nil, NewTransactionCommitError(i, NewValidationError("invalid source address"))
			}
			if !core.ValidateAddress(p.Destination) {
				return nil, NewTransactionCommitError(i, NewValidationError("invalid destination address"))
			}
			if !core.AssetIsValid(p.Asset) {
				return nil, NewTransactionCommitError(i, NewValidationError("invalid asset"))
			}
			err := txVolumeAggregator.transfer(ctx, p.Source, p.Destination, p.Asset, uint64(p.Amount))
			if err != nil {
				return nil, NewTransactionCommitError(i, err)
			}
		}

		for addr, volumes := range txVolumeAggregator.postCommitVolumes() {
			for asset, volume := range volumes {
				if addr == "world" {
					continue
				}

				expectedBalance := volume.Balance()
				for _, contract := range contracts {
					if contract.Match(addr) {
						account, ok := accounts[addr]
						if !ok {
							account, err = l.store.GetAccount(ctx, addr)
							if err != nil {
								return nil, err
							}
							accounts[addr] = account
						}

						if ok = contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": float64(expectedBalance),
							},
							Metadata: account.Metadata,
							Asset:    asset,
						}); !ok {
							return nil, NewTransactionCommitError(i, NewInsufficientFundError(asset))
						}
						break
					}
				}
			}
		}

		tx := core.Transaction{
			TransactionData:   t,
			ID:                nextTxId,
			Timestamp:         time.Now().UTC().Format(time.RFC3339),
			PostCommitVolumes: txVolumeAggregator.postCommitVolumes(),
			PreCommitVolumes:  txVolumeAggregator.preCommitVolumes(),
		}
		generatedTxs = append(generatedTxs, tx)
		newLog := core.NewTransactionLog(lastLog, tx)
		lastLog = &newLog
		generatedLogs = append(generatedLogs, newLog)
		nextTxId++
	}

	return &CommitResult{
		PreCommitVolumes:      volumeAggregator.aggregatedPreCommitVolumes(),
		PostCommitVolumes:     volumeAggregator.aggregatedPostCommitVolumes(),
		GeneratedTransactions: generatedTxs,
		GeneratedLogs:         generatedLogs,
	}, nil
}

func (l *Ledger) Commit(ctx context.Context, ts []core.TransactionData) (core.AggregatedVolumes, []core.Transaction, error) {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, nil, NewLockError(err)
	}
	defer unlock(ctx)

	result, err := l.processTx(ctx, ts)
	if err != nil {
		return nil, nil, err
	}

	if err = l.store.AppendLog(ctx, result.GeneratedLogs...); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return nil, nil, NewConflictError()
		default:
			return nil, nil, err
		}
	}

	l.monitor.CommittedTransactions(ctx, l.name, result)

	return result.PostCommitVolumes, result.GeneratedTransactions, nil
}

func (l *Ledger) CommitPreview(ctx context.Context, ts []core.TransactionData) (core.AggregatedVolumes, []core.Transaction, error) {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, nil, NewLockError(err)
	}
	defer unlock(ctx)

	result, err := l.processTx(ctx, ts)
	return result.PostCommitVolumes, result.GeneratedTransactions, err
}

func (l *Ledger) GetTransactions(ctx context.Context, m ...storage.TxQueryModifier) (sharedapi.Cursor[core.Transaction], error) {
	q := storage.NewTransactionsQuery(m)
	return l.store.GetTransactions(ctx, q)
}

func (l *Ledger) CountTransactions(ctx context.Context, m ...storage.TxQueryModifier) (uint64, error) {
	q := storage.NewTransactionsQuery(m)
	return l.store.CountTransactions(ctx, q)
}

func (l *Ledger) GetTransaction(ctx context.Context, id uint64) (core.Transaction, error) {
	return l.store.GetTransaction(ctx, id)
}

func (l *Ledger) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	err := l.store.SaveMapping(ctx, mapping)
	if err != nil {
		return err
	}
	l.monitor.UpdatedMapping(ctx, l.name, mapping)
	return nil
}

func (l *Ledger) LoadMapping(ctx context.Context) (*core.Mapping, error) {
	return l.store.LoadMapping(ctx)
}

func (l *Ledger) RevertTransaction(ctx context.Context, id uint64) (*core.Transaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}

	rt := tx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkReverts(tx.ID)

	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, NewLockError(err)
	}
	defer unlock(ctx)

	result, err := l.processTx(ctx, []core.TransactionData{rt})
	if err != nil {
		return nil, err
	}

	logs := result.GeneratedLogs
	logs = append(logs, core.NewSetMetadataLog(&logs[len(logs)-1], core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   id,
		Metadata:   core.RevertedMetadata(result.GeneratedTransactions[0].ID),
	}))

	err = l.store.AppendLog(ctx, logs...)
	if err != nil {
		return nil, err
	}
	l.monitor.RevertedTransaction(ctx, l.name, tx, result.GeneratedTransactions[0])
	return &result.GeneratedTransactions[0], nil
}

func (l *Ledger) CountAccounts(ctx context.Context, m ...storage.AccQueryModifier) (uint64, error) {
	q := storage.NewAccountsQuery(m)
	return l.store.CountAccounts(ctx, q)
}

func (l *Ledger) GetAccounts(ctx context.Context, m ...storage.AccQueryModifier) (sharedapi.Cursor[core.Account], error) {
	q := storage.NewAccountsQuery(m)
	return l.store.GetAccounts(ctx, q)
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (core.Account, error) {
	account, err := l.store.GetAccount(ctx, address)
	if err != nil {
		return core.Account{}, err
	}

	volumes, err := l.store.GetAccountVolumes(ctx, address)
	if err != nil {
		return account, err
	}

	account.Volumes = volumes
	account.Balances = volumes.Balances()

	return account, nil
}

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m core.Metadata) error {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return NewLockError(err)
	}
	defer unlock(ctx)

	if targetType == "" {
		return NewValidationError("empty target type")
	}
	if targetType != core.MetaTargetTypeTransaction && targetType != core.MetaTargetTypeAccount {
		return NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
	}
	if targetID == "" {
		return NewValidationError("empty target id")
	}

	lastLog, err := l.store.LastLog(ctx)
	if err != nil {
		return err
	}

	log := core.NewSetMetadataLog(lastLog, core.SetMetadata{
		TargetType: strings.ToUpper(targetType),
		TargetID:   targetID,
		Metadata:   m,
	})

	err = l.store.AppendLog(ctx, log)
	if err != nil {
		return err
	}

	l.monitor.SavedMetadata(ctx, l.name, targetType, fmt.Sprint(targetID), m)

	return nil
}

package chain

import (
	"context"
	"math/big"
	"sync"

	ledger "github.com/formancehq/ledger/internal"
	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"
)

type Chain struct {
	mu       sync.Mutex
	lastLog  *ledger.ChainedLog
	lastTXID *big.Int
	store    Store
}

func (chain *Chain) ChainLog(log *ledger.Log) *ledger.ChainedLog {
	chain.mu.Lock()
	defer chain.mu.Unlock()

	chain.lastLog = log.ChainLog(chain.lastLog)
	return chain.lastLog
}

func (chain *Chain) Init(ctx context.Context) error {
	lastTx, err := chain.store.GetLastTransaction(ctx)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		return err
	}
	if lastTx != nil {
		chain.lastTXID = lastTx.ID
	}

	chain.lastLog, err = chain.store.GetLastLog(ctx)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		return err
	}
	return nil
}

func (chain *Chain) AllocateNewTxID() *big.Int {
	chain.mu.Lock()
	defer chain.mu.Unlock()

	chain.lastTXID = chain.PredictNextTxID()

	return chain.lastTXID
}

func (chain *Chain) PredictNextTxID() *big.Int {
	return big.NewInt(0).Add(chain.lastTXID, big.NewInt(1))
}

func (chain *Chain) ReplaceLast(log *ledger.ChainedLog) {
	if log.Type == ledger.NewTransactionLogType {
		chain.lastTXID = log.Data.(ledger.NewTransactionLogPayload).Transaction.ID
	}
	chain.lastLog = log
}

func (chain *Chain) GetLastLog() *ledger.ChainedLog {
	return chain.lastLog
}

func New(store Store) *Chain {
	return &Chain{
		lastTXID: big.NewInt(-1),
		store:    store,
	}
}

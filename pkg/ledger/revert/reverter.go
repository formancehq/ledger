package revert

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

var (
	ErrAlreadyReverted = errors.New("transaction already reverted")
	ErrRevertOccurring = errors.New("revert already occurring")
)

type Store interface {
	GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error)
}

type Runner interface {
	Execute(
		ctx context.Context,
		script core.RunScript,
		dryRun bool,
		logComputer runner.LogComputer,
	) (*core.Transaction, *core.LogHolder, error)
}

type LogIngester interface {
	QueueLog(ctx context.Context, log *core.LogHolder, async bool) error
}

type Reverter struct {
	sync.Map
	store       Store
	runner      Runner
	logIngester LogIngester
}

func (r *Reverter) RevertTransaction(ctx context.Context, id uint64, async bool) (*core.Transaction, error) {
	_, loaded := r.Map.LoadOrStore(id, struct{}{})
	if loaded {
		return nil, ErrRevertOccurring
	}
	defer func() {
		r.Map.Delete(id)
	}()

	transactionToRevert, err := r.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if err != nil && !storage.IsNotFoundError(err) {
		return nil, errors.Wrap(err, "get transaction before revert")
	}

	if storage.IsNotFoundError(err) {
		return nil, errorsutil.NewError(err, errors.Errorf("transaction %d not found", id))
	}

	if transactionToRevert.IsReverted() {
		return nil, errorsutil.NewError(ErrAlreadyReverted, errors.Errorf("transaction %d already reverted", id))
	}

	rt := transactionToRevert.Reverse()
	rt.Metadata = core.MarkReverts(metadata.Metadata{}, transactionToRevert.ID)

	scriptData := core.TxsToScriptsData(core.TransactionData{
		Postings:  rt.Postings,
		Reference: rt.Reference,
		Metadata:  rt.Metadata,
	})
	newTx, logHolder, err := r.runner.Execute(ctx, scriptData[0], false, func(tx core.Transaction, accountMetadata map[string]metadata.Metadata) core.Log {
		return core.NewRevertedTransactionLog(tx.Timestamp, transactionToRevert.ID, tx)
	})
	if err != nil {
		return nil, errors.Wrap(err, "revert transaction")
	}

	err = r.logIngester.QueueLog(ctx, logHolder, async)
	if err != nil {
		return nil, err
	}
	return newTx, nil
}

func NewReverter(store Store, runner Runner, logIngester LogIngester) *Reverter {
	return &Reverter{
		store:       store,
		runner:      runner,
		logIngester: logIngester,
	}
}

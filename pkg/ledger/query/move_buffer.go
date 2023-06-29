package query

import (
	"context"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/utils/job"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
)

type moveBufferInput struct {
	move     *core.Move
	callback func()
}

type moveBufferAccount struct {
	account string
	moves   []*moveBufferInput
}

type insertMovesJob struct {
	buf      *moveBuffer
	moves    []*moveBufferInput
	accounts []*moveBufferAccount
}

func (j insertMovesJob) String() string {
	return fmt.Sprintf("inserting %d moves", len(j.moves))
}

func (j insertMovesJob) Terminated() {
	for _, input := range j.moves {
		input.callback()
	}

	j.buf.mu.Lock()
	defer j.buf.mu.Unlock()
	for _, account := range j.accounts {
		if len(account.moves) == 0 {
			delete(j.buf.accounts, account.account)
		} else {
			j.buf.accountsQueue.Append(account)
		}
	}
}

type moveBuffer struct {
	*job.Runner[insertMovesJob]
	accountsQueue *collectionutils.LinkedList[*moveBufferAccount]
	accounts      map[string]*moveBufferAccount
	inputMoves    chan *moveBufferInput
	mu            sync.Mutex
	maxBufferSize int
}

func (r *moveBuffer) AppendMove(move *core.Move, callback func()) {
	r.mu.Lock()
	mba, ok := r.accounts[move.Account]
	if !ok {
		mba = &moveBufferAccount{
			account: move.Account,
		}
		r.accounts[move.Account] = mba
		r.accountsQueue.Append(mba)
	}
	mba.moves = append(mba.moves, &moveBufferInput{
		move:     move,
		callback: callback,
	})
	r.mu.Unlock()

	r.Runner.Next()
}

func (r *moveBuffer) nextJob() *insertMovesJob {
	r.mu.Lock()
	defer r.mu.Unlock()

	batch := make([]*moveBufferInput, 0)
	accounts := make([]*moveBufferAccount, 0)
	for {
		mba := r.accountsQueue.TakeFirst()
		if mba == nil {
			break
		}
		accounts = append(accounts, mba)

		if len(batch)+len(mba.moves) >= r.maxBufferSize {
			nbItems := r.maxBufferSize - len(batch)
			batch = append(batch, mba.moves[:nbItems]...)
			mba.moves = mba.moves[nbItems:]
			break
		} else {
			batch = append(batch, mba.moves...)
			mba.moves = make([]*moveBufferInput, 0)
		}
	}

	if len(batch) == 0 {
		return nil
	}

	return &insertMovesJob{
		accounts: accounts,
		moves:    batch,
		buf:      r,
	}
}

func newMoveBuffer(runner func(context.Context, ...*core.Move) error, nbWorkers, maxBufferSize int) *moveBuffer {
	ret := &moveBuffer{
		accountsQueue: collectionutils.NewLinkedList[*moveBufferAccount](),
		accounts:      map[string]*moveBufferAccount{},
		inputMoves:    make(chan *moveBufferInput),
		maxBufferSize: maxBufferSize,
	}
	ret.Runner = job.NewJobRunner[insertMovesJob](func(ctx context.Context, job *insertMovesJob) error {
		return runner(ctx, collectionutils.Map(job.moves, func(from *moveBufferInput) *core.Move {
			return from.move
		})...)
	}, ret.nextJob, nbWorkers)
	return ret
}

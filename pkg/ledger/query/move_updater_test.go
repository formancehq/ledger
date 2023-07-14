package query

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

func TestMoveBuffer(t *testing.T) {
	t.Parallel()

	locked := sync.Map{}
	buf := newMoveUpdater(func(ctx context.Context, moves ...*core.Move) error {
		accounts := make(map[string]struct{})
		for _, move := range moves {
			accounts[move.Account] = struct{}{}
		}
		for account := range accounts {
			_, loaded := locked.LoadOrStore(account, struct{}{})
			if loaded {
				panic(fmt.Sprintf("account '%s' already used", account))
			}
		}
		<-time.After(10 * time.Millisecond)
		for account := range accounts {
			locked.Delete(account)
		}

		return nil
	}, 5, 100)
	go buf.Run(logging.ContextWithLogger(context.Background(), logging.Testing()))
	defer buf.Close()

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			j := j
			go func() {
				buf.AppendMove(&core.Move{
					Account: fmt.Sprintf("accounts:%d", j%10),
				}, func() {
					wg.Done()
				})
			}()
		}
	}
	wg.Wait()
	<-time.After(time.Second)
}

package command

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	locker := NewDefaultLocker()
	var accounts []string
	for i := 0; i < 10; i++ {
		accounts = append(accounts, fmt.Sprintf("accounts:%d", i))
	}

	r := rand.New(rand.NewSource(time.Now().Unix()))
	ctx := logging.TestingContext()

	const nbLoop = 1000
	wg := sync.WaitGroup{}
	wg.Add(nbLoop)

	for i := 0; i < nbLoop; i++ {
		read := accounts[r.Int31n(10)]
		write := accounts[r.Int31n(10)]
		go func() {
			unlock, err := locker.Lock(ctx, Accounts{
				Read:  []string{read},
				Write: []string{write},
			})
			require.NoError(t, err)
			defer unlock(ctx)

			<-time.After(10 * time.Millisecond)
			wg.Add(-1)
		}()
	}

	wg.Wait()

}

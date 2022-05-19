package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	randCpt := 0
	randomString = func() (string, error) {
		defer func() {
			randCpt++
		}()
		return fmt.Sprintf("%d", randCpt), nil
	}
	clusterClient, clusterMock := redismock.NewClientMock()
	duration := 5 * time.Second
	l := NewLock(clusterClient, duration, 100*time.Millisecond)

	ctx := context.Background()
	clusterMock.ExpectSetNX(lockKey("quickstart"), "0", duration).SetVal(true)
	ok, unlock, err := l.tryLock(ctx, "quickstart")
	assert.True(t, ok)
	assert.NoError(t, err)

	clusterMock.ExpectSetNX(lockKey("quickstart"), "1", duration).SetVal(false)
	ok, _, err = l.tryLock(ctx, "quickstart")
	assert.False(t, ok)
	assert.NoError(t, err)

	clusterMock.ExpectSetNX(lockKey("another"), "2", duration).SetVal(true)
	ok, _, err = l.tryLock(ctx, "another")
	assert.True(t, ok)
	assert.NoError(t, err)

	clusterMock.ExpectGet(lockKey("quickstart")).SetVal("0")
	clusterMock.ExpectDel(lockKey("quickstart")).SetVal(0)

	unlock(ctx)

	clusterMock.ExpectSetNX(lockKey("quickstart"), "3", duration).SetVal(true)
	ok, _, err = l.tryLock(ctx, "quickstart")
	assert.True(t, ok)
	assert.NoError(t, err)

}

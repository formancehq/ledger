package redis

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/formancehq/go-libs/sharedlogging"
	"github.com/go-redis/redis/v8"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/pkg/errors"
)

type BoolCmd = redis.BoolCmd
type StringCmd = redis.StringCmd
type IntCmd = redis.IntCmd

type Client interface {
	SetNX(ctx context.Context, lk string, rv interface{}, duration time.Duration) *BoolCmd
	Get(ctx context.Context, lk string) *StringCmd
	Del(ctx context.Context, lk ...string) *IntCmd
}

func lockKey(name string) string {
	return "ledger-lock-" + name
}

var randomString = func() (string, error) {
	data := make([]byte, 20)
	_, err := rand.Read(data)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

type Lock struct {
	redisClient  Client
	lockDuration time.Duration
	retry        time.Duration
}

func (l Lock) tryLock(ctx context.Context, name string) (bool, middlewares.Unlock, error) {
	rv, err := randomString()
	if err != nil {
		return false, nil, errors.Wrap(err, "generating random string")
	}
	lk := lockKey(name)
	cmd := l.redisClient.SetNX(ctx, lk, rv, l.lockDuration)
	ok, err := cmd.Result()
	if err != nil {
		return false, nil, errors.Wrap(err, "setting lock redis side")
	}
	if !ok {
		return false, nil, nil
	}

	logger := sharedlogging.GetLogger(ctx)

	return true, func(ctx context.Context) {
		getCmd := l.redisClient.Get(ctx, lk)
		if getCmd.Err() != nil {
			logger.Error(ctx, "error retrieving lock: %s", getCmd.Err())
			return
		}
		value := getCmd.Val()
		if value != rv {
			logger.Error(ctx, "unable to retrieve lock value, expect %s, got %s", rv, value)
			return
		}
		delCmd := l.redisClient.Del(ctx, lk)
		if delCmd.Err() != nil {
			logger.Error(ctx, "error deleting lock: %s", delCmd.Err())
			return
		}
	}, nil
}

func (l Lock) Lock(ctx context.Context, name string) (middlewares.Unlock, error) {
	for {
		ok, unlock, err := l.tryLock(ctx, name)
		if err != nil {
			return nil, errors.Wrap(err, "setting lock redis side")
		}
		if ok {
			return unlock, nil
		}
		select {
		case <-time.After(l.retry):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	}
}

var _ middlewares.Locker = &Lock{}

func NewLock(client Client, lockDuration, retry time.Duration) *Lock {
	return &Lock{
		redisClient:  client,
		lockDuration: lockDuration,
		retry:        retry,
	}
}

package redis

import (
	"github.com/go-redis/redis/v8"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
	"time"
)

const (
	DefaultLockDuration  = time.Minute
	DefaultRetryInterval = time.Second
)

type Options = redis.Options

type Config struct {
	Url          string
	LockDuration time.Duration
	LockRetry    time.Duration
}

func Module(cfg Config) fx.Option {
	if cfg.LockRetry == 0 {
		cfg.LockRetry = DefaultRetryInterval
	}
	if cfg.LockDuration == 0 {
		cfg.LockDuration = DefaultLockDuration
	}
	return fx.Options(
		fx.Provide(func() (Client, error) {
			options, err := redis.ParseURL(cfg.Url)
			if err != nil {
				return nil, err
			}
			return redis.NewClient(options), nil
		}),
		ledger.ProvideResolverOption(func(redisClient Client) ledger.ResolverOption {
			return ledger.WithLocker(NewLock(
				redisClient,
				cfg.LockDuration,
				cfg.LockRetry,
			))
		}),
	)
}

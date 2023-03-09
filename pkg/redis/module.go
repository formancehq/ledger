package redis

import (
	"crypto/tls"
	"time"

	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/go-redis/redis/v8"
	"go.uber.org/fx"
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
	TLSConfig    *tls.Config
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
			options.TLSConfig = cfg.TLSConfig
			return redis.NewClient(options), nil
		}),
		fx.Decorate(func(redisClient Client) ledger.Locker {
			return NewLock(
				redisClient,
				cfg.LockDuration,
				cfg.LockRetry,
			)
		}),
	)
}

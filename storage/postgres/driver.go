package postgres

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/numary/ledger/storage"
	"github.com/spf13/viper"
	"log"
	"sync"
)

type Driver struct {
	once sync.Once
	pool *pgxpool.Pool
}

func (d *Driver) Initialize() error {
	errCh := make(chan error, 1)
	d.once.Do(func() {
		log.Println("initiating postgres pool")

		pool, err := pgxpool.Connect(
			context.Background(),
			viper.GetString("storage.postgres.conn_string"),
		)
		if err != nil {
			errCh <- err
		}
		d.pool = pool
		errCh <- nil
	})
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (d *Driver) NewStore(name string) (storage.Store, error) {
	return NewStore(name, d.pool)
}

func (d *Driver) Close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	return nil
}

func init() {
	storage.RegisterDriver("postgres", &Driver{})
}

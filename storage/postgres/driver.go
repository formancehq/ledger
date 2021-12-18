package postgres

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/numary/ledger/storage"
	"log"
	"sync"
)

type Driver struct {
	once       sync.Once
	pool       *pgxpool.Pool
	connString string
}

func (d *Driver) Initialize(ctx context.Context) error {
	errCh := make(chan error, 1)
	d.once.Do(func() {
		log.Println("initiating postgres pool")

		pool, err := pgxpool.Connect(ctx, d.connString)
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

func (d *Driver) Close(ctx context.Context) error {
	if d.pool != nil {
		d.pool.Close()
	}
	return nil
}

func NewDriver(connString string) *Driver {
	return &Driver{
		connString: connString,
	}
}

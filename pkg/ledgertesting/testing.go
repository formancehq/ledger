package ledgertesting

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	"os"
	"time"
)

type PGServer struct {
	url   string
	close func() error
}

func (s *PGServer) ConnString() string {
	return s.url
}

func (s *PGServer) Close() error {
	if s.close == nil {
		return nil
	}
	return s.close()
}

func PostgresServer() (*PGServer, error) {

	externalConnectionString := os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING")
	if externalConnectionString != "" {
		return &PGServer{
			url: externalConnectionString,
			close: func() error {
				return nil
			},
		}, nil
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	resource, err := pool.Run("postgres", "13-alpine", []string{
		"POSTGRES_USER=root",
		"POSTGRES_PASSWORD=root",
		"POSTGRES_DB=ledger",
	})
	if err != nil {
		return nil, err
	}

	connString := "postgresql://root:root@localhost:" + resource.GetPort("5432/tcp") + "/ledger"
	try := time.Duration(0)
	delay := 200 * time.Millisecond
	for try*delay < 5*time.Second {
		conn, err := pgx.Connect(context.Background(), connString)
		if err != nil {
			try++
			<-time.After(delay)
			continue
		}
		_ = conn.Close(context.Background())
		break
	}

	return &PGServer{
		url: "postgresql://root:root@localhost:" + resource.GetPort("5432/tcp") + "/ledger",
		close: func() error {
			return pool.Purge(resource)
		},
	}, nil

}

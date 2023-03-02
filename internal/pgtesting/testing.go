package pgtesting

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
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

const MaxConnections = 3

func PostgresServer() (*PGServer, error) {

	externalConnectionString := os.Getenv("STORAGE_POSTGRES_CONN_STRING")
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

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "13.4-alpine",
		Env: []string{
			"POSTGRES_USER=root",
			"POSTGRES_PASSWORD=root",
			"POSTGRES_DB=ledger",
		},
		Entrypoint: nil,
		Cmd:        []string{"-c", fmt.Sprintf("max_connections=%d", MaxConnections), "-c", "superuser-reserved-connections=0"},
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

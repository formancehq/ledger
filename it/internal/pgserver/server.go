package pgserver

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
)

const (
	DefaultDatabase = "ledger"
)

type PGServer struct {
	url   string
	close func() error
}

func (s *PGServer) ConnString(name string) string {
	return fmt.Sprintf("%s/%s", s.url, name)
}

func (s *PGServer) Close() error {
	if s.close == nil {
		return nil
	}
	return s.close()
}

func PostgresServer() (*PGServer, error) {

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
			"POSTGRES_DB=" + DefaultDatabase,
		},
	})
	if err != nil {
		return nil, err
	}

	url := "postgresql://root:root@localhost:" + resource.GetPort("5432/tcp")

	try := time.Duration(0)
	delay := 200 * time.Millisecond
	for try*delay < 5*time.Second {
		conn, err := pgx.Connect(context.Background(), fmt.Sprintf("%s/%s", url, DefaultDatabase))
		if err != nil {
			try++
			<-time.After(delay)
			continue
		}
		_ = conn.Close(context.Background())
		break
	}

	return &PGServer{
		url: url,
		close: func() error {
			return pool.Purge(resource)
		},
	}, nil
}

func CreateDatabase(name string) string {
	conn, err := pgx.Connect(context.Background(), ConnString(DefaultDatabase))
	Expect(err).WithOffset(1).To(BeNil())
	defer func() {
		Expect(conn.Close(context.Background())).To(BeNil())
	}()

	_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE \"%s\"", name))
	Expect(err).WithOffset(1).To(BeNil())

	return ConnString(name)
}

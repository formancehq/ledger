package pgserver

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
)

var (
	pgServer *PGServer
)

func StartServer() string {
	var err error
	pgServer, err = newPostgresServer()
	Expect(err).WithOffset(1).To(BeNil())
	return pgServer.url
}

func StopServer() {
	pgServer.Close()
}

const (
	DefaultDatabase = "ledger"
)

type PGServer struct {
	url   string
	close func() error
}

func (s *PGServer) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close()
}

func newPostgresServer() (*PGServer, error) {

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

package pgserver

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	. "github.com/onsi/gomega"
)

var (
	pgUrl string
)

func SetUrl(url string) {
	pgUrl = url
}

func ConnString(name string) string {
	return fmt.Sprintf("%s/%s", pgUrl, name)
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

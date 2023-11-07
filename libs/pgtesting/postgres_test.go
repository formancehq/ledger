package pgtesting

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if err := CreatePostgresServer(); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	if err := DestroyPostgresServer(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestPostgres(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {
			t.Parallel()
			database := NewPostgresDatabase(t)
			conn, err := pgx.Connect(context.Background(), database.ConnString())
			require.NoError(t, err)
			require.NoError(t, conn.Close(context.Background()))
		})
	}
}

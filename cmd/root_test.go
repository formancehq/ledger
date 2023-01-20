package cmd

/*
import (
	"bytes"
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/numary/ledger/internal/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {

	pgServer, err := pgtesting.PostgresServer()
	assert.NoError(t, err)
	defer func(pgServer *pgtesting.PGServer) {
		if err := pgServer.Close(); err != nil {
			panic(err)
		}
	}(pgServer)

	type env struct {
		key   string
		value string
	}

	type testCase struct {
		name string
		args []string
		env  []env
	}

	for _, tc := range []testCase{
		{
			name: "default",
			env: []env{
				{
					key:   "NUMARY_STORAGE_DRIVER",
					value: "sqlite",
				},
			},
		},
		{
			name: "pg",
			args: []string{"--storage.driver", "postgres", "--storage.postgres.conn_string", pgServer.ConnString()},
		},
		{
			name: "pg-with-env-var",
			env: []env{
				{
					key:   "NUMARY_STORAGE_DRIVER",
					value: "postgres",
				},
				{
					key:   "NUMARY_STORAGE_POSTGRES_CONN_STRING",
					value: pgServer.ConnString(),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, e := range tc.env {
				oldValue := os.Getenv(e.key)
				if err := os.Setenv(e.key, e.value); err != nil {
					panic(err)
				}
				defer func(key, value string) {
					if err := os.Setenv(key, value); err != nil {
						panic(err)
					}
				}(e.key, oldValue)
			}
			args := []string{"server", "start", "--debug"}
			args = append(args, tc.args...)
			root := NewRootCommand()
			root.SetArgs(args)
			root.SetOut(os.Stdout)
			root.SetIn(os.Stdin)
			root.SetErr(os.Stdout)

			terminated := make(chan struct{})

			defer func() {
				<-terminated
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				assert.NoError(t, root.ExecuteContext(ctx))
				close(terminated)
			}()

			counter := time.Duration(0)
			timeout := 5 * time.Second
			delay := 200 * time.Millisecond
			for {
				rsp, err := http.DefaultClient.Get("http://localhost:3068/_info")
				if err != nil || rsp.StatusCode != http.StatusOK {
					if counter*delay < timeout {
						counter++
						<-time.After(delay)
						continue
					}
					if assert.FailNow(t, err.Error()) {
						return
					}
				}
				break
			}

			res, err := http.DefaultClient.Post("http://localhost:3068/"+uuid.New()+"/transactions", "application/json", bytes.NewBufferString(`{
				"postings": [{
					"source": "world",
					"destination": "central_bank",
					"asset": "USD",
					"amount": 100
				}]
			}`))
			if !assert.NoError(t, err) {
				return
			}
			if !assert.Equal(t, http.StatusOK, res.StatusCode) {
				return
			}
		})
	}

}
*/

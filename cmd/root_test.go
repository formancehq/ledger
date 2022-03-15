package cmd

import (
	"bytes"
	"context"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestServer(t *testing.T) {

	if testing.Verbose() {
		logrus.SetLevel(logrus.DebugLevel)
	}

	pgServer, err := pgtesting.PostgresServer()
	assert.NoError(t, err)
	defer pgServer.Close()

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
				os.Setenv(e.key, e.value)
				defer os.Setenv(e.key, oldValue)
			}
			args := []string{"server", "start", "--persist-config=false"}
			args = append(args, tc.args...)
			root := NewRootCommand()
			root.SetArgs(args)
			root.SetOut(os.Stdout)
			root.SetIn(os.Stdin)
			root.SetErr(os.Stdout)

			terminated := make(chan struct{})

			defer func() {
				select {
				case <-terminated:
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				err := root.ExecuteContext(ctx)
				assert.NoError(t, err)
				close(terminated)
			}()

			counter := time.Duration(0)
			timeout := 5 * time.Second
			delay := 200 * time.Millisecond
			for {
				_, err := http.DefaultClient.Get("http://localhost:3068/_info")
				if err != nil {
					if counter*delay < timeout {
						counter++
						<-time.After(delay)
						continue
					}
					assert.FailNow(t, err.Error())
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
			assert.NoError(t, err)
			assert.Equal(t, http.StatusOK, res.StatusCode)
		})
	}

}

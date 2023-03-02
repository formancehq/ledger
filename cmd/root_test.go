package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {

	db := pgtesting.NewPostgresDatabase(t)

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
					key:   "STORAGE_DRIVER",
					value: "sqlite",
				},
			},
		},
		{
			name: "pg",
			args: []string{"--storage.driver", "postgres", "--storage.postgres.conn_string", db.ConnString()},
		},
		{
			name: "pg-with-env-var",
			env: []env{
				{
					key:   "STORAGE_DRIVER",
					value: "postgres",
				},
				{
					key:   "STORAGE_POSTGRES_CONN_STRING",
					value: db.ConnString(),
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
			defer func() {
				cancel()
			}()

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
					if err != nil {
						require.Fail(t, err.Error())
					} else {
						require.Fail(t, fmt.Sprintf("unexpected status code: %d", rsp.StatusCode))
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

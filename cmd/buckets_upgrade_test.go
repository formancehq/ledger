//go:build it

package cmd

import (
	"io"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/stretchr/testify/require"
)

func TestBucketsUpgrade(t *testing.T) {
	t.Parallel()

	dockerPool := docker.NewPool(t, logging.Testing())
	srv := pgtesting.CreatePostgresServer(t, dockerPool)
	ctx := logging.TestingContext()

	type testCase struct {
		name string
		args []string
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			args: []string{"test"},
		},
		{
			name: "upgrade all",
			args: []string{"*"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := srv.NewDatabase(t)

			args := []string{
				"--" + bunconnect.PostgresURIFlag, db.ConnString(),
			}
			args = append(args, tc.args...)

			cmd := NewBucketUpgrade()
			cmd.SetOut(io.Discard)
			cmd.SetArgs(args)
			require.NoError(t, cmd.ExecuteContext(ctx))
		})
	}
}

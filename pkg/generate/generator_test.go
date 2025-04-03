//go:build it

package generate

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/docker"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestGenerator(t *testing.T) {

	dockerPool := docker.NewPool(t, logging.Testing())
	pgServer := pgtesting.CreatePostgresServer(t, dockerPool)
	ctx := logging.TestingContext()

	testServer := NewTestServer(testservice.Configuration[ServeConfiguration]{
		CommonConfiguration: testservice.CommonConfiguration{
			Debug: os.Getenv("DEBUG") == "true",
		},
		Configuration: ServeConfiguration{
			PostgresConfiguration: PostgresConfiguration(bunconnect.ConnectionOptions{
				DatabaseSourceName: pgServer.GetDSN(),
			}),
		},
	}, testservice.WithLogger(t))
	require.NoError(t, testServer.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, testServer.Stop(ctx))
	})

	_, err := Client(testServer).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: "default",
	})
	require.NoError(t, err)

	generator, err := NewGenerator(script, WithGlobals(map[string]interface{}{
		"globalMetadata": "test",
	}))
	require.NoError(t, err)

	const ledgerName = "default"

	for i := 0; i < 4; i++ {
		action, err := generator.Next(i)
		require.NoError(t, err)

		_, err = action.Apply(ctx, Client(testServer).Ledger.V2, ledgerName)
		require.NoError(t, err)
	}

	txs, err := ListTransactions(ctx, testServer, operations.V2ListTransactionsRequest{
		Ledger: ledgerName,
	})
	require.NoError(t, err)
	require.Len(t, txs.Data, 2)
	require.True(t, txs.Data[1].Reverted)
	require.False(t, txs.Data[0].Reverted)
	require.Equal(t, map[string]string{
		"foo":            "bar",
		"globalMetadata": "test",
	}, txs.Data[1].Metadata)
}

const script = `
function nextElement(iteration) {
	switch (iteration % 4) {
	case 0:
		return {
			action: 'CREATE_TRANSACTION',
			data: {
				script: {
					vars: {
						dst: "bank"
					},
					plain: ` + "`" + `
vars {
	account $dst
}

send [USD/2 100] (
	source = @world
	destination = $dst
)

set_tx_meta("globalMetadata", "${globalMetadata}")
` + "`" + `
				}
			}
		}
	case 1:
		return {
			action: 'ADD_METADATA',
			data: {
				targetID: 1,
				targetType: 'TRANSACTION',
				metadata: {
					"foo": "bar",
					"foo2": "bar2"
				}
			}
		}
	case 2:
		return {
			action: 'DELETE_METADATA',
			data: {
				targetID: 1,
				targetType: 'TRANSACTION',
				key: "foo2"
			}
		}
	case 3:
		return {
			action: 'REVERT_TRANSACTION',
			data: {
				id: 1
			}
		}
	}
}

function next(iteration) {
	return [nextElement(iteration)]
}
`

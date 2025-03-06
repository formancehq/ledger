//go:build it

package elasticsearch

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/elastictesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestElasticSearchConnector(t *testing.T) {
	t.Parallel()

	dockerPool := docker.NewPool(t, logging.Testing())
	srv := elastictesting.CreateServer(dockerPool, elastictesting.WithTimeout(2*time.Minute))

	ctx := context.TODO()
	esConfig := Config{
		Endpoint: srv.Endpoint(),
	}
	esConfig.SetDefaults()
	connector, err := NewConnector(esConfig, logging.Testing())
	require.NoError(t, err)
	require.NoError(t, connector.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, connector.Stop(ctx))
	})

	const (
		numberOfEvents = 50
		ledgerName     = "testing"
	)

	wg := sync.WaitGroup{}
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log := ledger.NewLog(ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			})
			log.ID = pointer.For(uint64(i))
			itemsErrors, err := connector.Accept(ctx, drivers.NewLogWithLedger(ledgerName, log))
			require.NoError(t, err)
			require.Len(t, itemsErrors, 1)
			require.Nil(t, itemsErrors[0])
		}()
	}
	wg.Wait()

	// Ensure all documents has been inserted
	require.Eventually(t, func() bool {
		rsp, err := connector.Client().Search(DefaultIndex).Do(ctx)
		require.NoError(t, err)

		return int64(numberOfEvents) == rsp.Hits.TotalHits.Value
	}, 2*time.Second, 50*time.Millisecond)
}

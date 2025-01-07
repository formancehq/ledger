//go:build it

package elasticsearch

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/docker"
	"github.com/formancehq/go-libs/v2/testing/platform/elastictesting"
	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestElasticSearchConnector(t *testing.T) {
	t.Parallel()

	dockerPool := docker.NewPool(t, logging.Testing())
	srv := elastictesting.CreateServer(dockerPool)

	ctx := context.TODO()
	stack := uuid.NewString()
	esConfig := Config{
		Endpoint: srv.Endpoint(),
	}
	esConfig.SetDefaults()
	connector, err := NewConnector(drivers.NewServiceConfig(stack, testing.Verbose()), esConfig, logging.Testing())
	require.NoError(t, err)
	require.NoError(t, connector.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, connector.Stop(ctx))
	})

	const (
		numberOfEvents = 50
		moduleName     = "testing"
	)

	wg := sync.WaitGroup{}
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			itemsErrors, err := connector.Accept(ctx, ingester.NewLogWithLedger(moduleName, ingester.Log{
				Shard:   "test",
				ID:      fmt.Sprint(i),
				Date:    time.Now(),
				Type:    "test",
				Payload: []byte(fmt.Sprintf(`{"id": "%s"}`, uuid.NewString())),
			}))
			require.NoError(t, err)
			require.Len(t, itemsErrors, 1)
			require.Nil(t, itemsErrors[0])
		}(i)
	}
	wg.Wait()

	// Ensure all documents has been inserted
	require.Eventually(t, func() bool {
		rsp, err := connector.Client().Search(DefaultIndex).Do(ctx)
		require.NoError(t, err)

		return int64(numberOfEvents) == rsp.Hits.TotalHits.Value
	}, 2*time.Second, 50*time.Millisecond)
}

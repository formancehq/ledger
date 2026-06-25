// parallel_driver_compaction triggers Pebble primary and read-index secondary
// compaction while other drivers are actively writing. This exercises the
// compaction code paths under concurrent load and fault injection.
//
// After each compaction, the driver verifies that the ledger is still readable
// and consistent (stats check, account reads).
package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_compaction", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		details := internal.Details{"ledger": ledger}

		conn, err := internal.NewGRPCConn()
		if err != nil {
			log.Printf("compaction: cannot create gRPC conn: %s", err)
			return
		}
		defer conn.Close()

		clusterClient := clusterpb.NewClusterServiceClient(conn)

		r := internal.Rand()

		// Pick primary or secondary compaction randomly.
		if r.Intn(2) == 0 {
			compactPrimary(ctx, clusterClient, client, ledger, details)
		} else {
			compactSecondary(ctx, clusterClient, client, ledger, details)
		}
	})
}

func compactPrimary(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, client servicepb.BucketServiceClient, ledger string, details internal.Details) {
	log.Printf("compaction: triggering primary compaction")

	resp, err := clusterClient.CompactPrimary(ctx, &clusterpb.CompactPrimaryRequest{})

	assert.Sometimes(err == nil || internal.IsTransient(err), "primary compaction should succeed or be transiently unavailable", details.With(internal.Details{"error": err}))

	if err != nil {
		log.Printf("compaction: primary compaction failed: %s", err)
		return
	}

	assert.Reachable("primary compaction completed", details.With(internal.Details{
		"durationMs": resp.GetDurationMs(),
	}))

	log.Printf("compaction: primary compaction done in %dms", resp.GetDurationMs())

	// Verify the ledger is still readable after compaction.
	verifyReadable(ctx, client, ledger, details)
}

func compactSecondary(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, client servicepb.BucketServiceClient, ledger string, details internal.Details) {
	log.Printf("compaction: triggering secondary (read-index) compaction")

	resp, err := clusterClient.CompactSecondary(ctx, &clusterpb.CompactSecondaryRequest{})

	assert.Sometimes(err == nil || internal.IsTransient(err), "secondary compaction should succeed or be transiently unavailable", details.With(internal.Details{"error": err}))

	if err != nil {
		log.Printf("compaction: secondary compaction failed: %s", err)
		return
	}

	assert.Reachable("secondary compaction completed", details.With(internal.Details{
		"durationMs":      resp.GetDurationMs(),
		"sizeBeforeBytes": resp.GetSizeBeforeBytes(),
		"sizeAfterBytes":  resp.GetSizeAfterBytes(),
	}))

	log.Printf("compaction: secondary compaction done in %dms (before=%d, after=%d)",
		resp.GetDurationMs(), resp.GetSizeBeforeBytes(), resp.GetSizeAfterBytes())

	// Note: we intentionally do NOT assert sizeAfter <= sizeBefore.
	// Pebble compaction can temporarily increase file size due to SSTable
	// format overhead on small stores or concurrent writes during compaction.

	// Verify the ledger is still readable after compaction.
	verifyReadable(ctx, client, ledger, details)
}

func verifyReadable(ctx context.Context, client servicepb.BucketServiceClient, ledger string, details internal.Details) {
	// Stats should work.
	stats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: ledger})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		assert.Unreachable("GetLedgerStats should not fail after compaction", details.With(internal.Details{"error": err}))

		return
	}

	assert.AlwaysOrUnreachable(stats.GetTransactionCount() >= 0, "stats must be valid after compaction", details)

	// Read a well-known account.
	_, err = client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: "world",
	})

	assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
		"world account must be readable after compaction",
		details.With(internal.Details{"error": err}),
	)
}

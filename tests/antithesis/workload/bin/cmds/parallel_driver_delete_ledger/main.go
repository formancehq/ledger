package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_delete_ledger")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	ledgerName := fmt.Sprintf("ephemeral-%d", internal.Rand().Uint64()%1e6)
	details := internal.Details{"ledger": ledgerName}

	// Create a dedicated ledger.
	err = internal.CreateLedger(ctx, client, ledgerName)
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create ephemeral ledger", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// Create a transaction in it so it's not empty.
	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledgerName,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
							},
							Force: true,
						},
					}},
				},
			},
		}},
	})
	if err != nil {
		return
	}

	// Delete the ledger.
	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_DeleteLedger{
				DeleteLedger: &servicepb.DeleteLedgerRequest{
					Name: ledgerName,
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to delete ledger", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// Verify the ledger no longer appears in ListLedgers.
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return
	}

	found := false

	for {
		info, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}

		if info.GetName() == ledgerName {
			found = true

			break
		}
	}

	assert.Always(!found, "deleted ledger should not appear in ListLedgers", details)

	log.Printf("composer: parallel_driver_delete_ledger: done")
}

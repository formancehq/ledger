package main

import (
	"errors"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/protohelpers"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_delete_ledger")

	ctx, cancel := internal.DriverContext()
	defer cancel()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)

		return
	}
	defer func() { _ = conn.Close() }()

	ledgerName := internal.PrefixEphemeral.New()
	details := internal.Details{"ledger": ledgerName}

	// Create a dedicated ledger. CreateLedger already emits the canonical
	// "should be able to create ledger" Sometimes assertion with the proper
	// IsTransient classification, so we just check the error here.
	if err := internal.CreateLedger(ctx, client, ledgerName); err != nil {
		return
	}

	// Create a transaction in it so it's not empty.
	_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
						Postings: []*ledgerpb.Posting{
							protohelpers.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
						},
						Force: true,
					},
				}},
			},
		},
	}))
	assert.Sometimes(internal.IsTolerated(err) || internal.IsLedgerDeleted(err),
		"should be able to seed ephemeral ledger before delete", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// Delete the ledger.
	_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
		Type: &ledgerpb.Request_DeleteLedger{
			DeleteLedger: &ledgerpb.DeleteLedgerRequest{
				Name: ledgerName,
			},
		},
	}))

	assert.Sometimes(internal.IsTolerated(err), "should be able to delete ledger", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// Verify the ledger no longer appears in ListLedgers.
	stream, err := client.ListLedgers(ctx, &ledgerpb.ListLedgersRequest{})
	if err != nil {
		internal.LogCleanupError("list ledgers after delete", err)

		return
	}

	found := false

	for {
		info, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			internal.LogCleanupError("list ledgers stream after delete", err)

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

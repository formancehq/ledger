package main

import (
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_event_sinks")

	ctx, cancel := internal.DriverContext()
	defer cancel()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)

		return
	}
	defer func() { _ = conn.Close() }()

	r := internal.Rand()
	sinkName := fmt.Sprintf("nats-sink-%d", r.Uint64())
	topic := fmt.Sprintf("ledger.events.%d", r.Uint64()%10)

	details := internal.Details{"sinkName": sinkName, "topic": topic}

	// 1. Add a NATS event sink.
	_, err = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
		Type: &commonpb.Request_AddEventsSink{
			AddEventsSink: &commonpb.AddEventsSinkRequest{
				Config: &commonpb.SinkConfig{
					Name: sinkName,
					Type: &commonpb.SinkConfig_Nats{
						Nats: &commonpb.NatsSinkConfig{
							Url:   "nats://nats:4222",
							Topic: topic,
						},
					},
					Format:    "json",
					BatchSize: 32,
				},
			},
		},
	}))

	assert.Sometimes(internal.IsTolerated(err),
		"should be able to add NATS event sink", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// 2. Verify the sink appears in GetEventsSinks.
	sinksResp, err := client.GetEventsSinks(ctx, &commonpb.GetEventsSinksRequest{})
	if err != nil {
		internal.LogCleanupError("get events sinks after add", err)

		return
	}

	found := false

	for _, sink := range sinksResp.GetSinks() {
		if sink.GetName() == sinkName {
			found = true

			break
		}
	}

	assert.AlwaysOrUnreachable(found, "added sink should appear in GetEventsSinks", details)

	// 3. Create a transaction to generate events for the sink.
	ledger, err := internal.GetRandomLedger(ctx, client)
	if err != nil {
		return
	}

	_, _ = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Postings: internal.RandomPostings(),
						Force:    true,
					},
				}},
			},
		},
	}))
	// Transaction creation is best-effort here; the sink test is what matters.

	// 4. Remove the event sink.
	_, err = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
		Type: &commonpb.Request_RemoveEventsSink{
			RemoveEventsSink: &commonpb.RemoveEventsSinkRequest{
				Name: sinkName,
			},
		},
	}))

	assert.Sometimes(internal.IsTolerated(err),
		"should be able to remove event sink", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// 5. Verify the sink is gone.
	sinksResp, err = client.GetEventsSinks(ctx, &commonpb.GetEventsSinksRequest{})
	if err != nil {
		internal.LogCleanupError("get events sinks after remove", err)

		return
	}

	foundAfter := false

	for _, sink := range sinksResp.GetSinks() {
		if sink.GetName() == sinkName {
			foundAfter = true

			break
		}
	}

	assert.AlwaysOrUnreachable(!foundAfter, "removed sink should not appear in GetEventsSinks", details)

	log.Printf("Event sink lifecycle complete: %s", sinkName)
}

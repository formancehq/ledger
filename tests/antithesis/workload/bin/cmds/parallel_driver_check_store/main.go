package main

import (
	"context"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_check_store")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		if internal.IsUnavailable(err) {
			log.Printf("CheckStore unavailable: %s", err)
			return
		}

		assert.Unreachable("CheckStore returned unexpected error", internal.Details{"error": err})

		return
	}

	var logsChecked uint64

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if internal.IsUnavailable(err) {
				return
			}

			assert.Unreachable("CheckStore stream error", internal.Details{"error": err})

			return
		}

		switch e := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Error:
			assert.Always(false, "CheckStore found integrity error", internal.Details{
				"errorType":     e.Error.GetErrorType().String(),
				"message":       e.Error.GetMessage(),
				"logSequence":   e.Error.GetLogSequence(),
				"ledger":        e.Error.GetLedger(),
				"account":       e.Error.GetAccount(),
				"asset":         e.Error.GetAsset(),
				"transactionId": e.Error.GetTransactionId(),
			})
		case *servicepb.CheckStoreEvent_Progress:
			logsChecked = e.Progress.GetLogsChecked()
		}
	}

	assert.Reachable("CheckStore completed successfully", internal.Details{
		"logsChecked": logsChecked,
	})

	log.Printf("CheckStore passed: %d logs verified", logsChecked)
}

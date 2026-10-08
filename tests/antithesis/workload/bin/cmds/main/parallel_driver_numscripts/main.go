package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_numscripts", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		scriptName := fmt.Sprintf("transfer-%d", r.Uint64())
		version := fmt.Sprintf("%d.0.0", r.Uint64()%10+1)

		details := internal.Details{"ledger": ledger, "scriptName": scriptName, "version": version}

		_, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_SaveNumscript{
				SaveNumscript: &ledgerpb.SaveNumscriptRequest{
					Name:    scriptName,
					Content: transferScript,
					Version: version,
					Ledger:  ledger,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to save numscript", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		info, err := client.GetNumscript(ctx, &ledgerpb.GetNumscriptRequest{
			Name:   scriptName,
			Ledger: ledger,
		})
		if err != nil {
			internal.LogCleanupError("get numscript after save", err)

			return
		}

		assert.AlwaysOrUnreachable(info.GetName() == scriptName, "saved numscript should be readable", details)

		vars := map[string]string{
			"from":   internal.GetRandomAddress(),
			"to":     internal.GetRandomAddress(),
			"amount": fmt.Sprintf("COIN %v", internal.RandomBigInt().String()),
		}

		resp, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							ScriptReference: &ledgerpb.ScriptReference{
								Name:    scriptName,
								Version: "latest",
								Vars:    vars,
							},
							Force: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to use saved numscript in transaction", details.With(internal.Details{"error": err}))
		if err == nil {
			internal.CheckCreatedTransaction(resp, details)
		}
	})
}

const transferScript = `
	vars {
		account $from
		account $to
		monetary $amount
	}
	send $amount (
		source = $from allowing unbounded overdraft
		destination = $to
	)
`

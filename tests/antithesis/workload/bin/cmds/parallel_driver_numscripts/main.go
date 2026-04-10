package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_numscripts", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		scriptName := fmt.Sprintf("transfer-%d", r.Uint64()%50)
		version := fmt.Sprintf("%d.0.0", r.Uint64()%10+1)

		details := internal.Details{"ledger": ledger, "scriptName": scriptName, "version": version}

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SaveNumscript{
					SaveNumscript: &servicepb.SaveNumscriptRequest{
						Name:    scriptName,
						Content: transferScript,
						Version: version,
						Ledger:  ledger,
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to save numscript", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		info, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
			Name:   scriptName,
			Ledger: ledger,
		})
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(info.GetName() == scriptName, "saved numscript should be readable", details)

		vars := map[string]string{
			"from":   internal.GetRandomAddress(),
			"to":     internal.GetRandomAddress(),
			"amount": fmt.Sprintf("COIN %v", internal.RandomBigInt().String()),
		}

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								ScriptReference: &servicepb.ScriptReference{
									Name: scriptName,
									Vars: vars,
								},
								Force: true,
							},
						},
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to use saved numscript in transaction", details.With(internal.Details{"error": err}))
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

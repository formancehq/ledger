package main

import (
	"context"
	"fmt"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_numscript_lifecycle", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		scriptName := fmt.Sprintf("lifecycle-%d", r.Uint64())
		version := "1.0.0"

		details := internal.Details{"ledger": ledger, "scriptName": scriptName}

		// 1. Save a numscript.
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

		// 2. Verify it appears in ListNumscripts.
		stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{
			Ledger: ledger,
		})
		if err != nil {
			return
		}

		var (
			found     bool
			streamErr bool
		)

		for {
			info, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				streamErr = true

				break
			}

			if info.GetName() == scriptName {
				found = true
			}
		}

		if !streamErr {
			assert.AlwaysOrUnreachable(found, "saved numscript should appear in ListNumscripts", details)
		}

		// 3. Save a new version.
		version2 := "2.0.0"
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SaveNumscript{
					SaveNumscript: &servicepb.SaveNumscriptRequest{
						Name:    scriptName,
						Content: transferScript,
						Version: version2,
						Ledger:  ledger,
					},
				},
			}},
		})
		if err != nil {
			return
		}

		// 4. GetNumscript should return the latest version.
		nsInfo, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
			Name:   scriptName,
			Ledger: ledger,
		})
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(nsInfo.GetVersion() == version2,
			"GetNumscript should return latest version",
			details.With(internal.Details{"expected": version2, "actual": nsInfo.GetVersion()}))

		// 5. Delete the numscript.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_DeleteNumscript{
					DeleteNumscript: &servicepb.DeleteNumscriptRequest{
						Name:   scriptName,
						Ledger: ledger,
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to delete numscript", details.With(internal.Details{"error": err}))
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

package main

import (
	"context"
	"fmt"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_numscript_lifecycle", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		scriptName := fmt.Sprintf("lifecycle-%d", r.Uint64())
		version := "1.0.0"

		details := internal.Details{"ledger": ledger, "scriptName": scriptName}

		// 1. Save a numscript.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SaveNumscript{
				SaveNumscript: &servicepb.SaveNumscriptRequest{
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

		// 2. Verify it appears in ListNumscripts.
		stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{
			Ledger: ledger,
		})
		if err != nil {
			internal.LogCleanupError("list numscripts after save", err)
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
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SaveNumscript{
				SaveNumscript: &servicepb.SaveNumscriptRequest{
					Name:    scriptName,
					Content: transferScript,
					Version: version2,
					Ledger:  ledger,
				},
			},
		}))
		assert.Sometimes(internal.IsTolerated(err),
			"should be able to save second numscript version", details.With(internal.Details{"version": version2, "error": err}))
		if err != nil {
			return
		}

		// 4. GetNumscript should return the latest version.
		nsInfo, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
			Name:   scriptName,
			Ledger: ledger,
		})
		if err != nil {
			internal.LogCleanupError("get numscript after second version", err)
			return
		}

		assert.AlwaysOrUnreachable(nsInfo.GetVersion() == version2,
			"GetNumscript should return latest version",
			details.With(internal.Details{"expected": version2, "actual": nsInfo.GetVersion()}))

		// 5. ListNumscriptVersions should report the latest pointer at the
		//    greatest saved semver (the library is immutable append-only).
		versions, err := client.ListNumscriptVersions(ctx, &servicepb.ListNumscriptVersionsRequest{
			Name:   scriptName,
			Ledger: ledger,
		})
		if err != nil {
			internal.LogCleanupError("list numscript versions", err)
			return
		}

		assert.AlwaysOrUnreachable(versions.GetLatestVersion() == version2,
			"latest pointer should be the greatest saved semver",
			details.With(internal.Details{"expected": version2, "actual": versions.GetLatestVersion()}))
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

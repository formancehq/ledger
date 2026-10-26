package main

import (
	"context"
	"fmt"
	"math"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgergrpc "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	internal.RunGeneratedDriver("parallel_driver_numscript_lifecycle", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
		first, later := numscriptVersions(src)
		runNumscriptLifecycle(ctx, client, ledger, first, later)
	})
}

func numscriptVersions(src *generation.Source) (string, string) {
	// Retain the original major-version regression, alongside generated pairs.
	if src.Intn(2) == 0 {
		return "1.0.0", "2.0.0"
	}
	// Leave room for an increment even at the uint32 boundary. Derive the
	// later version from the first; independent draws cannot guarantee order.
	first := semver.Version{
		Major: uint32(src.Uint64Range(0, math.MaxUint32-1)),
		Minor: uint32(src.Uint64Range(0, math.MaxUint32-1)),
		Patch: uint32(src.Uint64Range(0, math.MaxUint32-1)),
	}
	later := first
	switch src.Intn(3) {
	case 0:
		later.Major++
		later.Minor, later.Patch = 0, 0
	case 1:
		later.Minor++
		later.Patch = 0
	case 2:
		later.Patch++
	}

	return first.String(), later.String()
}

func runNumscriptLifecycle(ctx context.Context, client servicepb.BucketServiceClient, ledger, version, version2 string) {
	// SDK entropy isolates technical identities independently of business draws.
	r := internal.Rand()
	scriptName := fmt.Sprintf("lifecycle-%d", r.Uint64())

	details := internal.Details{"ledger": ledger, "scriptName": scriptName, "firstVersion": version, "laterVersion": version2}

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
	found, err := numscriptIsListed(ctx, client, ledger, scriptName)
	if err != nil {
		internal.LogCleanupError("list numscripts after save", err)
		assert.AlwaysOrUnreachable(internal.IsTolerated(err), "ListNumscripts should not return unexpected error",
			details.With(internal.Details{"error": err}))
	} else {
		assert.AlwaysOrUnreachable(found, "saved numscript should appear in ListNumscripts", details)
	}

	// 3. Save a new version.
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
}

// numscriptIsListed only returns a definitive result after every page succeeds.
// The routed client follows x-next-cursor using the server's default page size.
func numscriptIsListed(ctx context.Context, client servicepb.BucketServiceClient, ledger, name string) (bool, error) {
	scripts, err := ledgergrpc.NewLedgerGrpcClient(client).ListNumscripts(ctx, ledger)
	if err != nil {
		return false, err
	}
	for _, script := range scripts {
		if script.GetName() == name {
			return true, nil
		}
	}

	return false, nil
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

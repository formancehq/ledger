package internal

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// restrictedPrefixes lists ledger name prefixes created by specialized drivers
// that set account type restrictions or have specific balance assumptions.
// Generic drivers (via RunDriver) must not pick these ledgers. The "sentinel-"
// prefix is reserved for operational drivers (scaling_structured,
// rolling_restart, config_change, quorum_recovery) that commit a witness
// transaction and re-read it after a disruption — letting delete_ledger pick
// these would weaken the survival assertion to Reachable.
var restrictedPrefixes = []string{
	"transient-", "insuf-", "deltest-", "sentinel-",
	// parallel_driver_delete_ledger creates then deletes these; a generic driver
	// that picked one would race the deletion, and a read on a deleted ledger
	// returns plain NotFound (not LedgerDeleted), which generic drivers don't
	// tolerate — a false finding.
	"ephemeral-",
	// Wave-1 property drivers: each owns its ledgers and asserts balance/
	// completeness/ordering invariants that a foreign write would break.
	"refrace-",  // parallel_driver_reference_race
	"bulkatom-", // parallel_driver_bulk_atomicity
	"deferr-",   // parallel_driver_definitive_errors
	"lrec-",     // parallel_driver_ledger_recreate (covers lrecreate- too)
	"listcomp-", // parallel_driver_list_completeness
	"tsorder-",  // parallel_driver_timestamp_order
	"minseq-",   // parallel_driver_minlogseq
	"stale-",    // parallel_driver_stale_reads
	// singleton_driver_model assumes it is the only writer on its ledgers; a
	// foreign write would surface as a model divergence, not a ledger bug.
	"model-",
}

// CreateLedger creates a ledger via the Apply RPC and verifies it can be read back.
func CreateLedger(ctx context.Context, client servicepb.BucketServiceClient, name string) error {
	details := Details{"ledger": name}

	// A fresh idempotency key, reused across the client's internal retries: a
	// create whose commit response was lost (e.g. UNAVAILABLE) replays through the
	// server's idempotency cache and returns the committed success, rather than
	// re-running and surfacing a spurious AlreadyExists.
	key := fmt.Sprintf("create-ledger-%016x%016x", Rand().Uint64(), Rand().Uint64())
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(key, &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: name},
		},
	}))
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to create ledger", details.With(Details{"error": err}))
	if err != nil {
		return err
	}

	// Verify it's readable
	_, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	assert.Sometimes(err == nil || IsUnavailable(err), "should always be able to get created ledger", details.With(Details{"error": err}))
	return nil
}

// ListLedgers returns the names of all ledgers.
func ListLedgers(ctx context.Context, client servicepb.BucketServiceClient) ([]string, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}
	var names []string
	for {
		ledger, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, ledger.Name)
	}
	return names, nil
}

// GetRandomLedger returns a random unrestricted ledger name. Ledgers created
// by specialized drivers (transient-, insuf-, deltest-) are filtered out to
// prevent cross-driver interference (e.g. account type violations).
func GetRandomLedger(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	ledgers, err := ListLedgers(ctx, client)
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to get a random ledger", Details{"error": err})
	if err != nil {
		return "", err
	}

	filtered := ledgers[:0]
	for _, name := range ledgers {
		restricted := false
		for _, prefix := range restrictedPrefixes {
			if strings.HasPrefix(name, prefix) {
				restricted = true
				break
			}
		}
		if !restricted {
			filtered = append(filtered, name)
		}
	}

	if len(filtered) == 0 {
		return "", io.EOF
	}

	return filtered[Rand().Uint64()%uint64(len(filtered))], nil
}

package internal

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// OwnedLedgerPrefix tags a ledger-name token reserved for a single driver
// (or a tight family of drivers — see PrefixSentinel). Generic drivers go
// through GetRandomLedger and MUST NOT pick a ledger whose name starts
// with `<prefix>-`; the restriction is enforced by RestrictedPrefixes().
//
// Why typed: until run-#4 of the antithesis cleanup PR, this list was
// `[]string` and the parallel_driver_ledger_recreate entry was the
// truncated `"lrec-"` instead of `"lrecreate-"`. The hyphen-aware prefix
// match (`strings.HasPrefix("lrecreate-…", "lrec-")` → false, since char
// 5 is 'r', not '-') silently let every recreated ledger leak into the
// random pool and `typed_metadata` hit a deleted ledger mid-test.
//
// With OwnedLedgerPrefix, the constant is the token without the trailing
// hyphen, the hyphen is appended exactly once by the type's methods, and
// TestOwnedLedgerPrefixes_NoOverlap (in ledger_test.go) catches any new
// constant that is a prefix of another.
type OwnedLedgerPrefix string

// Driver-owned prefixes. Each constant pairs with a driver (or family) in
// `bin/cmds/`. New entries: add the constant here AND list it in
// `ownedLedgerPrefixes` below so the random-ledger pool filter picks it up.
const (
	// Wave-0 specialized drivers: each creates dedicated ledgers and asserts
	// account-type / balance invariants a foreign write would break.
	PrefixTransientAccounts OwnedLedgerPrefix = "transient"
	PrefixInsufficientFunds OwnedLedgerPrefix = "insuf"
	PrefixDeltest           OwnedLedgerPrefix = "deltest"   // parallel_driver_concurrent_ledger_delete
	PrefixMaintenance       OwnedLedgerPrefix = "maint"     // parallel_driver_maintenance
	PrefixAccountTypes      OwnedLedgerPrefix = "accttype"  // parallel_driver_account_types
	PrefixTypeViolation     OwnedLedgerPrefix = "typeviolation"
	PrefixEphemeral         OwnedLedgerPrefix = "ephemeral" // parallel_driver_delete_ledger

	// Wave-1 property drivers: each owns its ledgers and asserts
	// balance/completeness/ordering invariants that a foreign write would
	// break.
	PrefixReferenceRace    OwnedLedgerPrefix = "refrace"
	PrefixBulkAtomicity    OwnedLedgerPrefix = "bulkatom"
	PrefixDefinitiveErrors OwnedLedgerPrefix = "deferr"
	PrefixLedgerRecreate   OwnedLedgerPrefix = "lrecreate"
	PrefixListCompleteness OwnedLedgerPrefix = "listcomp"
	PrefixTimestampOrder   OwnedLedgerPrefix = "tsorder"
	PrefixMinLogSeq        OwnedLedgerPrefix = "minseq"
	PrefixStaleReads       OwnedLedgerPrefix = "stale"

	// PrefixSentinel covers the witness-ledger family used by the
	// operational singletons (scaling_structured, rolling_restart,
	// config_change, quorum_recovery). Each singleton picks its own full
	// name ("sentinel-scaling-structured", …) via PrefixSentinel.WithSuffix.
	PrefixSentinel OwnedLedgerPrefix = "sentinel"

	// PrefixModel covers singleton_driver_model's per-run fleet names
	// (model-<hex>-<idx>). Generic drivers reading a model-owned ledger
	// would surface as a model divergence, not a ledger bug.
	PrefixModel OwnedLedgerPrefix = "model"
)

// ownedLedgerPrefixes is the registry of every driver-owned prefix.
// GetRandomLedger filters its return value against RestrictedPrefixes()
// (derived from this slice) so generic drivers never see a driver-owned
// ledger in their random pool. New driver-owned prefixes MUST be appended
// here; TestOwnedLedgerPrefixes_NoOverlap pins the no-shared-prefix
// invariant so a typo like "lrec" (which does not match "lrecreate-N")
// is caught at test time, not in a chaos run.
var ownedLedgerPrefixes = []OwnedLedgerPrefix{
	PrefixTransientAccounts,
	PrefixInsufficientFunds,
	PrefixDeltest,
	PrefixMaintenance,
	PrefixAccountTypes,
	PrefixTypeViolation,
	PrefixEphemeral,
	PrefixReferenceRace,
	PrefixBulkAtomicity,
	PrefixDefinitiveErrors,
	PrefixLedgerRecreate,
	PrefixListCompleteness,
	PrefixTimestampOrder,
	PrefixMinLogSeq,
	PrefixStaleReads,
	PrefixSentinel,
	PrefixModel,
}

// RestrictedPrefixes returns the hyphen-terminated string forms of the
// owned ledger prefixes. GetRandomLedger uses these to filter the pool.
// Exposed for tests; production callers should use GetRandomLedger
// directly.
func RestrictedPrefixes() []string {
	out := make([]string, len(ownedLedgerPrefixes))
	for i, p := range ownedLedgerPrefixes {
		out[i] = string(p) + "-"
	}

	return out
}

// New returns a fresh ledger name with this prefix and a random 6-digit
// suffix. Use this when the driver only needs one ledger per run.
func (p OwnedLedgerPrefix) New() string {
	return p.WithSeed(Rand().Uint64())
}

// WithSeed returns a ledger name with this prefix and the given seed
// reduced to 6 digits. Use this when the driver derives multiple names
// (ledger + helper ledger + account references) from the same seed so
// they all share the same numeric tail.
func (p OwnedLedgerPrefix) WithSeed(seed uint64) string {
	return fmt.Sprintf("%s-%d", p, seed%1_000_000)
}

// WithSuffix returns a ledger name composed of this prefix and an
// explicit hyphen-separated suffix. Use this for fixed names like
// "sentinel-rolling-restart" or "model-<hex>-<idx>" that need to be
// stable across runs.
func (p OwnedLedgerPrefix) WithSuffix(suffix string) string {
	return string(p) + "-" + suffix
}

// CreateLedger creates a ledger via the Apply RPC and verifies it can be read back.
func CreateLedger(ctx context.Context, client servicepb.BucketServiceClient, name string, initialSchema ...*commonpb.SetMetadataFieldTypeCommand) error {
	details := Details{"ledger": name}

	// A fresh idempotency key, reused across the client's internal retries: a
	// create whose commit response was lost (e.g. UNAVAILABLE) replays through the
	// server's idempotency cache and returns the committed success, rather than
	// re-running and surfacing a spurious AlreadyExists.
	key := fmt.Sprintf("create-ledger-%016x%016x", Rand().Uint64(), Rand().Uint64())
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(key, &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: name, InitialSchema: initialSchema},
		},
	}))
	assert.Sometimes(IsTolerated(err), "should be able to create ledger", details.With(Details{"error": err}))
	if err != nil {
		return err
	}

	// Verify it's readable
	_, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	assert.Sometimes(IsTolerated(err), "should always be able to get created ledger", details.With(Details{"error": err}))
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

// GetRandomLedger returns a random unrestricted ledger name. Ledgers
// belonging to a driver-owned prefix (see ownedLedgerPrefixes) are
// filtered out to prevent cross-driver interference.
func GetRandomLedger(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	ledgers, err := ListLedgers(ctx, client)
	assert.Sometimes(IsTolerated(err), "should be able to get a random ledger", Details{"error": err})
	if err != nil {
		return "", err
	}

	restricted := RestrictedPrefixes()
	filtered := ledgers[:0]
	for _, name := range ledgers {
		isOwned := false
		for _, prefix := range restricted {
			if strings.HasPrefix(name, prefix) {
				isOwned = true
				break
			}
		}
		if !isOwned {
			filtered = append(filtered, name)
		}
	}

	if len(filtered) == 0 {
		return "", io.EOF
	}

	return filtered[Rand().Uint64()%uint64(len(filtered))], nil
}

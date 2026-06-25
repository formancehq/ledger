package main

import (
	"context"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/holiman/uint256"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// runRead picks a known account, issues a linearizable GetAccount, and validates
// the result — the picked asset's volumes and the account's whole metadata map —
// against the model (see validateAccountRead).
func runRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	ledger, addr, asset, ok := pickCell(c.modelState)
	if !ok {
		c.mu.Unlock()
		return
	}
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Be explicit about consistency so the test still validates the
	// property it cares about if the server-side default ever changes.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	acct, err := client.GetAccount(readCtx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: addr,
	})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned. Captured before validation so later
	// dispatches aren't folded into this read's candidate states.
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		// NotFound = no entries server-side; validate as zero volumes / no metadata.
		if status.Code(err) == codes.NotFound {
			c.validateAccountRead(maxTicket, ledger, addr, asset, uint256.Int{}, uint256.Int{}, false, nil)
			return
		}
		assert.Unreachable("singleton_driver_model: GetAccount returned unexpected error", internal.Details{
			"ledger":  ledger,
			"address": addr,
			"asset":   asset,
			"error":   err.Error(),
		})
		return
	}

	gotIn, gotOut, found := accountAssetVolumes(acct, asset)
	c.validateAccountRead(maxTicket, ledger, addr, asset, gotIn, gotOut, found, acct.GetMetadata())
}

// isShutdownError reports whether err is a context cancellation/deadline — what
// in-flight Apply/GetAccount calls return once MODEL_MAX_SECONDS expires (or the
// parent context is cancelled). It's a clean shutdown, not a server rejection to
// validate against, so callers drop the observation. (status.Code(nil) == OK.)
func isShutdownError(err error) bool {
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// pickCell returns a random readable account across all ledgers as
// (ledger, address, asset), or ok=false if there are none. Each volume cell is a
// candidate (carrying its asset); each metadata-bearing address is also a
// candidate with an empty asset, so a metadata-only account is still reachable —
// the read validates that account's metadata regardless of the asset.
func pickCell(g GlobalState) (ledger, addr, asset string, ok bool) {
	type cellRef struct {
		ledger string
		key    VolumeKey
	}

	var cells []cellRef
	for name, ls := range g.ledgers {
		for k := range ls.volumes {
			cells = append(cells, cellRef{ledger: name, key: k})
		}

		metaAddrs := map[string]bool{}
		for mk := range ls.metadata {
			metaAddrs[mk.Address] = true
		}
		for a := range metaAddrs {
			cells = append(cells, cellRef{ledger: name, key: VolumeKey{Address: a}})
		}
	}

	if len(cells) == 0 {
		return "", "", "", false
	}

	slices.SortFunc(cells, func(a, b cellRef) int {
		if a.ledger != b.ledger {
			if a.ledger < b.ledger {
				return -1
			}
			return 1
		}
		return compareVolumeKey(a.key, b.key)
	})

	c := random.RandomChoice(cells)

	return c.ledger, c.key.Address, c.key.Asset, true
}

// accountAssetVolumes extracts (input, output) for one asset from a GetAccount
// response. found=false when the asset entry is missing.
func accountAssetVolumes(acct *commonpb.Account, asset string) (in, out uint256.Int, found bool) {
	if acct == nil {
		return in, out, false
	}

	v, ok := acct.GetVolumes()[asset]
	if !ok {
		return in, out, false
	}

	if err := in.SetFromDecimal(v.GetInput()); err != nil {
		in.Clear()
	}

	if err := out.SetFromDecimal(v.GetOutput()); err != nil {
		out.Clear()
	}

	return in, out, true
}

// runLedgerRead issues a linearizable GetLedger and checks the server's whole
// ledger snapshot — account types and ledger metadata — against the model (see
// validateLedgerRead).
func runLedgerRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	info, err := client.GetLedger(readCtx, &servicepb.GetLedgerRequest{Ledger: ledger})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		// The fleet is created at setup and never deleted, so a definitive error
		// on a linearizable read — NotFound, Internal — is a real finding.
		assert.Unreachable("singleton_driver_model: GetLedger returned unexpected error", internal.Details{
			"ledger": ledger,
			"error":  err.Error(),
		})
		return
	}

	c.validateLedgerRead(maxTicket, ledger, info.GetAccountTypes(), info.GetMetadata())
}

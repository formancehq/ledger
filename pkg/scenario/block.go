package scenario

import (
	"context"
	"errors"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// ErrSkip is returned by a block's Run function when its precondition is not met.
var ErrSkip = errors.New("precondition not met")

// Random supplies typed draws for a block. Implementations may record each bounded
// decision separately. A source must remain local to the block invocation.
type Random interface {
	Intn(n int) int
	Int63n(n int64) int64
	Uint64() uint64
}

// RandFunc adapts an existing uint64 function to the typed Random interface.
type RandFunc func() uint64

// Uint64 returns the next raw value from the legacy source.
func (r RandFunc) Uint64() uint64 { return r() }

// Intn preserves the legacy one-draw modulo mapping. Typed callers implement
// Random directly, so their bounded choices do not pass through this adapter.
func (r RandFunc) Intn(n int) int {
	if n <= 0 {
		panic("non-positive bound")
	}

	return int(r() % uint64(n))
}

// Int63n preserves the legacy one-draw modulo mapping, including fixed sources.
func (r RandFunc) Int63n(n int64) int64 {
	if n <= 0 {
		panic("non-positive bound")
	}

	return int64(r() % uint64(n))
}

// RandIntN preserves the helper API while forwarding the bounded decision.
func RandIntN(r Random, n int) int { return r.Intn(n) }

// RandInt64N preserves the helper API while forwarding the bounded decision.
func RandInt64N(r Random, n int64) int64 { return r.Int63n(n) }

// Block is an atomic, self-contained business operation.
// It reads its preconditions from the ledger (no local state tracking).
// Blocks can be composed in any order (Antithesis) or run sequentially (scenario tests).
type Block struct {
	// Name uniquely identifies this block (e.g. "marketplace/deposit").
	Name string
	// Run executes the block. Returns the Apply response (for invariant checks)
	// and ErrSkip if the precondition is not met.
	Run func(ctx context.Context, client servicepb.BucketServiceClient, rand Random) (*servicepb.ApplyResponse, error)
}

// BlockGroup groups blocks that share the same setup actions.
type BlockGroup struct {
	// Setup returns the idempotent setup actions (create ledger, account types, numscripts).
	Setup  func() []*servicepb.Request
	Blocks []*Block
}

// BlockScenario extracts the scenario prefix from a block name (e.g. "marketplace" from "marketplace/deposit").
func BlockScenario(name string) string {
	for i, c := range name {
		if c == '/' {
			return name[:i]
		}
	}

	return name
}

// Helper functions for blocks to read ledger state.

// GetAccountBalance reads the uncolored balance of an account for a given asset.
// Returns (balance, true) if available, or (zero, false) otherwise. For a
// colored balance, see GetColoredAccountBalance.
func GetAccountBalance(ctx context.Context, client servicepb.BucketServiceClient, ledger, address, asset string) (*big.Int, bool) {
	return GetColoredAccountBalance(ctx, client, ledger, address, asset, "")
}

// GetColoredAccountBalance reads the balance of an account for a given
// (asset, color) bucket. Color "" is the uncolored bucket.
func GetColoredAccountBalance(ctx context.Context, client servicepb.BucketServiceClient, ledger, address, asset, color string) (*big.Int, bool) {
	acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: address,
	})
	if err != nil {
		return big.NewInt(0), false
	}
	for _, entry := range acct.GetVolumes() {
		if entry.GetAsset() != asset || entry.GetColor() != color {
			continue
		}
		vol := entry.GetVolumes()
		if vol.GetBalance() == nil {
			return big.NewInt(0), false
		}
		balance, err := vol.GetBalance().ToBigInt()
		if err != nil {
			return big.NewInt(0), false
		}

		return balance, true
	}

	return big.NewInt(0), false
}

// GetNonRevertedTransaction finds a random non-reverted transaction in a ledger.
func GetNonRevertedTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string, randFn Random) (*commonpb.Transaction, bool) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 100},
	})
	if err != nil {
		return nil, false
	}

	var candidates []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err != nil {
			break
		}
		if !tx.GetReverted() {
			candidates = append(candidates, tx)
		}
	}
	if len(candidates) == 0 {
		return nil, false
	}

	return candidates[RandIntN(randFn, len(candidates))], true
}

// ApplyActions sends a batch of actions and returns the response.
func ApplyActions(ctx context.Context, client servicepb.BucketServiceClient, reqs ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
	return client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
}

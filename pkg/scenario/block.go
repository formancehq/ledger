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

// RandFunc returns a uniformly distributed random uint64.
// It abstracts the randomness source so callers can substitute
// math/rand/v2 (default) or antithesis-sdk-go/random.GetRandom.
type RandFunc func() uint64

// Rand helpers — derive typed random values from a RandFunc.

// RandIntN returns a random int in [0, n).
func RandIntN(r RandFunc, n int) int {
	return int(r() % uint64(n))
}

// RandInt64N returns a random int64 in [0, n).
func RandInt64N(r RandFunc, n int64) int64 {
	return int64(r() % uint64(n))
}

// Block is an atomic, self-contained business operation.
// It reads its preconditions from the ledger (no local state tracking).
// Blocks can be composed in any order (Antithesis) or run sequentially (scenario tests).
type Block struct {
	// Name uniquely identifies this block (e.g. "marketplace/deposit").
	Name string
	// Run executes the block. Returns the Apply response (for invariant checks)
	// and ErrSkip if the precondition is not met.
	Run func(ctx context.Context, client servicepb.BucketServiceClient, rand RandFunc) (*servicepb.ApplyResponse, error)
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

// GetAccountBalance reads the balance of an account for a given asset.
// Returns (balance, true) if available, or (zero, false) otherwise.
func GetAccountBalance(ctx context.Context, client servicepb.BucketServiceClient, ledger, address, asset string) (*big.Int, bool) {
	acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: address,
	})
	if err != nil {
		return big.NewInt(0), false
	}
	vol, ok := acct.GetVolumes()[asset]
	if !ok {
		return big.NewInt(0), false
	}
	balance, ok := new(big.Int).SetString(vol.GetBalance(), 10)
	if !ok {
		return big.NewInt(0), false
	}

	return balance, true
}

// GetNonRevertedTransaction finds a random non-reverted transaction in a ledger.
func GetNonRevertedTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string, randFn RandFunc) (*commonpb.Transaction, bool) {
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

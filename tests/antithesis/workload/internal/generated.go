package internal

import (
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

// RunOperationalGeneration reports engine failures before callers abort a
// witness or round. A log-only return would silently remove operational coverage.
func RunOperationalGeneration(operation string, fn func(*generation.Source)) error {
	err := generation.RunOnce(fn)
	if err != nil {
		assert.Unreachable("Hegel operational generation failed", Details{"operation": operation, "error": err.Error()})
	}

	return err
}

// GeneratedBigInt retains the existing full uint64 amount domain, including
// zero and values above the signed and JSON exact-integer boundaries.
func GeneratedBigInt(src *generation.Source) *big.Int {
	return new(big.Int).SetUint64(src.Uint64())
}

// GeneratedAddress retains the shared account pool used by the workload's
// readers and invariants. Unique technical identities use a separate source.
func GeneratedAddress(src *generation.Source) string {
	if src.Intn(2) == 0 {
		return "world"
	}

	return fmt.Sprintf("users:%d", src.Intn(int(UserAccountCount)))
}

// GeneratedPostings describes every field directly to Hegel. Self-transfers,
// zero amounts and repeated accounts remain intentional generated inputs.
func GeneratedPostings(src *generation.Source) []*commonpb.Posting {
	postings := make([]*commonpb.Posting, 1+src.Intn(2))
	for i := range postings {
		postings[i] = commonpb.NewPosting(GeneratedAddress(src), GeneratedAddress(src),
			generation.Choice(src, []string{"USD/2", "EUR/2", "COIN"}), GeneratedBigInt(src))
	}

	return postings
}

func GeneratedMetadata(src *generation.Source) map[string]string {
	metadata := make(map[string]string)
	for range src.Intn(3) {
		metadata[strconv.Itoa(src.Intn(999))] = strconv.Itoa(src.Intn(999))
	}

	return metadata
}

// GeneratedBulkSize retains the truncated geometric transform and load bounds.
// Hegel biases primitive draws, so frequencies differ from uniform sampling.
// Selecting only short Hegel lists would lose the existing large-bulk stress.
func GeneratedBulkSize(src *generation.Source, p float64, low, high uint64) uint64 {
	if p <= 0 || p > 1 || low > high {
		panic("invalid geometric bulk bounds or probability")
	}
	if p == 1 {
		return low
	}
	u := 1 - src.Float64()
	size := min(uint64(math.Floor(math.Log(u)/math.Log(1-p))), high-low)

	return low + size
}

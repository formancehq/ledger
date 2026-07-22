package bulking

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/numscript"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

// A bulk element's business error must map to the same code the individual
// endpoints return, never the generic INTERNAL fallback.
func TestMapBulkElementError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"insufficient funds", &ledgercontroller.ErrInsufficientFunds{}, common.ErrInsufficientFund},
		{"numscript missing funds", numscript.MissingFundsErr{}, common.ErrInsufficientFund},
		{"invalid vars", &ledgercontroller.ErrInvalidVars{}, common.ErrCompilationFailed},
		{"compilation failed", ledgercontroller.ErrCompilationFailed{}, common.ErrCompilationFailed},
		{"metadata override", &ledgercontroller.ErrMetadataOverride{}, common.ErrMetadataOverride},
		{"no postings", ledgercontroller.ErrNoPostings, common.ErrNoPostings},
		{"reference conflict", ledgerstore.ErrTransactionReferenceConflict{}, common.ErrConflict},
		{"idempotency key conflict", ledgercontroller.ErrIdempotencyKeyConflict{}, common.ErrConflict},
		{"parsing", ledgercontroller.ErrParsing{}, common.ErrInterpreterParse},
		{"runtime", ledgercontroller.ErrRuntime{}, common.ErrInterpreterRuntime},
		{"already reverted", ledgercontroller.ErrAlreadyReverted{}, common.ErrAlreadyRevert},
		{"invalid idempotency input", ledgercontroller.ErrInvalidIdempotencyInput{}, common.ErrValidation},
		{"not found", ledgercontroller.ErrNotFound, "NOT_FOUND"},
		{"unknown", errors.New("boom"), api.ErrorInternal},
	} {
		require.Equal(t, tc.want, mapBulkElementError(tc.err), tc.name)
	}
}

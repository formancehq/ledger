package commonpb

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

func TestPostingValidateInputPresence(t *testing.T) {
	t.Parallel()
	for _, posting := range []*Posting{nil, {}, {Amount: nil}} {
		err := posting.ValidateInput()
		var fieldErr *inputvalidation.FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Path != "amount" {
			t.Fatalf("expected amount field error, got %v", err)
		}
	}
	if err := (&Posting{Amount: &Uint256{}}).ValidateInput(); err != nil {
		t.Fatalf("explicit zero must pass: %v", err)
	}
}

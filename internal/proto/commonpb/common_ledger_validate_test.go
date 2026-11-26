package commonpb

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

func TestPostingValidateInputPresence(t *testing.T) {
	t.Parallel()
	valid := &Posting{Source: "world", Destination: "users:alice", Amount: &Uint256{}, Asset: "USD"}
	for _, tt := range []struct {
		name    string
		posting *Posting
		field   string
	}{
		{"nil posting", nil, "source"},
		{"empty posting", &Posting{}, "source"},
		{"missing destination", &Posting{Source: "world"}, "destination"},
		{"missing amount", &Posting{Source: "world", Destination: "users:alice", Asset: "USD"}, "amount"},
		{"missing asset", &Posting{Source: "world", Destination: "users:alice", Amount: &Uint256{}}, "asset"},
	} {
		err := tt.posting.ValidateInput()
		var fieldErr *inputvalidation.FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Path != tt.field {
			t.Fatalf("%s: expected %s field error, got %v", tt.name, tt.field, err)
		}
	}
	if err := valid.ValidateInput(); err != nil {
		t.Fatalf("explicit zero must pass: %v", err)
	}
}

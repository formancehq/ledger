package restbulk

import (
	"fmt"

	"github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// BulkElement is the REST-only envelope around a public LedgerAction.
type BulkElement struct {
	Action           *grpc.LedgerAction
	IdempotencyKey   string
	SkippableReasons []grpc.ErrorReason
}

func (x *BulkElement) UnmarshalJSON(data []byte) error {
	var raw struct {
		IdempotencyKey   string   `json:"ik"`
		SkippableReasons []string `json:"skippableReasons"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("error parsing element: %w", err)
	}

	reasons := make([]grpc.ErrorReason, len(raw.SkippableReasons))
	for i, name := range raw.SkippableReasons {
		code, ok := grpc.ErrorReason_value["ERROR_REASON_"+name]
		if !ok || name == "" || name == "UNSPECIFIED" {
			return fmt.Errorf("invalid skippableReasons: %q", name)
		}
		reasons[i] = grpc.ErrorReason(code)
	}

	var action grpc.LedgerAction
	if err := json.Unmarshal(data, &action); err != nil {
		return err
	}
	x.Action = &action
	x.IdempotencyKey = raw.IdempotencyKey
	x.SkippableReasons = reasons

	return nil
}

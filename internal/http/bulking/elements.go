package bulking

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

const (
	ActionCreateTransaction = "CREATE_TRANSACTION"
	ActionAddMetadata       = "ADD_METADATA"
	ActionRevertTransaction = "REVERT_TRANSACTION"
	ActionDeleteMetadata    = "DELETE_METADATA"
)

type Bulk chan BulkElement

type BulkElement struct {
	Action         string `json:"action"`
	IdempotencyKey string `json:"ik"`
	Data           any    `json:"data"`
}

func (b BulkElement) GetAction() string {
	return b.Action
}

func (b *BulkElement) UnmarshalJSON(data []byte) error {
	type Aux BulkElement
	type X struct {
		Aux
		Data json.RawMessage `json:"data"`
	}
	x := X{}
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}

	*b = BulkElement(x.Aux)

	var err error
	b.Data, err = UnmarshalBulkElementPayload(x.Action, x.Data)

	return err
}

func UnmarshalBulkElementPayload(action string, data []byte) (any, error) {
	var req any
	switch action {
	case ActionCreateTransaction:
		req = &TransactionRequest{}
	case ActionAddMetadata:
		req = &AddMetadataRequest{}
	case ActionRevertTransaction:
		req = &RevertTransactionRequest{}
	case ActionDeleteMetadata:
		req = &DeleteMetadataRequest{}
	default:
		return nil, fmt.Errorf("unsupported action: %s", action)
	}
	if err := json.Unmarshal(data, req); err != nil {
		return nil, fmt.Errorf("error parsing element: %s", err)
	}

	return reflect.ValueOf(req).Elem().Interface(), nil
}

type BulkElementResult struct {
	Error     error
	Data      any    `json:"data,omitempty"`
	LogID     uint64 `json:"logID"`
	ElementID int    `json:"elementID"`
}

type AddMetadataRequest struct {
	TargetType string          `json:"targetType"`
	TargetID   json.RawMessage `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

type RevertTransactionRequest struct {
	ID              uint64            `json:"id"`
	Force           bool              `json:"force"`
	AtEffectiveDate bool              `json:"atEffectiveDate"`
	Metadata        metadata.Metadata `json:"metadata"`
}

type DeleteMetadataRequest struct {
	TargetType string          `json:"targetType"`
	TargetID   json.RawMessage `json:"targetId"`
	Key        string          `json:"key"`
}

type TransactionRequest struct {
	Postings        ledger.Postings              `json:"postings,omitempty"`
	Script          *service.TransactionScript    `json:"script,omitempty"`
	Timestamp       *time.Time                   `json:"timestamp,omitempty"`
	Reference       string                       `json:"reference,omitempty"`
	Metadata        metadata.Metadata            `json:"metadata,omitempty"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
	Runtime         string                       `json:"runtime,omitempty"`
}

func (req TransactionRequest) ToCore() (*service.CreateTransaction, error) {
	return &service.CreateTransaction{
		Postings:        req.Postings,
		Script:          req.Script,
		Timestamp:       req.Timestamp,
		Reference:       req.Reference,
		Metadata:        req.Metadata,
		AccountMetadata: req.AccountMetadata,
		Runtime:         req.Runtime,
	}, nil
}


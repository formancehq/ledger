package bulking

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"reflect"
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
	}
	if err := json.Unmarshal(data, req); err != nil {
		return nil, fmt.Errorf("error parsing element: %s", err)
	}

	return reflect.ValueOf(req).Elem().Interface(), nil
}

type BulkElementResult struct {
	Error     error
	Data      any `json:"data,omitempty"`
	LogID     int `json:"logID"`
	ElementID int `json:"elementID"`
}

type AddMetadataRequest struct {
	TargetType string            `json:"targetType"`
	TargetID   json.RawMessage   `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

type RevertTransactionRequest struct {
	ID              int  `json:"id"`
	Force           bool `json:"force"`
	AtEffectiveDate bool `json:"atEffectiveDate"`
}

type DeleteMetadataRequest struct {
	TargetType string          `json:"targetType"`
	TargetID   json.RawMessage `json:"targetId"`
	Key        string          `json:"key"`
}

type TransactionRequest struct {
	Postings  ledger.Postings           `json:"postings"`
	Script    ledgercontroller.ScriptV1 `json:"script"`
	Timestamp time.Time                 `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata" swaggertype:"object"`
}

func (req TransactionRequest) ToRunScript(allowUnboundedOverdrafts bool) (*ledgercontroller.RunScript, error) {

	if _, err := req.Postings.Validate(); err != nil {
		return nil, err
	}

	if len(req.Postings) > 0 {
		txData := ledger.TransactionData{
			Postings:  req.Postings,
			Timestamp: req.Timestamp,
			Reference: req.Reference,
			Metadata:  req.Metadata,
		}

		return pointer.For(ledgercontroller.TxToScriptData(txData, allowUnboundedOverdrafts)), nil
	}

	return &ledgercontroller.RunScript{
		Script:    req.Script.ToCore(),
		Timestamp: req.Timestamp,
		Reference: req.Reference,
		Metadata:  req.Metadata,
	}, nil
}
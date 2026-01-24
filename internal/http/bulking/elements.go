package bulking

import (
	"fmt"
	"reflect"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

const (
	ActionCreateTransaction = "CREATE_TRANSACTION"
	ActionAddMetadata       = "ADD_METADATA"
	ActionRevertTransaction = "REVERT_TRANSACTION"
	ActionDeleteMetadata    = "DELETE_METADATA"
)

type Bulk chan BulkElement

// todo: define protobuf types
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
		Data json.RawValue `json:"data"`
	}
	x := X{}
	if err := json.Unmarshal(data, &x); err != nil {
		return fmt.Errorf("error parsing element: %s", err)
	}

	*b = BulkElement(x.Aux)

	var err error
	b.Data, err = UnmarshalBulkElementPayload(x.Action, x.Data)

	return err
}

func UnmarshalBulkElementPayload(action string, data json.RawValue) (any, error) {
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
		return nil, fmt.Errorf("error parsing payload: %s", err)
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
	TargetType string            `json:"targetType"`
	TargetID   UInt64OrString    `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"` // todo: use protobuf
}

type UInt64OrString struct {
	Int   *uint64 `json:"int,omitempty"`
	Str   *string `json:"str,omitempty"`
	IsInt bool    `json:"isint"`
}

func (i UInt64OrString) MarshalJSON() ([]byte, error) {
	if i.IsInt {
		return json.Marshal(i.Int)
	}
	return json.Marshal(i.Str)
}

func (i *UInt64OrString) UnmarshalJSON(data []byte) error {
	var v uint64
	if err := json.Unmarshal(data, &v); err == nil {
		i.Int = &v
		i.IsInt = true
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		i.Str = &s
	}
	return nil
}

type RevertTransactionRequest struct {
	ID              uint64            `json:"id"`
	Force           bool              `json:"force"`
	AtEffectiveDate bool              `json:"atEffectiveDate"`
	Metadata        metadata.Metadata `json:"metadata"`
}

type DeleteMetadataRequest struct {
	TargetType string        `json:"targetType"`
	TargetID   json.RawValue `json:"targetId"`
	Key        string        `json:"key"`
}

type TransactionRequest struct {
	Postings        []*ledgerpb.Posting          `json:"postings,omitempty"`
	Script          *ledgerpb.Script             `json:"script,omitempty"`
	Timestamp       *time.Time                   `json:"timestamp,omitempty"`
	Reference       string                       `json:"reference,omitempty"`
	Metadata        metadata.Metadata            `json:"metadata,omitempty"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
}

func (req TransactionRequest) ToCore() (*ledgerpb.CreateTransactionRequestPayload, error) {
	// Convert account metadata to protobuf
	accountMetadata := make(map[string]*ledgerpb.Metadata)
	for addr, md := range req.AccountMetadata {
		if len(md) > 0 {
			accountMetadata[addr] = &ledgerpb.Metadata{Entries: md}
		}
	}

	// Convert timestamp
	var timestamp *ledgerpb.Timestamp
	if req.Timestamp != nil && !req.Timestamp.IsZero() {
		timestamp = ledgerpb.NewTimestamp(*req.Timestamp)
	}

	return &ledgerpb.CreateTransactionRequestPayload{
		Postings:        req.Postings,
		Script:          req.Script,
		Timestamp:       timestamp,
		Reference:       req.Reference,
		Metadata:        req.Metadata,
		AccountMetadata: accountMetadata,
	}, nil
}

package generate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dop251/goja"
	"github.com/formancehq/go-libs/v2/collectionutils"
	ledger "github.com/formancehq/ledger/internal"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/google/uuid"
	"math/big"
	"time"
)

type Action struct {
	v2.BulkElement
}

type Result struct {
	components.V2BulkElementResult
}

func (r Result) GetLogID() int64 {
	switch r.Type {
	case components.V2BulkElementResultTypeCreateTransaction:
		return r.V2BulkElementResultCreateTransaction.LogID
	case components.V2BulkElementResultTypeAddMetadata:
		return r.V2BulkElementResultAddMetadata.LogID
	case components.V2BulkElementResultTypeDeleteMetadata:
		return r.V2BulkElementResultDeleteMetadata.LogID
	case components.V2BulkElementResultTypeRevertTransaction:
		return r.V2BulkElementResultRevertTransaction.LogID
	default:
		panic(fmt.Sprintf("unexpected result type: %s", r.Type))
	}
}

func (r Action) Apply(ctx context.Context, client *client.V2, l string) (*Result, error) {

	var bulkElement components.V2BulkElement
	switch r.Action {
	case v2.ActionCreateTransaction:
		transactionRequest := &v2.TransactionRequest{}
		err := json.Unmarshal(r.Data, transactionRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal transaction request: %w", err)
		}

		bulkElement = components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
			Data: &components.V2PostTransaction{
				Timestamp: func() *time.Time {
					if transactionRequest.Timestamp.IsZero() {
						return nil
					}
					return &transactionRequest.Timestamp.Time
				}(),
				Script: &components.V2PostTransactionScript{
					Plain: transactionRequest.Script.Plain,
					Vars: collectionutils.ConvertMap(transactionRequest.Script.Vars, func(from any) string {
						return fmt.Sprint(from)
					}),
				},
				Postings: collectionutils.Map(transactionRequest.Postings, func(p ledger.Posting) components.V2Posting {
					return components.V2Posting{
						Amount:      p.Amount,
						Asset:       p.Asset,
						Destination: p.Destination,
						Source:      p.Source,
					}
				}),
				Reference: func() *string {
					if transactionRequest.Reference == "" {
						return nil
					}
					return &transactionRequest.Reference
				}(),
				Metadata: transactionRequest.Metadata,
			},
		})
	case v2.ActionAddMetadata:
		addMetadataRequest := &v2.AddMetadataRequest{}
		err := json.Unmarshal(r.Data, addMetadataRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal add metadata request: %w", err)
		}

		var targetID components.V2TargetID
		switch addMetadataRequest.TargetType {
		case ledger.MetaTargetTypeAccount:
			var targetIDStr string
			if err := json.Unmarshal(addMetadataRequest.TargetID, &targetIDStr); err != nil {
				return nil, fmt.Errorf("failed to unmarshal target id: %w", err)
			}
			targetID = components.CreateV2TargetIDStr(targetIDStr)
		case ledger.MetaTargetTypeTransaction:
			var targetIDInt int
			if err := json.Unmarshal(addMetadataRequest.TargetID, &targetIDInt); err != nil {
				return nil, fmt.Errorf("failed to unmarshal target id: %w", err)
			}
			targetID = components.CreateV2TargetIDBigint(big.NewInt(int64(targetIDInt)))
		default:
			panic("unexpected target id type")
		}

		bulkElement = components.CreateV2BulkElementAddMetadata(components.V2BulkElementAddMetadata{
			Data: &components.Data{
				TargetID:   targetID,
				TargetType: components.V2TargetType(addMetadataRequest.TargetType),
				Metadata:   addMetadataRequest.Metadata,
			},
		})
	case v2.ActionDeleteMetadata:
		deleteMetadataRequest := &v2.DeleteMetadataRequest{}
		err := json.Unmarshal(r.Data, deleteMetadataRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal delete metadata request: %w", err)
		}

		var targetID components.V2TargetID
		switch deleteMetadataRequest.TargetType {
		case ledger.MetaTargetTypeAccount:
			var targetIDStr string
			if err := json.Unmarshal(deleteMetadataRequest.TargetID, &targetIDStr); err != nil {
				return nil, fmt.Errorf("failed to unmarshal target id: %w", err)
			}
			targetID = components.CreateV2TargetIDStr(targetIDStr)
		case ledger.MetaTargetTypeTransaction:
			var targetIDInt int
			if err := json.Unmarshal(deleteMetadataRequest.TargetID, &targetIDInt); err != nil {
				return nil, fmt.Errorf("failed to unmarshal target id: %w", err)
			}
			targetID = components.CreateV2TargetIDBigint(big.NewInt(int64(targetIDInt)))
		default:
			panic("unexpected target id type")
		}

		bulkElement = components.CreateV2BulkElementDeleteMetadata(components.V2BulkElementDeleteMetadata{
			Data: &components.V2BulkElementDeleteMetadataData{
				TargetID:   targetID,
				TargetType: components.V2TargetType(deleteMetadataRequest.TargetType),
				Key:        deleteMetadataRequest.Key,
			},
		})
	case v2.ActionRevertTransaction:
		revertMetadataRequest := &v2.RevertTransactionRequest{}
		err := json.Unmarshal(r.Data, revertMetadataRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal delete metadata request: %w", err)
		}

		bulkElement = components.CreateV2BulkElementRevertTransaction(components.V2BulkElementRevertTransaction{
			Data: &components.V2BulkElementRevertTransactionData{
				ID:              big.NewInt(int64(revertMetadataRequest.ID)),
				Force:           &revertMetadataRequest.Force,
				AtEffectiveDate: &revertMetadataRequest.AtEffectiveDate,
			},
		})
	default:
		panic("unexpected action")
	}

	response, err := client.CreateBulk(ctx, operations.V2CreateBulkRequest{
		Ledger:      l,
		RequestBody: []components.V2BulkElement{bulkElement},
	})
	if err != nil {
		return nil, fmt.Errorf("creating transaction: %w", err)
	}

	if errorResponse := response.V2BulkResponse.Data[0].V2BulkElementResultError; errorResponse != nil {
		if errorResponse.ErrorCode != "" {
			errorDescription := errorResponse.ErrorDescription
			if errorDescription == "" {
				errorDescription = "<no description>"
			}
			return nil, fmt.Errorf("[%s] %s", errorResponse.ErrorCode, errorDescription)
		}
	}

	return &Result{response.V2BulkResponse.Data[0]}, nil
}

type Generator struct {
	next func(int) (*Action, error)
}

func (g *Generator) Next(iteration int) (*Action, error) {
	return g.next(iteration)
}

func NewGenerator(script string, opts ...Option) (*Generator, error) {

	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	runtime := goja.New()

	for k, v := range cfg.globals {
		err := runtime.Set(k, v)
		if err != nil {
			return nil, fmt.Errorf("failed to set global variable %s: %w", k, err)
		}
	}

	_, err := runtime.RunString(script)
	if err != nil {
		return nil, err
	}

	runtime.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	err = runtime.Set("uuid", uuid.NewString)
	if err != nil {
		return nil, err
	}

	var next func(int) map[string]any
	err = runtime.ExportTo(runtime.Get("next"), &next)
	if err != nil {
		panic(err)
	}

	return &Generator{
		next: func(i int) (*Action, error) {
			ret := next(i)

			var (
				action string
				ik     string
				data   map[string]any
				ok     bool
			)
			rawAction := ret["action"]
			if rawAction == nil {
				return nil, errors.New("'action' must be set")
			}

			action, ok = rawAction.(string)
			if !ok {
				return nil, errors.New("'action' must be a string")
			}

			rawData := ret["data"]
			if rawData == nil {
				return nil, errors.New("'data' must be set")
			}
			data, ok = rawData.(map[string]any)
			if !ok {
				return nil, errors.New("'data' must be a map[string]any")
			}

			dataAsJsonRawMessage, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}

			rawIK := ret["ik"]
			if rawIK != nil {
				ik, ok = rawIK.(string)
				if !ok {
					return nil, errors.New("'ik' must be a string")
				}
			}

			return &Action{
				BulkElement: v2.BulkElement{
					Action:         action,
					IdempotencyKey: ik,
					Data:           dataAsJsonRawMessage,
				},
			}, nil
		},
	}, nil
}

type config struct {
	globals map[string]any
}

type Option func(*config)

func WithGlobals(globals map[string]any) Option {
	return func(c *config) {
		c.globals = globals
	}
}

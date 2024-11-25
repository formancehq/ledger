package generate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dop251/goja"
	"github.com/formancehq/go-libs/v2/collectionutils"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/google/uuid"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

type Action struct {
	elements []bulking.BulkElement
}

func (r Action) Apply(ctx context.Context, client *client.V2, l string) ([]components.V2BulkElementResult, error) {

	bulkElements := make([]components.V2BulkElement, 0)

	for _, element := range r.elements {
		var bulkElement components.V2BulkElement

		switch element.Action {
		case bulking.ActionCreateTransaction:
			transactionRequest := element.Data.(bulking.TransactionRequest)

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
		case bulking.ActionAddMetadata:
			addMetadataRequest := element.Data.(bulking.AddMetadataRequest)

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
		case bulking.ActionDeleteMetadata:
			deleteMetadataRequest := element.Data.(bulking.DeleteMetadataRequest)

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
		case bulking.ActionRevertTransaction:
			revertMetadataRequest := element.Data.(bulking.RevertTransactionRequest)

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

		bulkElements = append(bulkElements, bulkElement)
	}

	response, err := client.CreateBulk(ctx, operations.V2CreateBulkRequest{
		Ledger:      l,
		RequestBody: bulkElements,
	})
	if err != nil {
		return nil, fmt.Errorf("creating transaction: %w", err)
	}

	return response.V2BulkResponse.Data, nil
}

type NextOptions struct {
	Globals map[string]any
}

type NextOption func(options *NextOptions)

func WithNextGlobals(globals map[string]any) NextOption {
	return func(options *NextOptions) {
		options.Globals = globals
	}
}

type Generator struct {
	next func(int, ...NextOption) (*Action, error)
}

func (g *Generator) Next(iteration int, options ...NextOption) (*Action, error) {
	return g.next(iteration, options...)
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

	err = runtime.Set("read_file", func(path string) string {
		fmt.Println("read file", path)
		f, err := os.ReadFile(filepath.Join(cfg.rootPath, path))
		if err != nil {
			panic(err)
		}

		return string(f)
	})
	if err != nil {
		return nil, err
	}

	var next func(int) []map[string]any
	err = runtime.ExportTo(runtime.Get("next"), &next)
	if err != nil {
		panic(err)
	}

	return &Generator{
		next: func(i int, options ...NextOption) (*Action, error) {

			nextOptions := NextOptions{}
			for _, option := range options {
				option(&nextOptions)
			}

			if nextOptions.Globals != nil {
				for k, v := range nextOptions.Globals {
					if err := runtime.Set(k, v); err != nil {
						return nil, fmt.Errorf("failed to set global variable %s: %w", k, err)
					}
				}
			}

			rawElements := next(i)

			var (
				action   string
				ik       string
				data     map[string]any
				ok       bool
				elements = make([]bulking.BulkElement, 0)
			)
			for _, rawElement := range rawElements {

				rawAction := rawElement["action"]
				if rawAction == nil {
					return nil, errors.New("'action' must be set")
				}

				action, ok = rawAction.(string)
				if !ok {
					return nil, errors.New("'action' must be a string")
				}

				rawData := rawElement["data"]
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
				payload, err := bulking.UnmarshalBulkElementPayload(action, dataAsJsonRawMessage)
				if err != nil {
					return nil, err
				}

				rawIK := rawElement["ik"]
				if rawIK != nil {
					ik, ok = rawIK.(string)
					if !ok {
						return nil, errors.New("'ik' must be a string")
					}
				}

				elements = append(elements, bulking.BulkElement{
					Action:         action,
					IdempotencyKey: ik,
					Data:           payload,
				})
			}

			return &Action{
				elements: elements,
			}, nil
		},
	}, nil
}

type config struct {
	globals  map[string]any
	rootPath string
}

type Option func(*config)

func WithGlobals(globals map[string]any) Option {
	return func(c *config) {
		c.globals = globals
	}
}

func WithRootPath(path string) Option {
	return func(c *config) {
		c.rootPath = path
	}
}

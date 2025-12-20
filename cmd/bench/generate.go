package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dop251/goja"
	ledgerclient "github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/google/uuid"
)

//go:embed scripts/*.js
var scriptsFS embed.FS

type Generator struct {
	next func(int, ...NextOption) (*components.BulkElement, error)
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

func (g *Generator) Next(iteration int, options ...NextOption) (*components.BulkElement, error) {
	return g.next(iteration, options...)
}

type GeneratorConfig struct {
	globals  map[string]any
	rootPath string
}

type GeneratorOption func(*GeneratorConfig)

func WithGlobals(globals map[string]any) GeneratorOption {
	return func(c *GeneratorConfig) {
		c.globals = globals
	}
}

func WithRootPath(path string) GeneratorOption {
	return func(c *GeneratorConfig) {
		c.rootPath = path
	}
}

func NewGenerator(script string, opts ...GeneratorOption) (*Generator, error) {
	cfg := &GeneratorConfig{}
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
		return nil, fmt.Errorf("failed to run script: %w", err)
	}

	runtime.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	err = runtime.Set("uuid", uuid.NewString)
	if err != nil {
		return nil, fmt.Errorf("failed to set uuid function: %w", err)
	}

	err = runtime.Set("read_file", func(path string) string {
		// Try embedded FS first
		if cfg.rootPath == "" {
			data, err := scriptsFS.ReadFile(filepath.Join("scripts", filepath.Base(path)))
			if err == nil {
				return string(data)
			}
		}

		// Fallback to file system
		f, err := os.ReadFile(filepath.Clean(filepath.Join(cfg.rootPath, path)))
		if err != nil {
			panic(fmt.Sprintf("failed to read file %s: %v", path, err))
		}
		return string(f)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set read_file function: %w", err)
	}

	fn := runtime.Get("next")
	if fn == nil {
		return nil, errors.New("script must export a 'next' function")
	}

	var next func(int) []map[string]any
	err = runtime.ExportTo(fn, &next)
	if err != nil {
		return nil, fmt.Errorf("script must export a 'next' function: %w", err)
	}

	return &Generator{
		next: func(i int, options ...NextOption) (*components.BulkElement, error) {
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
			if len(rawElements) == 0 {
				return nil, errors.New("script returned empty array")
			}

			// Take the first element (scripts can return multiple, but we'll use the first one for simplicity)
			rawElement := rawElements[0]

			rawAction := rawElement["action"]
			if rawAction == nil {
				return nil, errors.New("'action' must be set")
			}

			action, ok := rawAction.(string)
			if !ok {
				return nil, errors.New("'action' must be a string")
			}

			rawData := rawElement["data"]
			if rawData == nil {
				return nil, errors.New("'data' must be set")
			}
			data, ok := rawData.(map[string]any)
			if !ok {
				return nil, errors.New("'data' must be a map[string]any")
			}

			rawIK := rawElement["ik"]
			var ik *string
			if rawIK != nil {
				ikStr, ok := rawIK.(string)
				if !ok {
					return nil, errors.New("'ik' must be a string")
				}
				ik = &ikStr
			}

			bulkElement := components.BulkElement{
				Action: components.Action(action),
				Ik:     ik,
			}

			// Convert data based on action type
			switch components.Action(action) {
			case components.ActionCreateTransaction:
				// Convert script data to CreateTransactionRequest
				scriptData, ok := data["script"].(map[string]any)
				if !ok {
					return nil, errors.New("CREATE_TRANSACTION data must have a 'script' field")
				}

				plain, ok := scriptData["plain"].(string)
				if !ok {
					return nil, errors.New("script.plain must be a string")
				}

				varsRaw := scriptData["vars"]
				vars := make(map[string]string)
				if varsRaw != nil {
					varsMap, ok := varsRaw.(map[string]any)
					if ok {
						for k, v := range varsMap {
							vars[k] = fmt.Sprint(v)
						}
					}
				}

				var reference *string
				if ref, ok := data["reference"].(string); ok && ref != "" {
					reference = &ref
				}

				var metadata map[string]any
				if md, ok := data["metadata"].(map[string]any); ok {
					metadata = md
				}

				bulkElement.Data = components.CreateBulkElementDataCreateTransactionRequest(components.CreateTransactionRequest{
					Script: &components.Script{
						Plain: plain,
						Vars:  vars,
					},
					Reference: reference,
					Metadata:  metadata,
				})

			case components.ActionAddMetadata:
				targetType, ok := data["targetType"].(string)
				if !ok {
					return nil, errors.New("ADD_METADATA data must have a 'targetType' field")
				}

				targetIDRaw := data["targetId"]
				if targetIDRaw == nil {
					return nil, errors.New("ADD_METADATA data must have a 'targetId' field")
				}

				var targetID components.TargetID
				if targetIDStr, ok := targetIDRaw.(string); ok {
					targetID = components.CreateTargetIDStr(targetIDStr)
				} else if targetIDInt, ok := targetIDRaw.(float64); ok {
					targetID = components.CreateTargetIDInteger(int64(targetIDInt))
				} else {
					return nil, errors.New("targetId must be a string or number")
				}

				metadata, ok := data["metadata"].(map[string]any)
				if !ok {
					return nil, errors.New("ADD_METADATA data must have a 'metadata' field")
				}

				bulkElement.Data = components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
					TargetType: components.TargetType(targetType),
					TargetID:   targetID,
					Metadata:   metadata,
				})

			default:
				return nil, fmt.Errorf("unsupported action: %s", action)
			}

			return &bulkElement, nil
		},
	}, nil
}

type ActionProviderFactory interface {
	Create() (ActionProvider, error)
}

type ActionProvider interface {
	Get(globalIteration, iteration int) (*components.BulkElement, error)
}

type JSActionProviderFactory struct {
	rootPath string
	script   string
}

func NewJSActionProviderFactory(rootPath, script string) *JSActionProviderFactory {
	return &JSActionProviderFactory{
		rootPath: rootPath,
		script:   script,
	}
}

func (f *JSActionProviderFactory) Create() (ActionProvider, error) {
	generator, err := NewGenerator(f.script, WithRootPath(f.rootPath))
	if err != nil {
		return nil, err
	}

	return &JSActionProvider{
		generator: generator,
	}, nil
}

type JSActionProvider struct {
	generator *Generator
}

func (p *JSActionProvider) Get(globalIteration, iteration int) (*components.BulkElement, error) {
	return p.generator.Next(iteration, WithNextGlobals(map[string]any{
		"iteration": globalIteration,
	}))
}

// LoadScriptsFromEmbed loads scripts from the embedded filesystem
func LoadScriptsFromEmbed() (map[string]ActionProviderFactory, error) {
	scripts := make(map[string]ActionProviderFactory)

	entries, err := scriptsFS.ReadDir("scripts")
	if err != nil {
		return nil, fmt.Errorf("reading scripts directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".js") {
			continue
		}

		script, err := scriptsFS.ReadFile(filepath.Join("scripts", entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading script %s: %w", entry.Name(), err)
		}

		rootPath, err := filepath.Abs("cmd/bench/scripts")
		if err != nil {
			rootPath = "cmd/bench/scripts"
		}

		scriptName := strings.TrimSuffix(entry.Name(), ".js")
		scripts[scriptName] = NewJSActionProviderFactory(rootPath, string(script))
	}

	return scripts, nil
}

// LoadScriptFromFile loads a script from a file path
func LoadScriptFromFile(scriptPath string) (ActionProviderFactory, error) {
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("reading script file %s: %w", scriptPath, err)
	}

	rootPath, err := filepath.Abs(filepath.Dir(scriptPath))
	if err != nil {
		rootPath = filepath.Dir(scriptPath)
	}

	return NewJSActionProviderFactory(rootPath, string(script)), nil
}

// ApplyAction applies a bulk element to the ledger using the SDK
func ApplyAction(ctx context.Context, client *ledgerclient.Formance, ledgerName string, element *components.BulkElement) error {
	_, err := client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
		LedgerName:  ledgerName,
		RequestBody: []components.BulkElement{*element},
	})
	return err
}

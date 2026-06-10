package ledgers

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// EditableConfig represents the editable (declarative) configuration of a ledger.
// Read-only status fields (build status, conversion status, etc.) are excluded.
type EditableConfig struct {
	DefaultEnforcementMode string                                  `json:"defaultEnforcementMode,omitempty" yaml:"defaultEnforcementMode,omitempty"`
	AccountTypes           map[string]EditableAccountType          `json:"accountTypes,omitempty"           yaml:"accountTypes,omitempty"`
	MetadataSchema         map[string]map[string]EditableMetaField `json:"metadataSchema,omitempty"         yaml:"metadataSchema,omitempty"`
	Indexes                EditableIndexes                         `json:"indexes"                          yaml:"indexes"`
	PreparedQueries        map[string]EditablePreparedQuery        `json:"preparedQueries,omitempty"        yaml:"preparedQueries,omitempty"`
	Numscripts             map[string]EditableNumscript            `json:"numscripts,omitempty"             yaml:"numscripts,omitempty"`
}

// EditableAccountType is the editable subset of an account type.
type EditableAccountType struct {
	Pattern string `json:"pattern" yaml:"pattern"`
}

// EditableMetaField is the editable subset of a metadata field declaration.
type EditableMetaField struct {
	Type    string `json:"type"              yaml:"type"`
	Indexed bool   `json:"indexed,omitempty" yaml:"indexed,omitempty"`
}

// EditableIndexes represents the set of enabled builtin indexes.
type EditableIndexes struct {
	Reference     bool `json:"reference"     yaml:"reference"`
	Timestamp     bool `json:"timestamp"     yaml:"timestamp"`
	Address       bool `json:"address"       yaml:"address"`
	SourceAddress bool `json:"sourceAddress" yaml:"sourceAddress"`
	DestAddress   bool `json:"destAddress"   yaml:"destAddress"`
	InsertedAt    bool `json:"insertedAt"    yaml:"insertedAt"`
}

// EditablePreparedQuery is the editable subset of a prepared query.
type EditablePreparedQuery struct {
	Target string `json:"target"           yaml:"target"`
	Filter string `json:"filter,omitempty" yaml:"filter,omitempty"`
}

// EditableNumscript is the editable subset of a numscript library entry.
type EditableNumscript struct {
	Content string `json:"content"           yaml:"content"`
	Version string `json:"version,omitempty" yaml:"version,omitempty"`
}

// ConfigFromProto builds an EditableConfig from the server's proto responses.
func ConfigFromProto(
	ledger *commonpb.LedgerInfo,
	queries []*commonpb.PreparedQuery,
	numscripts []*commonpb.NumscriptInfo,
) *EditableConfig {
	cfg := &EditableConfig{
		DefaultEnforcementMode: strings.ToLower(ledger.GetDefaultEnforcementMode().String()),
		AccountTypes:           make(map[string]EditableAccountType),
		MetadataSchema:         make(map[string]map[string]EditableMetaField),
		PreparedQueries:        make(map[string]EditablePreparedQuery),
		Numscripts:             make(map[string]EditableNumscript),
	}

	// Account types
	for name, at := range ledger.GetAccountTypes() {
		cfg.AccountTypes[name] = EditableAccountType{
			Pattern: at.GetPattern(),
		}
	}

	// indexedKeys collects the (target, key) pairs that have an active index
	// declared on the ledger. The metadata schema no longer carries the
	// "indexed" flag itself — it lives on LedgerInfo.indexes.
	indexedKeys := map[commonpb.TargetType]map[string]bool{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT:     {},
		commonpb.TargetType_TARGET_TYPE_TRANSACTION: {},
		commonpb.TargetType_TARGET_TYPE_LEDGER:      {},
	}

	for _, idx := range ledger.GetIndexes() {
		m, ok := idx.GetId().GetKind().(*commonpb.IndexID_Metadata)
		if !ok {
			continue
		}

		indexedKeys[m.Metadata.GetTarget()][m.Metadata.GetKey()] = true
	}

	// Metadata schema
	if schema := ledger.GetMetadataSchema(); schema != nil {
		if acct := schema.GetAccountFields(); len(acct) > 0 {
			m := make(map[string]EditableMetaField, len(acct))
			for key, field := range acct {
				m[key] = EditableMetaField{
					Type:    commonpb.MetadataTypeToString(field.GetType()),
					Indexed: indexedKeys[commonpb.TargetType_TARGET_TYPE_ACCOUNT][key],
				}
			}
			cfg.MetadataSchema["account"] = m
		}
		if tx := schema.GetTransactionFields(); len(tx) > 0 {
			m := make(map[string]EditableMetaField, len(tx))
			for key, field := range tx {
				m[key] = EditableMetaField{
					Type:    commonpb.MetadataTypeToString(field.GetType()),
					Indexed: indexedKeys[commonpb.TargetType_TARGET_TYPE_TRANSACTION][key],
				}
			}
			cfg.MetadataSchema["transaction"] = m
		}
		if lf := schema.GetLedgerFields(); len(lf) > 0 {
			m := make(map[string]EditableMetaField, len(lf))
			for key, field := range lf {
				m[key] = EditableMetaField{
					Type:    commonpb.MetadataTypeToString(field.GetType()),
					Indexed: indexedKeys[commonpb.TargetType_TARGET_TYPE_LEDGER][key],
				}
			}
			cfg.MetadataSchema["ledger"] = m
		}
	}

	// Builtin indexes — derived from LedgerInfo.indexes.
	for _, idx := range ledger.GetIndexes() {
		b, ok := idx.GetId().GetKind().(*commonpb.IndexID_TxBuiltin)
		if !ok {
			continue
		}

		switch b.TxBuiltin {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
			cfg.Indexes.Reference = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
			cfg.Indexes.Timestamp = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
			cfg.Indexes.Address = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
			cfg.Indexes.SourceAddress = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
			cfg.Indexes.DestAddress = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			cfg.Indexes.InsertedAt = true
		}
	}

	// Prepared queries
	for _, q := range queries {
		pq := EditablePreparedQuery{
			Target: queryTargetString(q.GetTarget()),
		}
		if q.GetFilter() != nil {
			pq.Filter = filterexpr.Format(q.GetFilter())
		}
		cfg.PreparedQueries[q.GetName()] = pq
	}

	// Numscripts
	for _, ns := range numscripts {
		cfg.Numscripts[ns.GetName()] = EditableNumscript{
			Content: ns.GetContent(),
			Version: ns.GetVersion(),
		}
	}

	return cfg
}

// WriteJSON writes the config to w as indented JSON.
func (c *EditableConfig) WriteJSON(w *os.File) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(c)
}

// WriteYAML writes the config to w as YAML.
func (c *EditableConfig) WriteYAML(w *os.File) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)

	err := enc.Encode(c)
	if closeErr := enc.Close(); err == nil {
		err = closeErr
	}

	return err
}

// ReadConfigFile reads an EditableConfig from a JSON or YAML file.
func ReadConfigFile(path string) (*EditableConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg EditableConfig

	// Try JSON first, fall back to YAML.
	if json.Valid(data) {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parse JSON config: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parse YAML config: %w", err)
		}
	}

	return &cfg, nil
}

// DiffAction describes one mutation to apply.
type DiffAction struct {
	Section     string // e.g. "accountType", "metadataSchema", "index", "preparedQuery", "numscript"
	Operation   string // "add", "update", "remove"
	Description string // human-readable description
	Request     *servicepb.Request
}

// ComputeDiff compares current (from server) vs desired (from file) and returns
// the list of Apply requests needed to reconcile.
func ComputeDiff(ledgerName string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction

	actions = append(actions, diffDefaultEnforcementMode(ledgerName, current, desired)...)
	actions = append(actions, diffAccountTypes(ledgerName, current, desired)...)

	mdActions, err := diffMetadataSchema(ledgerName, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, mdActions...)

	actions = append(actions, diffIndexes(ledgerName, current, desired)...)

	pqActions, err := diffPreparedQueries(ledgerName, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, pqActions...)

	actions = append(actions, diffNumscripts(ledgerName, current, desired)...)

	return actions, nil
}

func diffDefaultEnforcementMode(ledgerName string, current, desired *EditableConfig) []DiffAction {
	if strings.EqualFold(current.DefaultEnforcementMode, desired.DefaultEnforcementMode) {
		return nil
	}

	mode := parseEnforcementModeProto(desired.DefaultEnforcementMode)

	return []DiffAction{
		{
			Section:     "defaultEnforcementMode",
			Operation:   "update",
			Description: fmt.Sprintf("Update default enforcement mode: %s -> %s", current.DefaultEnforcementMode, desired.DefaultEnforcementMode),
			Request: &servicepb.Request{
				Type: &servicepb.Request_SetDefaultEnforcementMode{
					SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
						Ledger:          ledgerName,
						EnforcementMode: mode,
					},
				},
			},
		},
	}
}

func diffAccountTypes(ledgerName string, current, desired *EditableConfig) []DiffAction {
	var actions []DiffAction

	// Added or updated
	for name, desiredAT := range desired.AccountTypes {
		currentAT, exists := current.AccountTypes[name]
		if !exists {
			actions = append(actions, DiffAction{
				Section:     "accountType",
				Operation:   "add",
				Description: fmt.Sprintf("Add account type %q (pattern=%s)", name, desiredAT.Pattern),
				Request: &servicepb.Request{
					Type: &servicepb.Request_AddAccountType{
						AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
							Ledger: ledgerName,
							AccountType: &commonpb.AccountType{
								Name:    name,
								Pattern: desiredAT.Pattern,
							},
						},
					},
				},
			})

			continue
		}

		// Pattern changed → remove + add
		if currentAT.Pattern != desiredAT.Pattern {
			actions = append(actions, DiffAction{
				Section:     "accountType",
				Operation:   "remove",
				Description: fmt.Sprintf("Remove account type %q (pattern change)", name),
				Request: &servicepb.Request{
					Type: &servicepb.Request_RemoveAccountType{
						RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
							Ledger: ledgerName,
							Name:   name,
						},
					},
				},
			})
			actions = append(actions, DiffAction{
				Section:     "accountType",
				Operation:   "add",
				Description: fmt.Sprintf("Re-add account type %q (pattern=%s)", name, desiredAT.Pattern),
				Request: &servicepb.Request{
					Type: &servicepb.Request_AddAccountType{
						AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
							Ledger: ledgerName,
							AccountType: &commonpb.AccountType{
								Name:    name,
								Pattern: desiredAT.Pattern,
							},
						},
					},
				},
			})

			continue
		}
	}

	// Removed
	names := sortedKeys(current.AccountTypes)
	for _, name := range names {
		if _, exists := desired.AccountTypes[name]; !exists {
			actions = append(actions, DiffAction{
				Section:     "accountType",
				Operation:   "remove",
				Description: fmt.Sprintf("Remove account type %q", name),
				Request: &servicepb.Request{
					Type: &servicepb.Request_RemoveAccountType{
						RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
							Ledger: ledgerName,
							Name:   name,
						},
					},
				},
			})
		}
	}

	return actions
}

func diffMetadataSchema(ledgerName string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction

	for _, target := range []string{"account", "transaction", "ledger"} {
		targetType, err := commonpb.ParseTargetType(target)
		if err != nil {
			return nil, err
		}

		currentFields := current.MetadataSchema[target]
		desiredFields := desired.MetadataSchema[target]

		// Added or updated fields
		for key, desiredField := range desiredFields {
			currentField, exists := currentFields[key]

			if !exists || currentField.Type != desiredField.Type {
				mdType, err := commonpb.ParseMetadataType(desiredField.Type)
				if err != nil {
					return nil, fmt.Errorf("metadata field %s.%s: %w", target, key, err)
				}

				op := "add"
				desc := fmt.Sprintf("Set metadata type %s.%s = %s", target, key, desiredField.Type)
				if exists {
					op = "update"
					desc = fmt.Sprintf("Change metadata type %s.%s: %s -> %s", target, key, currentField.Type, desiredField.Type)
				}

				actions = append(actions, DiffAction{
					Section:     "metadataSchema",
					Operation:   op,
					Description: desc,
					Request: &servicepb.Request{
						Type: &servicepb.Request_SetMetadataFieldType{
							SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
								Ledger:     ledgerName,
								TargetType: targetType,
								Key:        key,
								Type:       mdType,
							},
						},
					},
				})
			}

			// Indexed changed: add index
			if desiredField.Indexed && (!exists || !currentField.Indexed) {
				actions = append(actions, metadataIndexAction(ledgerName, target, targetType, key, "add"))
			}

			// Indexed removed: drop index
			if !desiredField.Indexed && exists && currentField.Indexed {
				actions = append(actions, metadataIndexAction(ledgerName, target, targetType, key, "remove"))
			}
		}

		// Removed fields
		for key := range currentFields {
			if _, exists := desiredFields[key]; !exists {
				actions = append(actions, DiffAction{
					Section:     "metadataSchema",
					Operation:   "remove",
					Description: fmt.Sprintf("Remove metadata type %s.%s", target, key),
					Request: &servicepb.Request{
						Type: &servicepb.Request_RemoveMetadataFieldType{
							RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
								Ledger:     ledgerName,
								TargetType: targetType,
								Key:        key,
							},
						},
					},
				})
			}
		}
	}

	return actions, nil
}

func metadataIndexAction(ledgerName, target string, targetType commonpb.TargetType, key, op string) DiffAction {
	id := &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
		Target: targetType,
		Key:    key,
	}}}

	if op == "add" {
		return DiffAction{
			Section:     "index",
			Operation:   "add",
			Description: fmt.Sprintf("Create metadata index %s.%s", target, key),
			Request: &servicepb.Request{
				Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{
					Ledger: ledgerName,
					Id:     id,
				}},
			},
		}
	}

	return DiffAction{
		Section:     "index",
		Operation:   "remove",
		Description: fmt.Sprintf("Drop metadata index %s.%s", target, key),
		Request: &servicepb.Request{
			Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{
				Ledger: ledgerName,
				Id:     id,
			}},
		},
	}
}

func diffIndexes(ledgerName string, current, desired *EditableConfig) []DiffAction {
	var actions []DiffAction

	type builtinDef struct {
		name    string
		cur     bool
		des     bool
		builtin commonpb.TransactionBuiltinIndex
	}

	builtins := []builtinDef{
		{"reference", current.Indexes.Reference, desired.Indexes.Reference, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
		{"timestamp", current.Indexes.Timestamp, desired.Indexes.Timestamp, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP},
		{"address", current.Indexes.Address, desired.Indexes.Address, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS},
		{"source-address", current.Indexes.SourceAddress, desired.Indexes.SourceAddress, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS},
		{"dest-address", current.Indexes.DestAddress, desired.Indexes.DestAddress, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS},
		{"inserted-at", current.Indexes.InsertedAt, desired.Indexes.InsertedAt, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT},
	}

	for _, b := range builtins {
		id := &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: b.builtin}}

		if b.des && !b.cur {
			actions = append(actions, DiffAction{
				Section:     "index",
				Operation:   "add",
				Description: "Create index " + b.name,
				Request: &servicepb.Request{
					Type: &servicepb.Request_CreateIndex{
						CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledgerName, Id: id},
					},
				},
			})
		}
		if !b.des && b.cur {
			actions = append(actions, DiffAction{
				Section:     "index",
				Operation:   "remove",
				Description: "Drop index " + b.name,
				Request: &servicepb.Request{
					Type: &servicepb.Request_DropIndex{
						DropIndex: &servicepb.DropIndexRequest{Ledger: ledgerName, Id: id},
					},
				},
			})
		}
	}

	// Log-ledger index
	return actions
}

func diffPreparedQueries(ledgerName string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction

	// Added or updated
	for name, desiredPQ := range desired.PreparedQueries {
		currentPQ, exists := current.PreparedQueries[name]
		if !exists {
			target := parseQueryTarget(desiredPQ.Target)
			var filter *commonpb.QueryFilter
			if desiredPQ.Filter != "" {
				var err error
				filter, err = filterexpr.Parse(desiredPQ.Filter)
				if err != nil {
					return nil, fmt.Errorf("prepared query %q filter: %w", name, err)
				}
			}
			actions = append(actions, DiffAction{
				Section:     "preparedQuery",
				Operation:   "add",
				Description: fmt.Sprintf("Create prepared query %q (target=%s)", name, desiredPQ.Target),
				Request: &servicepb.Request{
					Type: &servicepb.Request_CreatePreparedQuery{
						CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
							Query: &commonpb.PreparedQuery{
								Name:   name,
								Ledger: ledgerName,
								Target: target,
								Filter: filter,
							},
						},
					},
				},
			})

			continue
		}

		// Filter or target changed → update
		if currentPQ.Target != desiredPQ.Target || currentPQ.Filter != desiredPQ.Filter {
			var filter *commonpb.QueryFilter
			if desiredPQ.Filter != "" {
				var err error
				filter, err = filterexpr.Parse(desiredPQ.Filter)
				if err != nil {
					return nil, fmt.Errorf("prepared query %q filter: %w", name, err)
				}
			}
			actions = append(actions, DiffAction{
				Section:     "preparedQuery",
				Operation:   "update",
				Description: fmt.Sprintf("Update prepared query %q", name),
				Request: &servicepb.Request{
					Type: &servicepb.Request_UpdatePreparedQuery{
						UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
							Ledger: ledgerName,
							Name:   name,
							Filter: filter,
						},
					},
				},
			})
		}
	}

	// Removed
	for name := range current.PreparedQueries {
		if _, exists := desired.PreparedQueries[name]; !exists {
			actions = append(actions, DiffAction{
				Section:     "preparedQuery",
				Operation:   "remove",
				Description: fmt.Sprintf("Delete prepared query %q", name),
				Request: &servicepb.Request{
					Type: &servicepb.Request_DeletePreparedQuery{
						DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
							Ledger: ledgerName,
							Name:   name,
						},
					},
				},
			})
		}
	}

	return actions, nil
}

func diffNumscripts(ledgerName string, current, desired *EditableConfig) []DiffAction {
	var actions []DiffAction

	// Added or updated
	for name, desiredNS := range desired.Numscripts {
		currentNS, exists := current.Numscripts[name]
		if !exists || currentNS.Content != desiredNS.Content || currentNS.Version != desiredNS.Version {
			op := "add"
			desc := fmt.Sprintf("Save numscript %q", name)
			if desiredNS.Version != "" {
				desc += fmt.Sprintf(" (v%s)", desiredNS.Version)
			}
			if exists {
				op = "update"
				desc = fmt.Sprintf("Update numscript %q", name)
				if desiredNS.Version != "" {
					desc += fmt.Sprintf(" (v%s)", desiredNS.Version)
				}
			}
			actions = append(actions, DiffAction{
				Section:     "numscript",
				Operation:   op,
				Description: desc,
				Request: &servicepb.Request{
					Type: &servicepb.Request_SaveNumscript{
						SaveNumscript: &servicepb.SaveNumscriptRequest{
							Ledger:  ledgerName,
							Name:    name,
							Content: desiredNS.Content,
							Version: desiredNS.Version,
						},
					},
				},
			})
		}
	}

	// Removed
	for name := range current.Numscripts {
		if _, exists := desired.Numscripts[name]; !exists {
			actions = append(actions, DiffAction{
				Section:     "numscript",
				Operation:   "remove",
				Description: fmt.Sprintf("Delete numscript %q", name),
				Request: &servicepb.Request{
					Type: &servicepb.Request_DeleteNumscript{
						DeleteNumscript: &servicepb.DeleteNumscriptRequest{
							Ledger: ledgerName,
							Name:   name,
						},
					},
				},
			})
		}
	}

	return actions
}

func parseEnforcementModeProto(s string) commonpb.ChartEnforcementMode {
	switch strings.ToLower(s) {
	case "audit", "chart_enforcement_audit":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
	default:
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
	}
}

// parseEnforcementModeProtoStrict parses an enforcement mode string with validation.
func parseEnforcementModeProtoStrict(s string) (commonpb.ChartEnforcementMode, error) {
	switch strings.ToUpper(s) {
	case "STRICT":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nil
	case "AUDIT":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nil
	default:
		return 0, fmt.Errorf("invalid enforcement mode %q: must be STRICT or AUDIT", s)
	}
}

func parseQueryTarget(s string) commonpb.QueryTarget {
	switch strings.ToLower(s) {
	case "transactions":
		return commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	case "logs":
		return commonpb.QueryTarget_QUERY_TARGET_LOGS
	default:
		return commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

package ledgers

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
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

// EditableAccountType is the editable subset of an account type. Persistence
// is omitted when it is the proto default ("normal") so simple configs stay
// terse; non-default values must round-trip to avoid silently downgrading
// ephemeral/transient types to normal at apply time.
type EditableAccountType struct {
	Pattern     string `json:"pattern"               yaml:"pattern"`
	Persistence string `json:"persistence,omitempty" yaml:"persistence,omitempty"`
}

// EditableMetaField is the editable subset of a metadata field declaration.
type EditableMetaField struct {
	Type    string `json:"type"              yaml:"type"`
	Indexed bool   `json:"indexed,omitempty" yaml:"indexed,omitempty"`
}

// EditableIndexes represents the set of enabled builtin indexes.
type EditableIndexes struct {
	Reference          bool `json:"reference"          yaml:"reference"`
	Timestamp          bool `json:"timestamp"          yaml:"timestamp"`
	Address            bool `json:"address"            yaml:"address"`
	SourceAddress      bool `json:"sourceAddress"      yaml:"sourceAddress"`
	DestinationAddress bool `json:"destinationAddress" yaml:"destinationAddress"`
	InsertedAt         bool `json:"insertedAt"         yaml:"insertedAt"`
	RevertedAt         bool `json:"revertedAt"         yaml:"revertedAt"`
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
//
// indexes carries the entries returned by BucketService.ListIndexes scoped to
// this ledger; it replaces the former LedgerInfo.indexes embed.
func ConfigFromProto(
	ledger *commonpb.LedgerInfo,
	indexes []*commonpb.Index,
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
			Pattern:     at.GetPattern(),
			Persistence: persistenceToString(at.GetPersistence()),
		}
	}

	// indexedKeys collects the (target, key) pairs that have an active index
	// declared on the ledger. Sourced from the indexes slice (BucketService.
	// ListIndexes) since indexes no longer live inside LedgerInfo.
	indexedKeys := map[commonpb.TargetType]map[string]bool{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT:     {},
		commonpb.TargetType_TARGET_TYPE_TRANSACTION: {},
		commonpb.TargetType_TARGET_TYPE_LEDGER:      {},
	}

	for _, idx := range indexes {
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

	// Builtin indexes — sourced from the bucket index registry.
	for _, idx := range indexes {
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
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS:
			cfg.Indexes.DestinationAddress = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			cfg.Indexes.InsertedAt = true
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
			cfg.Indexes.RevertedAt = true
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
func (c *EditableConfig) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(c)
}

// WriteYAML writes the config to w as YAML.
func (c *EditableConfig) WriteYAML(w io.Writer) error {
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

	atActions, err := diffAccountTypes(ledgerName, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, atActions...)

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

	nsActions, err := diffNumscripts(ledgerName, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, nsActions...)

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

func diffAccountTypes(ledgerName string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction

	addAction := func(name string, at EditableAccountType, reAdd bool) (DiffAction, error) {
		persistence, err := accounttypes.ParsePersistence(at.Persistence)
		if err != nil {
			return DiffAction{}, fmt.Errorf("account type %q: %w", name, err)
		}

		desc := fmt.Sprintf("Add account type %q (pattern=%s", name, at.Pattern)
		if reAdd {
			desc = fmt.Sprintf("Re-add account type %q (pattern=%s", name, at.Pattern)
		}

		if at.Persistence != "" {
			desc += ", persistence=" + at.Persistence
		}

		desc += ")"

		return DiffAction{
			Section:     "accountType",
			Operation:   "add",
			Description: desc,
			Request: &servicepb.Request{
				Type: &servicepb.Request_AddAccountType{
					AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
						Ledger: ledgerName,
						AccountType: &commonpb.AccountType{
							Name:        name,
							Pattern:     at.Pattern,
							Persistence: persistence,
						},
					},
				},
			},
		}, nil
	}

	// Added or updated
	for name, desiredAT := range desired.AccountTypes {
		currentAT, exists := current.AccountTypes[name]
		if !exists {
			action, err := addAction(name, desiredAT, false)
			if err != nil {
				return nil, err
			}

			actions = append(actions, action)

			continue
		}

		// Compare persistence on the parsed enum, not the raw string —
		// ParsePersistence accepts "", "normal", "NORMAL", "Normal" as
		// equivalent (same for ephemeral/transient). A raw string compare
		// would plan a spurious remove+add when a hand-edited manifest
		// uses a different-but-equivalent spelling than the canonical
		// export form.
		currentPersistence, err := accounttypes.ParsePersistence(currentAT.Persistence)
		if err != nil {
			return nil, fmt.Errorf("current account type %q: %w", name, err)
		}

		desiredPersistence, err := accounttypes.ParsePersistence(desiredAT.Persistence)
		if err != nil {
			return nil, fmt.Errorf("account type %q: %w", name, err)
		}

		// Pattern or persistence changed → remove + add. The server has no
		// in-place updater for account types, and persistence drift would
		// silently flip volume storage behavior if we ignored it here.
		if currentAT.Pattern != desiredAT.Pattern || currentPersistence != desiredPersistence {
			reason := "pattern change"
			if currentAT.Pattern == desiredAT.Pattern {
				reason = "persistence change"
			}

			actions = append(actions, DiffAction{
				Section:     "accountType",
				Operation:   "remove",
				Description: fmt.Sprintf("Remove account type %q (%s)", name, reason),
				Request: &servicepb.Request{
					Type: &servicepb.Request_RemoveAccountType{
						RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
							Ledger: ledgerName,
							Name:   name,
						},
					},
				},
			})

			action, err := addAction(name, desiredAT, true)
			if err != nil {
				return nil, err
			}

			actions = append(actions, action)

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

	return actions, nil
}

// persistenceToString returns the lowercase string form of an account-type
// persistence mode, or "" for the proto default (NORMAL) so that
// `omitempty` keeps it out of exported manifests.
func persistenceToString(p commonpb.AccountTypePersistence) string {
	switch p {
	case commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL:
		return "ephemeral"
	case commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT:
		return "transient"
	default:
		return ""
	}
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
		{"destination-address", current.Indexes.DestinationAddress, desired.Indexes.DestinationAddress, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS},
		{"inserted-at", current.Indexes.InsertedAt, desired.Indexes.InsertedAt, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT},
		{"reverted-at", current.Indexes.RevertedAt, desired.Indexes.RevertedAt, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT},
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
				filter, err = filterexpr.Parse(desiredPQ.Filter, target)
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
							Ledger: ledgerName,
							Query: &commonpb.PreparedQuery{
								Name:   name,
								Target: target,
								Filter: filter,
							},
						},
					},
				},
			})

			continue
		}

		// Target changed → delete + create. `UpdatePreparedQueryRequest`
		// carries only Filter (no Target), so an in-place update would
		// silently drop the target change. Mirror the `diffAccountTypes`
		// remove+re-add pattern (#502 review).
		if currentPQ.Target != desiredPQ.Target {
			target := parseQueryTarget(desiredPQ.Target)

			var filter *commonpb.QueryFilter
			if desiredPQ.Filter != "" {
				var err error
				filter, err = filterexpr.Parse(desiredPQ.Filter, target)
				if err != nil {
					return nil, fmt.Errorf("prepared query %q filter: %w", name, err)
				}
			}

			actions = append(actions, DiffAction{
				Section:     "preparedQuery",
				Operation:   "remove",
				Description: fmt.Sprintf("Delete prepared query %q (target change)", name),
				Request: &servicepb.Request{
					Type: &servicepb.Request_DeletePreparedQuery{
						DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
							Ledger: ledgerName,
							Name:   name,
						},
					},
				},
			})

			actions = append(actions, DiffAction{
				Section:     "preparedQuery",
				Operation:   "add",
				Description: fmt.Sprintf("Re-create prepared query %q (target=%s)", name, desiredPQ.Target),
				Request: &servicepb.Request{
					Type: &servicepb.Request_CreatePreparedQuery{
						CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
							Ledger: ledgerName,
							Query: &commonpb.PreparedQuery{
								Name:   name,
								Target: target,
								Filter: filter,
							},
						},
					},
				},
			})

			continue
		}

		// Same target — only Filter can have moved. Use the in-place update.
		if currentPQ.Filter != desiredPQ.Filter {
			target := parseQueryTarget(desiredPQ.Target)

			var filter *commonpb.QueryFilter
			if desiredPQ.Filter != "" {
				var err error
				filter, err = filterexpr.Parse(desiredPQ.Filter, target)
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

func diffNumscripts(ledgerName string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction

	// The library is immutable and append-only: a save is only ever a NEW
	// version. Editing a version's content in place is impossible server-side,
	// so an edit that keeps the same version is a mistake — catch it here
	// rather than emitting a save the server rejects with
	// NUMSCRIPT_VERSION_ALREADY_EXISTS.
	for name, desiredNS := range desired.Numscripts {
		currentNS, exists := current.Numscripts[name]

		if exists && currentNS.Version == desiredNS.Version {
			if currentNS.Content != desiredNS.Content {
				return nil, fmt.Errorf("numscript %q: content changed but version %q was not bumped; publish the change under a new semver", name, desiredNS.Version)
			}

			continue
		}

		// The server requires an explicit full canonical semver; validate here
		// so an omitted/partial/non-canonical version fails the diff with a
		// clear message rather than a valid-looking plan that only errors at
		// apply time.
		if _, err := semver.Parse(desiredNS.Version); err != nil {
			return nil, fmt.Errorf("numscript %q: %w (a full canonical semver is required, e.g. 1.0.0)", name, err)
		}

		desc := fmt.Sprintf("Save numscript %q (v%s)", name, desiredNS.Version)

		actions = append(actions, DiffAction{
			Section:     "numscript",
			Operation:   "add",
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

	// Numscripts are immutable and append-only — there is no removal action.

	return actions, nil
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

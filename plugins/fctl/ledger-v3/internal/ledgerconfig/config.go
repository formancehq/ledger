package ledgerconfig

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// EditableConfig is the declarative, writable subset of a Ledger configuration.
type EditableConfig struct {
	DefaultEnforcementMode string                                  `json:"defaultEnforcementMode,omitempty" yaml:"defaultEnforcementMode,omitempty"`
	AccountTypes           map[string]EditableAccountType          `json:"accountTypes,omitempty" yaml:"accountTypes,omitempty"`
	MetadataSchema         map[string]map[string]EditableMetaField `json:"metadataSchema,omitempty" yaml:"metadataSchema,omitempty"`
	Indexes                EditableIndexes                         `json:"indexes" yaml:"indexes"`
	PreparedQueries        map[string]EditablePreparedQuery        `json:"preparedQueries,omitempty" yaml:"preparedQueries,omitempty"`
	Numscripts             map[string]EditableNumscript            `json:"numscripts,omitempty" yaml:"numscripts,omitempty"`
}

type EditableAccountType struct {
	Pattern     string `json:"pattern" yaml:"pattern"`
	Persistence string `json:"persistence,omitempty" yaml:"persistence,omitempty"`
}

type EditableMetaField struct {
	Type    string `json:"type" yaml:"type"`
	Indexed bool   `json:"indexed,omitempty" yaml:"indexed,omitempty"`
}

type EditableIndexes struct {
	Reference          bool `json:"reference" yaml:"reference"`
	Timestamp          bool `json:"timestamp" yaml:"timestamp"`
	Address            bool `json:"address" yaml:"address"`
	SourceAddress      bool `json:"sourceAddress" yaml:"sourceAddress"`
	DestinationAddress bool `json:"destinationAddress" yaml:"destinationAddress"`
	InsertedAt         bool `json:"insertedAt" yaml:"insertedAt"`
	RevertedAt         bool `json:"revertedAt" yaml:"revertedAt"`
}

type EditablePreparedQuery struct {
	Target string `json:"target" yaml:"target"`
	Filter string `json:"filter,omitempty" yaml:"filter,omitempty"`
}

type EditableNumscript struct {
	Content string `json:"content" yaml:"content"`
	Version string `json:"version,omitempty" yaml:"version,omitempty"`
}

// DiffAction describes one mutation in a declarative reconciliation.
type DiffAction struct {
	Section     string
	Operation   string
	Description string
	Request     *servicepb.Request
}

func ConfigFromProto(ledger *commonpb.LedgerInfo, indexes []*commonpb.Index, queries []*commonpb.PreparedQuery, numscripts []*commonpb.NumscriptInfo) *EditableConfig {
	cfg := &EditableConfig{
		DefaultEnforcementMode: strings.ToLower(ledger.GetDefaultEnforcementMode().String()),
		AccountTypes:           make(map[string]EditableAccountType),
		MetadataSchema:         make(map[string]map[string]EditableMetaField),
		PreparedQueries:        make(map[string]EditablePreparedQuery),
		Numscripts:             make(map[string]EditableNumscript),
	}
	for name, accountType := range ledger.GetAccountTypes() {
		cfg.AccountTypes[name] = EditableAccountType{Pattern: accountType.GetPattern(), Persistence: formatPersistence(accountType.GetPersistence())}
	}
	indexed := map[commonpb.TargetType]map[string]bool{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT: {}, commonpb.TargetType_TARGET_TYPE_TRANSACTION: {}, commonpb.TargetType_TARGET_TYPE_LEDGER: {},
	}
	for _, index := range indexes {
		if metadata, ok := index.GetId().GetKind().(*commonpb.IndexID_Metadata); ok {
			indexed[metadata.Metadata.GetTarget()][metadata.Metadata.GetKey()] = true
		}
	}
	if schema := ledger.GetMetadataSchema(); schema != nil {
		addSchema(cfg, "account", schema.GetAccountFields(), indexed[commonpb.TargetType_TARGET_TYPE_ACCOUNT])
		addSchema(cfg, "transaction", schema.GetTransactionFields(), indexed[commonpb.TargetType_TARGET_TYPE_TRANSACTION])
		addSchema(cfg, "ledger", schema.GetLedgerFields(), indexed[commonpb.TargetType_TARGET_TYPE_LEDGER])
	}
	for _, index := range indexes {
		builtin, ok := index.GetId().GetKind().(*commonpb.IndexID_TxBuiltin)
		if !ok {
			continue
		}
		switch builtin.TxBuiltin {
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
	for _, query := range queries {
		value := EditablePreparedQuery{Target: queryTargetString(query.GetTarget())}
		if query.GetFilter() != nil {
			value.Filter = Format(query.GetFilter())
		}
		cfg.PreparedQueries[query.GetName()] = value
	}
	for _, numscript := range numscripts {
		cfg.Numscripts[numscript.GetName()] = EditableNumscript{Content: numscript.GetContent(), Version: numscript.GetVersion()}
	}
	return cfg
}

func addSchema(cfg *EditableConfig, target string, fields map[string]*commonpb.MetadataFieldSchema, indexed map[string]bool) {
	if len(fields) == 0 {
		return
	}
	out := make(map[string]EditableMetaField, len(fields))
	for key, field := range fields {
		out[key] = EditableMetaField{Type: commonpb.MetadataTypeToString(field.GetType()), Indexed: indexed[key]}
	}
	cfg.MetadataSchema[target] = out
}

func ComputeDiff(ledger string, current, desired *EditableConfig) ([]DiffAction, error) {
	actions := diffEnforcement(ledger, current, desired)
	accountTypes, err := diffAccountTypes(ledger, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, accountTypes...)
	metadata, err := diffMetadata(ledger, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, metadata...)
	actions = append(actions, diffIndexes(ledger, current, desired)...)
	queries, err := diffQueries(ledger, current, desired)
	if err != nil {
		return nil, err
	}
	actions = append(actions, queries...)
	numscripts, err := diffNumscripts(ledger, current, desired)
	if err != nil {
		return nil, err
	}
	return append(actions, numscripts...), nil
}

func diffEnforcement(ledger string, current, desired *EditableConfig) []DiffAction {
	if strings.EqualFold(current.DefaultEnforcementMode, desired.DefaultEnforcementMode) {
		return nil
	}
	mode := commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
	if strings.EqualFold(desired.DefaultEnforcementMode, "audit") || strings.EqualFold(desired.DefaultEnforcementMode, "chart_enforcement_audit") {
		mode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
	}
	return []DiffAction{{Section: "defaultEnforcementMode", Operation: "update", Request: &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{Ledger: ledger, EnforcementMode: mode}}}}}
}

func diffAccountTypes(ledger string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction
	add := func(name string, value EditableAccountType) (DiffAction, error) {
		persistence, err := parsePersistence(value.Persistence)
		if err != nil {
			return DiffAction{}, fmt.Errorf("account type %q: %w", name, err)
		}
		return DiffAction{Section: "accountType", Operation: "add", Request: &servicepb.Request{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{Ledger: ledger, AccountType: &commonpb.AccountType{Name: name, Pattern: value.Pattern, Persistence: persistence}}}}}, nil
	}
	for _, name := range sortedKeys(desired.AccountTypes) {
		want := desired.AccountTypes[name]
		have, exists := current.AccountTypes[name]
		if !exists {
			action, err := add(name, want)
			if err != nil {
				return nil, err
			}
			actions = append(actions, action)
			continue
		}
		havePersistence, err := parsePersistence(have.Persistence)
		if err != nil {
			return nil, fmt.Errorf("current account type %q: %w", name, err)
		}
		wantPersistence, err := parsePersistence(want.Persistence)
		if err != nil {
			return nil, fmt.Errorf("account type %q: %w", name, err)
		}
		if have.Pattern != want.Pattern || havePersistence != wantPersistence {
			actions = append(actions, DiffAction{Section: "accountType", Operation: "remove", Request: &servicepb.Request{Type: &servicepb.Request_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{Ledger: ledger, Name: name}}}})
			action, err := add(name, want)
			if err != nil {
				return nil, err
			}
			actions = append(actions, action)
		}
	}
	for _, name := range sortedKeys(current.AccountTypes) {
		if _, exists := desired.AccountTypes[name]; !exists {
			actions = append(actions, DiffAction{Section: "accountType", Operation: "remove", Request: &servicepb.Request{Type: &servicepb.Request_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{Ledger: ledger, Name: name}}}})
		}
	}
	return actions, nil
}

func diffMetadata(ledger string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction
	for _, target := range []string{"account", "transaction", "ledger"} {
		targetType, err := commonpb.ParseTargetType(target)
		if err != nil {
			return nil, err
		}
		have, want := current.MetadataSchema[target], desired.MetadataSchema[target]
		for _, key := range sortedKeys(want) {
			wantField := want[key]
			haveField, exists := have[key]
			if !exists || haveField.Type != wantField.Type {
				metadataType, err := commonpb.ParseMetadataType(wantField.Type)
				if err != nil {
					return nil, fmt.Errorf("metadata field %s.%s: %w", target, key, err)
				}
				op := "add"
				if exists {
					op = "update"
				}
				actions = append(actions, DiffAction{Section: "metadataSchema", Operation: op, Request: &servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{Ledger: ledger, TargetType: targetType, Key: key, Type: metadataType}}}})
			}
			if wantField.Indexed && (!exists || !haveField.Indexed) {
				actions = append(actions, metadataIndex(ledger, targetType, key, true))
			}
			if !wantField.Indexed && exists && haveField.Indexed {
				actions = append(actions, metadataIndex(ledger, targetType, key, false))
			}
		}
		for _, key := range sortedKeys(have) {
			if _, exists := want[key]; !exists {
				actions = append(actions, DiffAction{Section: "metadataSchema", Operation: "remove", Request: &servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{Ledger: ledger, TargetType: targetType, Key: key}}}})
			}
		}
	}
	return actions, nil
}

func metadataIndex(ledger string, target commonpb.TargetType, key string, add bool) DiffAction {
	id := &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{Target: target, Key: key}}}
	if add {
		return DiffAction{Section: "index", Operation: "add", Request: &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledger, Id: id}}}}
	}
	return DiffAction{Section: "index", Operation: "remove", Request: &servicepb.Request{Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{Ledger: ledger, Id: id}}}}
}

func diffIndexes(ledger string, current, desired *EditableConfig) []DiffAction {
	definitions := []struct {
		have, want bool
		kind       commonpb.TransactionBuiltinIndex
	}{
		{current.Indexes.Reference, desired.Indexes.Reference, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
		{current.Indexes.Timestamp, desired.Indexes.Timestamp, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP},
		{current.Indexes.Address, desired.Indexes.Address, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS},
		{current.Indexes.SourceAddress, desired.Indexes.SourceAddress, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS},
		{current.Indexes.DestinationAddress, desired.Indexes.DestinationAddress, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS},
		{current.Indexes.InsertedAt, desired.Indexes.InsertedAt, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT},
		{current.Indexes.RevertedAt, desired.Indexes.RevertedAt, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT},
	}
	var actions []DiffAction
	for _, definition := range definitions {
		id := &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: definition.kind}}
		if definition.want && !definition.have {
			actions = append(actions, DiffAction{Section: "index", Operation: "add", Request: &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledger, Id: id}}}})
		}
		if !definition.want && definition.have {
			actions = append(actions, DiffAction{Section: "index", Operation: "remove", Request: &servicepb.Request{Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{Ledger: ledger, Id: id}}}})
		}
	}
	return actions
}

func diffQueries(ledger string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction
	parse := func(name string, value EditablePreparedQuery) (*commonpb.QueryFilter, commonpb.QueryTarget, error) {
		target := parseQueryTarget(value.Target)
		if value.Filter == "" {
			return nil, target, nil
		}
		filter, err := Parse(value.Filter, target)
		if err != nil {
			return nil, target, fmt.Errorf("prepared query %q filter: %w", name, err)
		}
		return filter, target, nil
	}
	for _, name := range sortedKeys(desired.PreparedQueries) {
		want := desired.PreparedQueries[name]
		have, exists := current.PreparedQueries[name]
		if !exists {
			filter, target, err := parse(name, want)
			if err != nil {
				return nil, err
			}
			actions = append(actions, createQueryAction(ledger, name, target, filter))
			continue
		}
		if have.Target != want.Target {
			filter, target, err := parse(name, want)
			if err != nil {
				return nil, err
			}
			actions = append(actions, deleteQueryAction(ledger, name), createQueryAction(ledger, name, target, filter))
			continue
		}
		if have.Filter != want.Filter {
			filter, _, err := parse(name, want)
			if err != nil {
				return nil, err
			}
			actions = append(actions, DiffAction{Section: "preparedQuery", Operation: "update", Request: &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: ledger, Name: name, Filter: filter}}}})
		}
	}
	for _, name := range sortedKeys(current.PreparedQueries) {
		if _, exists := desired.PreparedQueries[name]; !exists {
			actions = append(actions, deleteQueryAction(ledger, name))
		}
	}
	return actions, nil
}

func createQueryAction(ledger, name string, target commonpb.QueryTarget, filter *commonpb.QueryFilter) DiffAction {
	return DiffAction{Section: "preparedQuery", Operation: "add", Request: &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: ledger, Query: &commonpb.PreparedQuery{Name: name, Target: target, Filter: filter}}}}}
}

func deleteQueryAction(ledger, name string) DiffAction {
	return DiffAction{Section: "preparedQuery", Operation: "remove", Request: &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: ledger, Name: name}}}}
}

func diffNumscripts(ledger string, current, desired *EditableConfig) ([]DiffAction, error) {
	var actions []DiffAction
	for _, name := range sortedKeys(desired.Numscripts) {
		want := desired.Numscripts[name]
		have, exists := current.Numscripts[name]
		if exists && have.Version == want.Version {
			if have.Content != want.Content {
				return nil, fmt.Errorf("numscript %q: content changed but version %q was not bumped; publish the change under a new semver", name, want.Version)
			}
			continue
		}
		wantVersion, err := parseSemver(want.Version)
		if err != nil {
			return nil, fmt.Errorf("numscript %q: %w (a full canonical semver is required, e.g. 1.0.0)", name, err)
		}
		if exists {
			haveVersion, err := parseSemver(have.Version)
			if err != nil {
				return nil, fmt.Errorf("numscript %q: stored version %q is not a canonical semver: %w", name, have.Version, err)
			}
			if wantVersion.compare(haveVersion) <= 0 {
				return nil, fmt.Errorf("numscript %q: desired version %q is not greater than the current greatest %q; publish a higher version to advance latest", name, want.Version, have.Version)
			}
		}
		actions = append(actions, DiffAction{Section: "numscript", Operation: "add", Request: &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{Ledger: ledger, Name: name, Content: want.Content, Version: want.Version}}}})
	}
	for _, name := range sortedKeys(current.Numscripts) {
		if _, exists := desired.Numscripts[name]; !exists {
			return nil, fmt.Errorf("numscript %q exists in the ledger but is absent from the desired config; the append-only library does not support removal", name)
		}
	}
	return actions, nil
}

func parsePersistence(value string) (commonpb.AccountTypePersistence, error) {
	switch strings.ToLower(value) {
	case "", "normal", "account_type_normal":
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL, nil
	case "ephemeral", "account_type_ephemeral":
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, nil
	case "transient", "account_type_transient":
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT, nil
	default:
		return 0, fmt.Errorf("invalid persistence %q: must be NORMAL, EPHEMERAL, or TRANSIENT", value)
	}
}

func formatPersistence(value commonpb.AccountTypePersistence) string {
	switch value {
	case commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL:
		return "ephemeral"
	case commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT:
		return "transient"
	default:
		return ""
	}
}

type version struct{ major, minor, patch uint32 }

func parseSemver(value string) (version, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return version{}, fmt.Errorf("invalid semver %q: expected major.minor.patch", value)
	}
	numbers := [3]uint32{}
	for index, part := range parts {
		if part == "" {
			return version{}, fmt.Errorf("invalid semver %q: empty component", value)
		}
		number, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return version{}, fmt.Errorf("invalid semver %q: %w", value, err)
		}
		numbers[index] = uint32(number)
	}
	parsed := version{numbers[0], numbers[1], numbers[2]}
	canonical := fmt.Sprintf("%d.%d.%d", parsed.major, parsed.minor, parsed.patch)
	if canonical != value {
		return version{}, fmt.Errorf("invalid semver %q: non-canonical form, expected %q", value, canonical)
	}
	return parsed, nil
}
func (value version) compare(other version) int {
	if value.major != other.major {
		if value.major < other.major {
			return -1
		}
		return 1
	}
	if value.minor != other.minor {
		if value.minor < other.minor {
			return -1
		}
		return 1
	}
	if value.patch != other.patch {
		if value.patch < other.patch {
			return -1
		}
		return 1
	}
	return 0
}

func parseQueryTarget(value string) commonpb.QueryTarget {
	switch strings.ToLower(value) {
	case "transactions":
		return commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	case "logs":
		return commonpb.QueryTarget_QUERY_TARGET_LOGS
	default:
		return commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	}
}

func queryTargetString(value commonpb.QueryTarget) string {
	switch value {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return "transactions"
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		return "logs"
	default:
		return "accounts"
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

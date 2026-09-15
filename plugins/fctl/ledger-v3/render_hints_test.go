package ledgerv3

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	ledgerconfig "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3/internal/ledgerconfig"
	"google.golang.org/protobuf/proto"
)

var expectedRenderColumns = map[string][]sdk.TableColumn{
	"ledger.v3.account-types.get":            {{Header: "NAME", Field: "name"}, {Header: "PATTERN", Field: "pattern"}},
	"ledger.v3.account-types.list":           {{Header: "NAME", Field: "name"}, {Header: "PATTERN", Field: "pattern"}},
	"ledger.v3.accounts.analyze":             {{Header: "TOTAL ACCOUNTS", Field: "totalAccounts"}},
	"ledger.v3.accounts.get":                 {{Header: "ADDRESS", Field: "address"}, {Header: "FIRST USAGE", Field: "firstUsage"}, {Header: "UPDATED AT", Field: "updatedAt"}},
	"ledger.v3.accounts.list":                {{Header: "ADDRESS", Field: "address"}, {Header: "FIRST USAGE", Field: "firstUsage"}, {Header: "UPDATED AT", Field: "updatedAt"}},
	"ledger.v3.audit.get":                    {{Header: "SEQUENCE", Field: "sequence"}, {Header: "TIMESTAMP", Field: "timestamp"}, {Header: "PROPOSAL ID", Field: "proposalId"}, {Header: "ORDERS", Field: "orderCount"}},
	"ledger.v3.audit.list":                   {{Header: "SEQUENCE", Field: "sequence"}, {Header: "TIMESTAMP", Field: "timestamp"}, {Header: "PROPOSAL ID", Field: "proposalId"}, {Header: "ORDERS", Field: "orderCount"}},
	"ledger.v3.indexes.list":                 {{Header: "LEDGER", Field: "ledger"}, {Header: "ENCODING VERSION", Field: "forwardEncodingVersion"}},
	"ledger.v3.ledgers.configuration":        {{Header: "ENFORCEMENT", Field: "defaultEnforcementMode"}, {Header: "REFERENCE INDEX", Field: "indexes.reference"}, {Header: "TIMESTAMP INDEX", Field: "indexes.timestamp"}},
	"ledger.v3.ledgers.configuration.export": {{Header: "ENFORCEMENT", Field: "defaultEnforcementMode"}, {Header: "REFERENCE INDEX", Field: "indexes.reference"}, {Header: "TIMESTAMP INDEX", Field: "indexes.timestamp"}},
	"ledger.v3.ledgers.create":               {{Header: "NAME", Field: "name"}, {Header: "CREATED AT", Field: "createdAt"}},
	"ledger.v3.ledgers.delete":               {{Header: "NAME", Field: "name"}, {Header: "DELETED AT", Field: "deletedAt"}},
	"ledger.v3.ledgers.get":                  {{Header: "NAME", Field: "name"}, {Header: "CREATED AT", Field: "createdAt"}},
	"ledger.v3.ledgers.list":                 {{Header: "NAME", Field: "name"}, {Header: "CREATED AT", Field: "createdAt"}},
	"ledger.v3.ledgers.stats":                {{Header: "TRANSACTIONS", Field: "transactionCount"}, {Header: "POSTINGS", Field: "postingCount"}, {Header: "LOGS", Field: "logCount"}},
	"ledger.v3.logs.get":                     {{Header: "SEQUENCE", Field: "sequence"}},
	"ledger.v3.logs.list":                    {{Header: "SEQUENCE", Field: "sequence"}},
	"ledger.v3.numscripts.get":               {{Header: "NAME", Field: "name"}, {Header: "VERSION", Field: "version"}, {Header: "LEDGER", Field: "ledger"}, {Header: "CREATED AT", Field: "createdAt"}},
	"ledger.v3.numscripts.list":              {{Header: "NAME", Field: "name"}, {Header: "VERSION", Field: "version"}, {Header: "LEDGER", Field: "ledger"}, {Header: "CREATED AT", Field: "createdAt"}},
	"ledger.v3.numscripts.versions":          {{Header: "LATEST VERSION", Field: "latestVersion"}},
	"ledger.v3.queries.list":                 {{Header: "NAME", Field: "name"}, {Header: "TARGET", Field: "target"}},
	"ledger.v3.transactions.analyze":         {{Header: "TRANSACTIONS", Field: "totalTransactions"}, {Header: "REVERTED", Field: "totalReverted"}},
	"ledger.v3.transactions.create":          {{Header: "ID", Field: "transaction.id"}, {Header: "REFERENCE", Field: "transaction.reference"}, {Header: "TIMESTAMP", Field: "transaction.timestamp"}},
	"ledger.v3.transactions.get":             {{Header: "ID", Field: "transaction.id"}, {Header: "REFERENCE", Field: "transaction.reference"}, {Header: "TIMESTAMP", Field: "transaction.timestamp"}, {Header: "REVERTED", Field: "transaction.reverted"}},
	"ledger.v3.transactions.list":            {{Header: "ID", Field: "id"}, {Header: "REFERENCE", Field: "reference"}, {Header: "TIMESTAMP", Field: "timestamp"}, {Header: "REVERTED", Field: "reverted"}},
	"ledger.v3.transactions.revert":          {{Header: "REVERTED ID", Field: "revertedTransactionId"}, {Header: "REVERT ID", Field: "revertTransaction.id"}, {Header: "TIMESTAMP", Field: "revertTransaction.timestamp"}},
}

var expectedCommandsWithoutTable = map[string]string{
	"ledger.v3.account-types.add":                     "successful mutation emits an empty result",
	"ledger.v3.account-types.remove":                  "successful mutation emits an empty result",
	"ledger.v3.account-types.set-default-enforcement": "successful mutation emits an empty result",
	"ledger.v3.accounts.aggregate-volumes":            "result contains only volume collections, not a stable scalar row",
	"ledger.v3.accounts.delete-metadata":              "successful mutation emits an empty result",
	"ledger.v3.accounts.set-metadata":                 "successful mutation emits an empty result",
	"ledger.v3.indexes.create":                        "successful mutation emits an empty result",
	"ledger.v3.indexes.drop":                          "successful mutation emits an empty result",
	"ledger.v3.indexes.inspect":                       "result shape depends on the selected inspection mode",
	"ledger.v3.ledgers.configuration.apply":           "success may be empty, an action plan, or an apply response",
	"ledger.v3.ledgers.delete-metadata":               "successful mutation emits an empty result",
	"ledger.v3.ledgers.get-schema":                    "result contains only metadata-field maps, not a stable scalar row",
	"ledger.v3.ledgers.remove-metadata-type":          "successful mutation emits an empty result",
	"ledger.v3.ledgers.set-metadata":                  "successful mutation emits an empty result",
	"ledger.v3.ledgers.set-metadata-type":             "successful mutation emits an empty result",
	"ledger.v3.numscripts.save":                       "successful apply response has no stable scalar result",
	"ledger.v3.queries.create":                        "successful mutation emits an empty result",
	"ledger.v3.queries.delete":                        "successful mutation emits an empty result",
	"ledger.v3.queries.execute":                       "rows vary between accounts, transactions, logs, and aggregates",
	"ledger.v3.queries.update":                        "successful mutation emits an empty result",
	"ledger.v3.transactions.delete-metadata":          "successful mutation emits an empty result",
	"ledger.v3.transactions.set-metadata":             "successful mutation emits an empty result",
}

func TestRenderHintsPartitionAndPinTheWholeCatalogue(t *testing.T) {
	t.Parallel()
	commands := (Plugin{}).Commands()
	if got, want := len(commands), 48; got != want {
		t.Fatalf("commands = %d, want %d", got, want)
	}
	if got, want := len(expectedRenderColumns), 26; got != want {
		t.Fatalf("hinted = %d, want %d", got, want)
	}
	if got, want := len(expectedCommandsWithoutTable), 22; got != want {
		t.Fatalf("omitted = %d, want %d", got, want)
	}
	seen := map[string]bool{}
	for _, command := range commands {
		columns, hinted := expectedRenderColumns[command.ID]
		reason, omitted := expectedCommandsWithoutTable[command.ID]
		if hinted == omitted {
			t.Errorf("%s must belong to exactly one render partition", command.ID)
			continue
		}
		if omitted && strings.TrimSpace(reason) == "" {
			t.Errorf("%s has an empty omission reason", command.ID)
		}
		if hinted {
			if command.Render.Table == nil || !reflect.DeepEqual(command.Render.Table.Columns, columns) {
				t.Errorf("%s columns = %#v, want %#v", command.ID, command.Render.Table, columns)
			}
		} else if command.Render.Table != nil {
			t.Errorf("%s unexpectedly has table hints: %#v", command.ID, command.Render.Table.Columns)
		}
		seen[command.ID] = true
	}
	for id := range expectedRenderColumns {
		if !seen[id] {
			t.Errorf("hinted command %s is not in catalogue", id)
		}
	}
	for id := range expectedCommandsWithoutTable {
		if !seen[id] {
			t.Errorf("omitted command %s is not in catalogue", id)
		}
	}
}

func TestTableColumnsResolveToStableScalarsInTypedSuccessOutputs(t *testing.T) {
	t.Parallel()
	for id, columns := range expectedRenderColumns {
		id, columns := id, columns
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			row := representativeSuccessRow(t, id)
			for _, column := range columns {
				value, ok := resolveField(row, column.Field)
				if !ok {
					t.Errorf("column %q does not resolve in typed output %#v", column.Field, row)
					continue
				}
				switch value.(type) {
				case string, float64, bool, nil:
				default:
					t.Errorf("column %q resolves to non-scalar %T", column.Field, value)
				}
			}
		})
	}
}

func TestTableColumnsAreDeclaredByNonVacuousPublicSchemas(t *testing.T) {
	t.Parallel()
	for _, command := range (Plugin{}).Commands() {
		columns, hinted := expectedRenderColumns[command.ID]
		if !hinted {
			continue
		}
		if !bytes.Equal(command.RawOutputSchema, command.PublicOutputSchema) {
			t.Errorf("%s raw and public schemas diverge", command.ID)
		}
		for _, column := range columns {
			if !schemaDeclaresScalarPath(t, command.PublicOutputSchema, catalogueSpecCollection(command.ID), column.Field) {
				t.Errorf("%s schema does not declare scalar path %q: %s", command.ID, column.Field, command.PublicOutputSchema)
			}
		}
	}
}

func catalogueSpecCollection(id string) bool {
	for _, item := range catalogue() {
		if item.id() == id {
			return item.collection
		}
	}
	return false
}

func representativeSuccessRow(t *testing.T, id string) map[string]any {
	t.Helper()
	timestamp := &commonpb.Timestamp{Data: 1_000_000}
	var value any
	switch id {
	case "ledger.v3.account-types.get", "ledger.v3.account-types.list":
		value = &commonpb.AccountType{Name: "users", Pattern: "users:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL}
	case "ledger.v3.accounts.analyze":
		value = &analyzeAccountsJSON{TotalAccounts: 4, Patterns: []*accountPatternJSON{}}
	case "ledger.v3.accounts.get", "ledger.v3.accounts.list":
		value = &commonpb.Account{Address: "users:1", FirstUsage: timestamp, UpdatedAt: timestamp}
	case "ledger.v3.audit.get", "ledger.v3.audit.list":
		value = &auditpb.AuditEntry{Sequence: 7, Timestamp: timestamp, ProposalId: 8, OrderCount: 2}
	case "ledger.v3.indexes.list":
		value = &commonpb.Index{Ledger: "main", CreatedAt: timestamp, ForwardEncodingVersion: 3}
	case "ledger.v3.ledgers.configuration", "ledger.v3.ledgers.configuration.export":
		value = &ledgerconfig.EditableConfig{DefaultEnforcementMode: "strict", Indexes: ledgerconfig.EditableIndexes{Reference: true, Timestamp: true}}
	case "ledger.v3.ledgers.create", "ledger.v3.ledgers.get", "ledger.v3.ledgers.list":
		value = &commonpb.LedgerInfo{Id: 1, Name: "main", Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL, CreatedAt: timestamp}
	case "ledger.v3.ledgers.delete":
		value = &commonpb.DeletedLedgerLog{Name: "main", DeletedAt: timestamp}
	case "ledger.v3.ledgers.stats":
		value = &commonpb.LedgerStats{TransactionCount: 3, PostingCount: 4, LogCount: 5}
	case "ledger.v3.logs.get", "ledger.v3.logs.list":
		value = &commonpb.Log{Sequence: 9}
	case "ledger.v3.numscripts.get", "ledger.v3.numscripts.list":
		value = &commonpb.NumscriptInfo{Name: "pay", Version: "1.0.0", Ledger: "main", CreatedAt: timestamp}
	case "ledger.v3.numscripts.versions":
		value = &servicepb.ListNumscriptVersionsResponse{LatestVersion: "1.0.0"}
	case "ledger.v3.queries.list":
		value = &commonpb.PreparedQuery{Name: "active", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS}
	case "ledger.v3.transactions.analyze":
		value = &analyzeTransactionsJSON{TotalTransactions: 9, TotalReverted: 2, FlowPatterns: []*flowPatternJSON{}}
	case "ledger.v3.transactions.create":
		value = &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1, Reference: "ref", Timestamp: timestamp}}
	case "ledger.v3.transactions.get":
		value = &servicepb.GetTransactionResponse{Transaction: &commonpb.Transaction{Id: 1, Reference: "ref", Timestamp: timestamp, Reverted: true}}
	case "ledger.v3.transactions.list":
		value = &commonpb.Transaction{Id: 1, Reference: "ref", Timestamp: timestamp, Reverted: true}
	case "ledger.v3.transactions.revert":
		value = &commonpb.RevertedTransaction{RevertedTransactionId: 1, RevertTransaction: &commonpb.Transaction{Id: 2, Timestamp: timestamp}}
	default:
		t.Fatalf("no typed representative output for %s", id)
	}
	encoded, err := marshalTypedResult(value)
	if err != nil {
		t.Fatalf("marshal typed result: %v", err)
	}
	var row map[string]any
	if err := json.Unmarshal(encoded, &row); err != nil {
		t.Fatalf("decode typed result %s: %v", encoded, err)
	}
	return row
}

func marshalTypedResult(value any) ([]byte, error) {
	if message, ok := value.(proto.Message); ok {
		return marshalProductProto(message)
	}
	return json.Marshal(value)
}

func resolveField(row map[string]any, field string) (any, bool) {
	var current any = row
	for _, segment := range strings.Split(field, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func schemaDeclaresScalarPath(t *testing.T, encoded []byte, collection bool, field string) bool {
	t.Helper()
	var schema map[string]any
	if err := json.Unmarshal(encoded, &schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	current := schema
	if collection {
		items, ok := current["items"].(map[string]any)
		if !ok {
			return false
		}
		current = items
	}
	segments := strings.Split(field, ".")
	for index, segment := range segments {
		properties, ok := current["properties"].(map[string]any)
		if !ok {
			return false
		}
		next, ok := properties[segment].(map[string]any)
		if !ok {
			return false
		}
		if index == len(segments)-1 {
			types, ok := next["type"].([]any)
			if !ok {
				return false
			}
			allowed := map[string]bool{}
			for _, item := range types {
				if text, ok := item.(string); ok {
					allowed[text] = true
				}
			}
			return allowed["string"] && allowed["number"] && allowed["boolean"] && allowed["null"]
		}
		current = next
	}
	return false
}

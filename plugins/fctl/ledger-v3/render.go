package ledgerv3

import (
	"encoding/json"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// tableColumns is the deliberately compact human view of commands whose
// successful result has stable scalar identity or summary fields. The JSON and
// YAML result remains exhaustive; these hints are presentation metadata only.
// Dot-separated fields traverse nested result objects.
var tableColumns = map[string][]sdk.TableColumn{
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

func renderHintFor(commandID string) sdk.RenderHints {
	columns, ok := tableColumns[commandID]
	if !ok {
		return sdk.RenderHints{}
	}
	return sdk.RenderHints{Table: &sdk.TableRenderHint{Columns: append([]sdk.TableColumn(nil), columns...)}}
}

// outputSchemaFor keeps the complete permissive product document while naming
// every scalar leaf that the table renderer reads. This turns schema/hint
// coherence into a real contract without narrowing additional product fields.
func outputSchemaFor(collection bool, columns []sdk.TableColumn) []byte {
	if len(columns) == 0 {
		if collection {
			return collectionSchema
		}
		return objectSchema
	}
	root := map[string]any{"$schema": schemaDialect, "type": "object", "properties": map[string]any{}}
	object := root
	if collection {
		items := map[string]any{"type": "object", "properties": map[string]any{}}
		root = map[string]any{"$schema": schemaDialect, "type": "array", "items": items}
		object = items
	}
	for _, column := range columns {
		current := object
		segments := strings.Split(column.Field, ".")
		for index, segment := range segments {
			properties := current["properties"].(map[string]any)
			if index == len(segments)-1 {
				properties[segment] = map[string]any{"type": []string{"string", "number", "boolean", "null"}}
				continue
			}
			next, ok := properties[segment].(map[string]any)
			if !ok {
				next = map[string]any{"type": "object", "properties": map[string]any{}}
				properties[segment] = next
			}
			current = next
		}
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		panic("ledger v3: encode static output schema: " + err.Error())
	}
	return encoded
}

// ledgerInfoOutputSchema closes the public product shape at its root and along
// every mirror credential-bearing branch. Other rich Ledger subdocuments keep
// their product-defined contents, while clientSecret and dsn are deliberately
// absent from the only locations where LedgerInfo can carry them.
func ledgerInfoOutputSchema(collection bool) []byte {
	credentials := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"clientId":      map[string]any{"type": "string"},
			"tokenEndpoint": map[string]any{"type": "string"},
			"scopes":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
		"additionalProperties": false,
	}
	http := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"baseUrl":                 map[string]any{"type": "string"},
			"oauth2ClientCredentials": credentials,
		},
		"additionalProperties": false,
	}
	awsIAM := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"region":        map[string]any{"type": "string"},
			"assumeRoleArn": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
	}
	postgres := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"awsIamAuth": awsIAM},
		"additionalProperties": false,
	}
	mirrorSource := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"ledgerName":   map[string]any{"type": "string"},
			"http":         http,
			"postgres":     postgres,
			"batchSize":    map[string]any{"type": "integer"},
			"rewriteRules": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		},
		"additionalProperties": false,
	}
	item := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name":                   map[string]any{"type": []string{"string", "number", "boolean", "null"}},
			"createdAt":              map[string]any{"type": []string{"string", "number", "boolean", "null"}},
			"deletedAt":              map[string]any{},
			"metadataSchema":         map[string]any{"type": "object"},
			"mode":                   map[string]any{"type": "string"},
			"mirrorSource":           mirrorSource,
			"mirrorSyncProgress":     map[string]any{"type": "object"},
			"accountTypes":           map[string]any{"type": "object"},
			"defaultEnforcementMode": map[string]any{"type": "string"},
			"metadata":               map[string]any{"type": "object"},
			"id":                     map[string]any{"type": "integer"},
		},
		"additionalProperties": false,
	}
	root := item
	if collection {
		root = map[string]any{"type": "array", "items": item}
	}
	root["$schema"] = schemaDialect
	encoded, err := json.Marshal(root)
	if err != nil {
		panic("ledger v3: encode LedgerInfo output schema: " + err.Error())
	}
	return encoded
}

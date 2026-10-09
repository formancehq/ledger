package ledger

import (
	"net/http"
	"strings"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func ledgerInput(op operation) pluginsdk.InputSpec {
	input := pluginsdk.InputSpec{Title: "Ledger", Kind: "select", Flag: "ledger", ValueType: "string", Required: true,
		Source: &pluginsdk.ChoiceSource{CommandPath: []string{"ledger", "list"}, ValueField: "name", LabelFields: []string{"name"}, EmptyMessage: "No ledgers are available; create a ledger first"}}
	if op.ledgerArg {
		input.AlternativeArgument = new(0)
		if op.method == http.MethodPost {
			input.Title, input.Kind, input.Source = "Ledger name", "input", nil
		}
	}

	return input
}

func operationInputs(path []string, op operation) []pluginsdk.InputSpec {
	inputs := argumentInputs(path, op)
	switch strings.Join(path, " ") {
	case "ledger create":
		inputs = append(inputs, ledgerCreateInputs()...)
	case "ledger transactions create":
		inputs = append(inputs,
			pluginsdk.InputSpec{Title: "Numscript", Description: "Adapt the example's amount, asset and destination before submitting. Use --data for postings or scriptReference.", Kind: "text", BodyPointer: "/script/plain", ValueType: "string", Required: true,
				Default: "send [USD/2 100] (\n  source = @world\n  destination = @users:001\n)"},
			pluginsdk.InputSpec{Title: "Reference", Description: "Optional unique transaction reference", Kind: "input", BodyPointer: "/reference", ValueType: "string"},
			pluginsdk.InputSpec{Title: "Metadata", Description: `Optional JSON object, e.g. {"order_id":"order-42"}; keep integer values exact`, Kind: "text", BodyPointer: "/metadata", ValueType: "json"},
			pluginsdk.InputSpec{Title: "Force execution", Description: "Bypass balance checks; this can overdraw accounts", Kind: "confirm", BodyPointer: "/force", ValueType: "bool", Default: "false"},
		)
	case "ledger indexes create":
		// Index list IDs are protobuf objects, not canonical strings. Creating
		// an index must also permit IDs that do not exist yet.
		inputs = append(inputs, pluginsdk.InputSpec{Title: "Canonical index ID", Description: indexIDHelp, Kind: "input", BodyPointer: "/id", ValueType: "string", Required: true, Default: "log_builtin:LOG_BUILTIN_INDEX_DATE"})
	case "ledger metadata set", "ledger accounts metadata set", "ledger transactions metadata set":
		inputs = append(inputs, pluginsdk.InputSpec{Title: "Metadata JSON", Description: `JSON object to merge, e.g. {"purpose":"automation"}; existing keys are preserved`, Kind: "text", Flag: "data", ValueType: "json", Required: true, Default: `{"purpose":"automation"}`})
	case "ledger bulk":
		inputs = append(inputs, pluginsdk.InputSpec{Title: "Bulk operations JSON", Description: "JSON array of v3 operations; each operation may provide its own ik", Kind: "text", Flag: "data", ValueType: "json", Required: true,
			Default: `[{"action":"CREATE_TRANSACTION","ik":"payment-42","data":{"postings":[{"source":"world","destination":"users:001","asset":"USD/2","amount":100}]}}]`})
	}

	return inputs
}

func ledgerCreateInputs() []pluginsdk.InputSpec {
	return []pluginsdk.InputSpec{
		{Title: "Account type enforcement", Description: "STRICT rejects transactions that violate account types; AUDIT records violations and permits the transaction", Kind: "select", BodyPointer: "/defaultEnforcementMode", ValueType: "string", Default: "STRICT",
			Options: []pluginsdk.InputOption{{Label: "Strict (server default)", Value: "STRICT"}, {Label: "Audit", Value: "AUDIT"}}},
		{Title: "Ledger metadata", Description: `Optional JSON object with string, integer or boolean values. Creation metadata requires Ledger 3.0.0-beta.10 or later; on earlier versions use ledger metadata set after creation.`, Kind: "text", BodyPointer: "/metadata", ValueType: "json"},
		{Title: "Initial metadata schema", Description: `Optional JSON array of field declarations, e.g. [{"targetType":"account","key":"color","type":"string"}]; targets: account, transaction or ledger`, Kind: "text", BodyPointer: "/initialSchema", ValueType: "json"},
		{Title: "Account types", Description: `Optional JSON object of account type models, e.g. {"user-checking":{"name":"user-checking","pattern":"users:{id}:checking","persistence":"EPHEMERAL","segmentTypes":{"id":{"type":"uint64"}}}}`, Kind: "text", BodyPointer: "/accountTypes", ValueType: "json"},
	}
}

func argumentInputs(path []string, op operation) []pluginsdk.InputSpec {
	if op.ledgerArg || op.args == 0 {
		return nil
	}
	if path[1] == "accounts" || path[1] == "transactions" {
		input := resourceInput(path[1])
		inputs := []pluginsdk.InputSpec{input}
		if path[len(path)-2] == "metadata" && path[len(path)-1] == "delete" {
			inputs = append(inputs, metadataKeyInput(1))
		}

		return inputs
	}
	if path[1] == "metadata" {
		return []pluginsdk.InputSpec{metadataKeyInput(0)}
	}
	if path[1] == "indexes" {
		return []pluginsdk.InputSpec{{Title: "Canonical index ID", Description: indexIDHelp + " Use ledger indexes list to inspect registered IDs.", Kind: "input", Argument: new(0), ValueType: "string", Required: true}}
	}

	return nil
}

func resourceInput(collection string) pluginsdk.InputSpec {
	input := pluginsdk.InputSpec{Title: "Account address", Kind: "select", Argument: new(0), ValueType: "string", Required: true,
		Source: &pluginsdk.ChoiceSource{CommandPath: []string{"ledger", collection, "list"}, Flags: map[string]string{"ledger": "$ledger"}, ValueField: "address", AfterField: "address", LabelFields: []string{"address"}, EmptyMessage: "No accounts are available in this ledger"}}
	if collection == "transactions" {
		input.Title = "Transaction ID"
		// IDs are unsigned integers, but the argument is an exact decimal
		// string. The host must keep JSON numbers as json.Number.
		input.Source.ValueField = "id"
		input.Source.AfterField = "id"
		input.Source.LabelFields = []string{"id", "reference"}
		input.Source.EmptyMessage = "No transactions are available in this ledger"
	}

	return input
}

func metadataKeyInput(argument int) pluginsdk.InputSpec {
	return pluginsdk.InputSpec{Title: "Metadata key", Description: "Exact key to delete", Kind: "input", Argument: new(argument), ValueType: "string", Required: true}
}

const indexIDHelp = "Canonical ID, e.g. log_builtin:LOG_BUILTIN_INDEX_DATE, tx_builtin:TX_BUILTIN_INDEX_REFERENCE, account_builtin:ACCT_BUILTIN_INDEX_ASSET, metadata:TARGET_TYPE_ACCOUNT:color or metadata:TARGET_TYPE_TRANSACTION:order_id. Metadata fields must have a declared schema type."

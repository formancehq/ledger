// Package ledger implements the Ledger v3 plugin without CLI or configuration dependencies.
// Routes follow release/v3.0 at 0f4656d1efbcac42705839daccb34012e70d783a.
package ledger

import (
	"errors"
	"net/http"
	"strings"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	httpclient "github.com/formancehq/fctl/pkg/pluginsdk/httpclient"
)

type layout struct{}

type node struct {
	spec     pluginsdk.CommandSpec
	op       *operation
	children []*node
}

func group(use, short string) *node {
	return &node{spec: pluginsdk.CommandSpec{Use: use, Short: short}}
}

func (n *node) add(children ...*node) { n.children = append(n.children, children...) }

func buildLayout() (pluginsdk.Manifest, map[string]operation) {
	m := layout{}
	root := group("ledger", "Use the Ledger v3 data-plane API")
	root.spec.Target = "stack"
	root.spec.Long = "Use the Ledger v3 data-plane API. Nested commands select a ledger with --ledger.\nLedger, account, transaction and log lists return one page; continue with --cursor\nusing the response's next or previous token, keeping the same filters and order.\n--after remains a compatibility alias for forward account, transaction and log pagination."
	root.spec.Example = "fctl ledger create books\nfctl ledger --ledger books transactions create --data @transaction.json --idempotency-key payment-42"
	root.spec.Flags = []pluginsdk.FlagSpec{
		{Name: "ledger", Type: "string", Persistent: true, Usage: "Ledger name for nested commands"},
		{Name: "consistency", Type: "string", Persistent: true, Usage: "Read consistency: linearizable or stale (server default: linearizable)"},
	}
	root.add(
		m.endpoint(operation{use: "list", short: "List one page of ledgers", global: true, page: true, reverse: true, path: func([]string) []string { return []string{"v3", ""} }}),
		m.endpoint(operation{use: "create [name]", short: "Create a ledger", ledgerArg: true, method: http.MethodPost, body: bodyDefault, validateBody: validateLedgerPayload, idempotency: true}),
		m.endpoint(operation{use: "show [name]", short: "Show a ledger", ledgerArg: true}),
		m.endpoint(operation{use: "delete [name]", short: "Delete a ledger", ledgerArg: true, method: http.MethodDelete, idempotency: true}),
		m.endpoint(operation{use: "stats", short: "Show ledger statistics", path: fixed("stats")}),
		m.endpoint(operation{use: "balances", short: "Aggregate ledger balances and volumes", path: fixed("volumes"), filter: true,
			boolQuery: map[string]string{"use-max-precision": "useMaxPrecision", "collapse-colors": "collapseColors"}, stringQuery: map[string]string{"group-by-prefixes": "groupByPrefixes"}}),
		m.endpoint(operation{use: "info", short: "Show Ledger server version information", global: true, path: func([]string) []string { return []string{"_info"} }}),
		m.accounts(), m.transactions(), m.metadata(nil, false), m.indexes(),
	)
	logs := group("logs", "Read ledger logs (requires the LOG index)")
	logs.add(m.endpoint(operation{use: "list", short: "List one page of ledger logs", path: fixed("logs"), page: true, reverse: true, afterID: true, filter: true, dates: true}))
	root.add(logs)
	bulk := m.endpoint(operation{use: "bulk", short: "Submit v3 bulk operations once", method: http.MethodPost, path: fixed("bulk"), body: bodyRequired, validateBody: validateBulkPayload, idempotency: true, bulk: true,
		boolQuery: map[string]string{"atomic": "atomic", "continue-on-failure": "continueOnFailure"}})
	bulk.spec.Long = "Submit a JSON array of v3 bulk operations once. With --atomic, --idempotency-key identifies the whole batch. Otherwise each element's ik identifies its operation and the header is ignored. Inspect every returned element for business failures, including when --continue-on-failure is enabled. This is not a log import or backup restore."
	root.add(bulk)
	operations := make(map[string]operation)
	spec := publish(root, nil, operations)

	return pluginsdk.Manifest{Name: "ledger", Version: "3.0.0", Service: "ledger", ProtocolVersion: pluginsdk.ProtocolVersion, Root: spec}, operations
}

func (m layout) accounts() *node {
	group := group("accounts", "Read accounts and balances, update metadata")
	group.add(
		m.endpoint(operation{use: "list", short: "List accounts", path: fixed("accounts"), page: true, reverse: true, filter: true}),
		m.endpoint(operation{use: "show <address>", short: "Show an account including volumes and metadata", args: 1, path: resource("accounts"), boolQuery: map[string]string{"collapse-colors": "collapseColors"}}),
		m.endpoint(operation{use: "balances <address>", short: "Show account balances in the full account envelope", args: 1, path: resource("accounts"), boolQuery: map[string]string{"collapse-colors": "collapseColors"}}),
		m.metadata([]string{"accounts"}, false),
	)

	return group
}

func (m layout) transactions() *node {
	group := group("transactions", "Read, create and revert transactions")
	group.add(
		m.endpoint(operation{use: "list", short: "List transactions (newest first by default)", path: fixed("transactions"), page: true, afterID: true, reverse: true, filter: true, dates: true}),
		m.endpoint(operation{use: "show <id>", short: "Show a transaction", args: 1, transactionID: true, path: resource("transactions")}),
		m.endpoint(operation{use: "create", short: "Create a transaction from v3 JSON (postings or Numscript)", method: http.MethodPost, path: fixed("transactions"), body: bodyRequired, validateBody: validateTransactionPayload, idempotency: true}),
		m.endpoint(operation{use: "revert <id>", short: "Revert a transaction", args: 1, transactionID: true, method: http.MethodPost, path: resource("transactions", "revert"), body: bodyOptional, validateBody: validateRevertPayload, idempotency: true}),
		m.metadata([]string{"transactions"}, true),
	)

	return group
}

// Metadata reads use the owning resource's GET route: v3 has no GET /metadata.
func (m layout) metadata(parent []string, transactionID bool) *node {
	group := group("metadata", "Read metadata in the resource envelope, set or delete keys")
	args := 0
	argLabel := ""
	if len(parent) > 0 {
		args = 1
		argLabel = " <address>"
		if transactionID {
			argLabel = " <id>"
		}
	}
	base := func(a []string) []string {
		segments := append([]string{}, parent...)
		if args > 0 {
			segments = append(segments, a[0])
		}

		return segments
	}
	group.add(
		m.endpoint(operation{use: "show" + argLabel, short: "Show the resource including metadata", args: args, transactionID: transactionID, path: base}),
		m.endpoint(operation{use: "set" + argLabel, short: "Merge a JSON metadata object", args: args, transactionID: transactionID, method: http.MethodPost, body: bodyRequired, validateBody: validateMetadataPayload, idempotency: true, path: func(a []string) []string { return append(base(a), "metadata") }}),
		m.endpoint(operation{use: "delete" + argLabel + " <key>", short: "Delete one raw metadata key", args: args + 1, transactionID: transactionID, method: http.MethodDelete, idempotency: true, path: func(a []string) []string { return append(base(a), "metadata", a[args]) }}),
	)

	return group
}

func (m layout) indexes() *node {
	group := group("indexes", "Manage and inspect ledger indexes")
	group.add(
		m.endpoint(operation{use: "list", short: "List ledger indexes", path: fixed("indexes")}),
		m.endpoint(operation{use: "show <canonical-id>", short: "Show an index", args: 1, path: resource("indexes")}),
		m.endpoint(operation{use: "status <canonical-id>", short: "Show index build status", args: 1, path: resource("indexes", "status")}),
		m.endpoint(operation{use: "create", short: "Create an index from JSON, e.g. {\"id\":\"log_builtin:LOG_BUILTIN_INDEX_DATE\"}", method: http.MethodPost, path: fixed("indexes"), body: bodyRequired, validateBody: validateIndexPayload, idempotency: true}),
		m.endpoint(operation{use: "delete <canonical-id>", short: "Drop an index", args: 1, method: http.MethodDelete, path: resource("indexes"), idempotency: true}),
	)
	inspect := m.endpoint(operation{use: "inspect <canonical-id>", short: "Inspect a metadata index", args: 1, path: resource("indexes", "inspect"), inspect: true})
	group.add(inspect)

	return group
}

func fixed(segments ...string) func([]string) []string {
	return func([]string) []string { return segments }
}

func resource(collection string, suffix ...string) func([]string) []string {
	return func(args []string) []string { return append([]string{collection, args[0]}, suffix...) }
}

func requestPath(op operation, args []string, selected string) (string, error) {
	if op.global {
		return httpclient.Path(op.path(args)...), nil
	}
	name := selected
	if op.ledgerArg && len(args) > 0 {
		if name != "" && name != args[0] {
			return "", errors.New("positional ledger name conflicts with --ledger")
		}
		name = args[0]
	}
	if name == "" {
		return "", errors.New("select a ledger with --ledger (create/show/delete also accept a positional name)")
	}
	segments := []string{"v3", name}
	if op.path != nil {
		segments = append(segments, op.path(args)...)
	}

	return httpclient.Path(segments...), nil
}

func publish(n *node, parent []string, operations map[string]operation) pluginsdk.CommandSpec {
	name := strings.Fields(n.spec.Use)[0]
	path := append(append([]string{}, parent...), name)
	spec := n.spec
	if n.op != nil {
		operations[strings.Join(path, "/")] = *n.op
		spec.Inputs = append(spec.Inputs, operationInputs(path, *n.op)...)
	}
	for _, child := range n.children {
		spec.Subcommands = append(spec.Subcommands, publish(child, path, operations))
	}

	return spec
}

package ledgerv2

import (
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// Table render hints are a declaration, not a rendering. The host at the pinned
// SDK revision derives its own table columns from the result document and does
// not read Command.Render, so these columns change no observable CLI output
// today; they travel across the component codec and are validated for
// completeness, and they become the layout the moment a host chooses to honour
// them. See README.md "Table render hints".
//
// A column is published only where the emitted result proves the field exists.
// Nine commands publish the empty object because their operation returns no
// content, and `ledger export` publishes an opaque payload; all ten declare no
// hint rather than a fabricated one.
//
// Every column projects a scalar leaf of the result the command already emits,
// so a hint can never widen what the plugin discloses. Dynamic key/value blobs
// (`metadata`, `features`), volume aggregations (`volumes`, `postCommitVolumes`
// and their siblings), posting arrays and schema charts are deliberately left
// out: they are composites the host would have to serialise back into one cell.
//
// TableColumn.Field is a dot-separated path, so a nested scalar leaf is
// expressible. No column below uses one: every Ledger v2 result reaches its
// nested values only through dynamic keys (an asset code, a metadata key, an
// account address) or through arrays, and neither can be named by a static
// path. Inventing `metadata.<key>` would bind the table to a key the product
// does not guarantee.
var renderHints = map[string][]sdk.TableColumn{
	"ledger.v2.list": {
		{Header: "Name", Field: "name"},
		{Header: "Bucket", Field: "bucket"},
		{Header: "Added At", Field: "addedAt"},
	},
	"ledger.v2.stats": {
		{Header: "Accounts", Field: "accounts"},
		{Header: "Transactions", Field: "transactions"},
	},
	"ledger.v2.accounts.list":       accountColumns(),
	"ledger.v2.accounts.show":       accountColumns(),
	"ledger.v2.transactions.list":   transactionColumns(),
	"ledger.v2.transactions.show":   transactionColumns(),
	"ledger.v2.transactions.num":    transactionColumns(),
	"ledger.v2.transactions.revert": transactionColumns(),
	"ledger.v2.send":                transactionColumns(),
	"ledger.v2.volumes.list": {
		{Header: "Account", Field: "account"},
		{Header: "Asset", Field: "asset"},
		{Header: "Input", Field: "input"},
		{Header: "Output", Field: "output"},
		{Header: "Balance", Field: "balance"},
	},
	"ledger.v2.schemas.list": schemaColumns(),
	"ledger.v2.schemas.get":  schemaColumns(),
}

// accountColumns projects the account identity and the two request-time
// timestamps. `firstUsage` is omitted to keep the row compact, and the volume
// maps are excluded by the composite rule above.
func accountColumns() []sdk.TableColumn {
	return []sdk.TableColumn{
		{Header: "Address", Field: "address"},
		{Header: "Insertion Date", Field: "insertionDate"},
		{Header: "Updated At", Field: "updatedAt"},
	}
}

// transactionColumns is shared by every command that emits one V2Transaction,
// so a single transaction reads the same whether it was listed, shown, sent,
// scripted or reverted.
func transactionColumns() []sdk.TableColumn {
	return []sdk.TableColumn{
		{Header: "ID", Field: "id"},
		{Header: "Timestamp", Field: "timestamp"},
		{Header: "Reference", Field: "reference"},
		{Header: "Reverted", Field: "reverted"},
	}
}

func schemaColumns() []sdk.TableColumn {
	return []sdk.TableColumn{
		{Header: "Version", Field: "version"},
		{Header: "Created At", Field: "createdAt"},
	}
}

// renderHintFor returns the table hint published for one command, or the zero
// value when the command's result carries no field evidence.
func renderHintFor(commandID string) sdk.RenderHints {
	columns, ok := renderHints[commandID]
	if !ok {
		return sdk.RenderHints{}
	}
	return sdk.RenderHints{Table: &sdk.TableRenderHint{Columns: append([]sdk.TableColumn(nil), columns...)}}
}

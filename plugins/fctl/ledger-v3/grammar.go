package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

// Flag and argument names shared across families. They are declared once so a
// rename cannot leave one family behind, and so the execution side can look
// values up by the same constant the descriptor advertises.
const (
	argLedger  = "ledger"
	argName    = "name"
	argKey     = "key"
	argAddress = "address"

	flagPageSize       = "page-size"
	flagCursor         = "cursor"
	flagReverse        = "reverse"
	flagFilter         = "filter"
	flagCheckpointID   = "checkpoint-id"
	flagIdempotencyKey = "idempotency-key"
	flagMetadata       = "metadata"
	flagDryRun         = "dry-run"
)

// defaultPageSize is the descriptor default for every paginated read. The host
// still owns the continuation decision; this only sizes one page.
const defaultPageSize = "50"

const maxPageSize int64 = 1000

func ledgerArgument() sdk.Argument {
	return sdk.Argument{
		Name:     argLedger,
		Usage:    "Ledger name",
		Type:     sdk.ArgumentString,
		Required: true,
	}
}

func requiredStringArgument(name, usage string) sdk.Argument {
	return sdk.Argument{Name: name, Usage: usage, Type: sdk.ArgumentString, Required: true}
}

func repeatedStringArgument(name, usage string) sdk.Argument {
	return sdk.Argument{Name: name, Usage: usage, Type: sdk.ArgumentStringArray}
}

func stringFlag(name, usage string) sdk.Flag {
	return sdk.Flag{Name: name, Usage: usage, Type: sdk.FlagString}
}

func stringArrayFlag(name, usage string) sdk.Flag {
	return sdk.Flag{Name: name, Usage: usage, Type: sdk.FlagStringArray}
}

func boolFlag(name, usage string) sdk.Flag {
	return sdk.Flag{Name: name, Usage: usage, Type: sdk.FlagBool}
}

// readFlags expose ReadOptions fields. Checkpoint ids are fixed64 on the wire,
// so they are strings: the descriptor grammar has no 64-bit integer type and
// an int32 flag would silently truncate a real checkpoint id.
func readFlags() []sdk.Flag {
	return []sdk.Flag{
		stringFlag(flagCheckpointID, "Read from this query checkpoint id (unsigned 64-bit decimal)"),
	}
}

// listFlags are the ListOptions fields plus the read options. filterTarget
// documents which query target the filter expression is parsed against.
func listFlags(filterUsage string) []sdk.Flag {
	flags := []sdk.Flag{
		{Name: flagPageSize, Usage: "Maximum items per page", Type: sdk.FlagInt32, HasDefault: true, DefaultValue: defaultPageSize},
		stringFlag(flagCursor, "Opaque cursor returned by a previous page"),
		boolFlag(flagReverse, "Walk the collection in reverse order"),
		stringFlag(flagFilter, filterUsage),
	}
	return append(flags, readFlags()...)
}

// listFlagsWithoutFilter describes ListOptions endpoints whose server-side
// validation deliberately rejects filters while still accepting read options.
func listFlagsWithoutFilter() []sdk.Flag {
	flags := []sdk.Flag{
		{Name: flagPageSize, Usage: "Maximum items per page", Type: sdk.FlagInt32, HasDefault: true, DefaultValue: defaultPageSize},
		stringFlag(flagCursor, "Opaque cursor returned by a previous page"),
		boolFlag(flagReverse, "Walk the collection in reverse order"),
	}
	return append(flags, readFlags()...)
}

// listFlagsWithoutReverse describes filtered endpoints whose server contract
// has no reverse traversal.
func listFlagsWithoutReverse(filterUsage string) []sdk.Flag {
	flags := []sdk.Flag{
		{Name: flagPageSize, Usage: "Maximum items per page", Type: sdk.FlagInt32, HasDefault: true, DefaultValue: defaultPageSize},
		stringFlag(flagCursor, "Opaque cursor returned by a previous page"),
		stringFlag(flagFilter, filterUsage),
	}
	return append(flags, readFlags()...)
}

// writeFlags are shared by every command that composes an ApplyBatch.
func writeFlags() []sdk.Flag {
	return []sdk.Flag{
		stringFlag(flagIdempotencyKey, "Idempotency key carried inside the signed ApplyBatch"),
	}
}

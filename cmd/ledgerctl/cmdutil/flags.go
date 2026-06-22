package cmdutil

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NextCursorTrailerKey is the gRPC trailer key that carries the opaque
// resume token for the next page of a streaming list response. Mirrors
// the constant defined in internal/adapter/grpc/stream_helper.go.
const NextCursorTrailerKey = "x-next-cursor"

// NextCursorFromTrailer returns the opaque cursor for the following page, or
// "" when the server signaled end-of-stream (no trailer).
func NextCursorFromTrailer(trailer metadata.MD) string {
	if vals := trailer.Get(NextCursorTrailerKey); len(vals) > 0 {
		return vals[0]
	}

	return ""
}

// FetchPager is the signature accepted by DrainAllPages and FetchSinglePageOrAll:
// it builds and drains one streaming RPC, returning the page items plus the
// gRPC trailer that may carry the x-next-cursor token.
type FetchPager[T any] func(cursor string) (items []T, trailer metadata.MD, err error)

// FetchSinglePageOrAll branches on whether the user explicitly set --page-size:
//
//   - when --page-size or --cursor is set, fetch exactly one page starting
//     at initialCursor and return its x-next-cursor (if any) so the caller
//     can hint resumption to the user. Setting either flag is the canonical
//     way to opt into pagination — including following the "resume with
//     --cursor X" hint printed after the first page.
//   - when neither flag is set, drain every page via DrainAllPages so the
//     historical "show everything" UX is preserved.
//
// Use this on CLI list commands that historically streamed an unbounded
// collection (ledgers, numscripts, signing keys).
func FetchSinglePageOrAll[T any](
	cmd *cobra.Command,
	initialCursor string,
	fetchPage FetchPager[T],
) (items []T, nextCursor string, err error) {
	if cmd.Flags().Changed("page-size") || cmd.Flags().Changed("cursor") {
		var trailer metadata.MD

		items, trailer, err = fetchPage(initialCursor)
		if err != nil {
			return nil, "", err
		}

		return items, NextCursorFromTrailer(trailer), nil
	}

	items, err = DrainAllPages(initialCursor, fetchPage)

	return items, "", err
}

// DrainAllPages follows the x-next-cursor trailer chain across pages of a
// paginated streaming RPC and returns every item.
//
// initialCursor seeds the first iteration: pass the user-supplied --cursor
// value to resume mid-stream, or "" to start at the head. fetchPage receives
// the cursor for the current iteration and returns the page's items plus the
// trailer that carries the next-page cursor. Returns when fetchPage's
// trailer has no x-next-cursor.
func DrainAllPages[T any](
	initialCursor string,
	fetchPage FetchPager[T],
) ([]T, error) {
	var all []T

	cursor := initialCursor

	for {
		items, trailer, err := fetchPage(cursor)
		if err != nil {
			return nil, err
		}

		all = append(all, items...)

		next := NextCursorFromTrailer(trailer)
		if next == "" {
			return all, nil
		}

		cursor = next
	}
}

// BuildListOptions packages the pagination + filter + consistency flag values
// into the canonical commonpb.ListOptions every streaming list RPC consumes.
// Pass filter == nil for endpoints that don't expose --filter / --prefix.
// Returns nil when no field is set so handlers see a clean "default
// everything" signal on the wire.
func BuildListOptions(pgn PaginationFlags, cns ConsistencyFlags, filter *commonpb.QueryFilter) *commonpb.ListOptions {
	if pgn.PageSize == 0 && pgn.Cursor == "" && !pgn.Reverse && cns.CheckpointID == 0 && cns.MinLogSequence == 0 && filter == nil {
		return nil
	}

	return &commonpb.ListOptions{
		PageSize: pgn.PageSize,
		Cursor:   pgn.Cursor,
		Reverse:  pgn.Reverse,
		Read:     buildReadOptions(cns),
		Filter:   filter,
	}
}

// BuildReadOptions packages just the consistency flags into a commonpb.ReadOptions
// for Get* RPCs that do not paginate. Returns nil when both fields are zero.
func BuildReadOptions(cns ConsistencyFlags) *commonpb.ReadOptions {
	return buildReadOptions(cns)
}

func buildReadOptions(cns ConsistencyFlags) *commonpb.ReadOptions {
	if cns.CheckpointID == 0 && cns.MinLogSequence == 0 {
		return nil
	}

	return &commonpb.ReadOptions{
		CheckpointId:   cns.CheckpointID,
		MinLogSequence: cns.MinLogSequence,
	}
}

// PaginationOptions configures AddPaginationFlags per resource.
type PaginationOptions struct {
	// DefaultPageSize is the value used when --page-size is not provided.
	// Falls back to DefaultPageSize (10) if zero.
	DefaultPageSize uint32
	// SupportsReverse exposes --reverse when true.
	SupportsReverse bool
	// SupportsAll exposes --all (fetch everything, ignoring pagination) when true.
	SupportsAll bool
}

// AddPaginationFlags registers the canonical pagination flags on cmd:
//
//	--page-size  (uint32) — number of items per page
//	--cursor     (string) — opaque cursor returned by the previous page
//	--reverse    (bool)   — reverse iteration order (only if SupportsReverse)
//	--all        (bool)   — fetch every page (only if SupportsAll)
//
// --cursor is opaque to the client: the server publishes its value as the
// x-next-cursor gRPC trailer of the previous page, and the client passes it
// back unchanged. This lets the server evolve its on-wire cursor format
// without coordinating with deployed clients.
func AddPaginationFlags(cmd *cobra.Command, opts PaginationOptions) {
	pageSize := opts.DefaultPageSize
	if pageSize == 0 {
		pageSize = DefaultPageSize
	}

	cmd.Flags().Uint32("page-size", pageSize, "Number of items per page")
	cmd.Flags().String("cursor", "", "Opaque resume token from the x-next-cursor trailer of the previous page")

	if opts.SupportsReverse {
		cmd.Flags().Bool("reverse", false, "Reverse iteration order")
	}

	if opts.SupportsAll {
		cmd.Flags().Bool("all", false, "Fetch all pages at once (no interactive pagination)")
	}
}

// PaginationFlags carries the parsed values from AddPaginationFlags.
type PaginationFlags struct {
	PageSize uint32
	Cursor   string
	Reverse  bool
	All      bool
}

// GetPaginationFlags reads the pagination flags registered by AddPaginationFlags.
// Missing flags (e.g. --cursor on a resource that does not expose it) read as
// their zero value.
func GetPaginationFlags(cmd *cobra.Command) PaginationFlags {
	pageSize, _ := cmd.Flags().GetUint32("page-size")
	cursor, _ := cmd.Flags().GetString("cursor")
	reverse, _ := cmd.Flags().GetBool("reverse")
	all, _ := cmd.Flags().GetBool("all")

	return PaginationFlags{
		PageSize: pageSize,
		Cursor:   cursor,
		Reverse:  reverse,
		All:      all,
	}
}

// AddMinLogSequenceFlag registers --min-log-sequence on cmd. Use this on
// endpoints that gate live reads on log-sequence catch-up but do NOT support
// checkpoint reads (audit, chapters, etc.); other callers should use
// AddConsistencyFlags.
func AddMinLogSequenceFlag(cmd *cobra.Command) {
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
}

// AddConsistencyFlags registers --checkpoint-id and --min-log-sequence on cmd.
// Both are uint64 and default to 0 (no constraint).
//
// Commands whose server endpoint reads from the read store should call this;
// raft-state-only commands (ledgers, signing, etc.) should omit it.
func AddConsistencyFlags(cmd *cobra.Command) {
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	AddMinLogSequenceFlag(cmd)
}

// ConsistencyFlags carries the parsed values from AddConsistencyFlags.
type ConsistencyFlags struct {
	CheckpointID   uint64
	MinLogSequence uint64
}

// GetConsistencyFlags reads the consistency flags registered by AddConsistencyFlags.
func GetConsistencyFlags(cmd *cobra.Command) ConsistencyFlags {
	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")

	return ConsistencyFlags{
		CheckpointID:   checkpointID,
		MinLogSequence: minLogSeq,
	}
}

// FilterOptions configures AddFilterFlags per resource.
type FilterOptions struct {
	// SupportsPrefix exposes --prefix as a server-side address-prefix shortcut.
	// Currently only meaningful on accounts (server supports a dedicated
	// HardcodedPrefix on AddressMatch).
	SupportsPrefix bool
	// PrefixHelp overrides the default --prefix help string.
	PrefixHelp string
}

// AddFilterFlags registers --filter (filterexpr DSL) on cmd, plus --prefix when
// SupportsPrefix is set.
func AddFilterFlags(cmd *cobra.Command, opts FilterOptions) {
	cmd.Flags().String("filter", "", `Filter expression (see "filterexpr" grammar; e.g. "metadata[k] == v")`)

	if opts.SupportsPrefix {
		help := opts.PrefixHelp
		if help == "" {
			help = "Filter results by address prefix (e.g. users:)"
		}

		cmd.Flags().String("prefix", "", help)
	}
}

// FilterFlags carries the parsed values from AddFilterFlags.
type FilterFlags struct {
	Expr   string
	Prefix string
}

// GetFilterFlags reads the filter flags registered by AddFilterFlags.
func GetFilterFlags(cmd *cobra.Command) FilterFlags {
	expr, _ := cmd.Flags().GetString("filter")
	prefix, _ := cmd.Flags().GetString("prefix")

	return FilterFlags{Expr: expr, Prefix: prefix}
}

// BuildQueryFilter combines a --filter expression and an optional --prefix into
// a single QueryFilter. Returns nil when both inputs are empty.
//
// The prefix is applied as an AddressMatch_HardcodedPrefix; when --filter is
// also set, the two are AND-combined.
func BuildQueryFilter(filterExpr, prefix string) (*commonpb.QueryFilter, error) {
	var parsed *commonpb.QueryFilter

	if filterExpr != "" {
		var err error

		parsed, err = filterexpr.Parse(filterExpr)
		if err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	var prefixFilter *commonpb.QueryFilter
	if prefix != "" {
		prefixFilter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
				},
			},
		}
	}

	switch {
	case parsed != nil && prefixFilter != nil:
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{prefixFilter, parsed}},
			},
		}, nil
	case parsed != nil:
		return parsed, nil
	case prefixFilter != nil:
		return prefixFilter, nil
	default:
		return nil, nil
	}
}

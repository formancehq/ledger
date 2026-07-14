package cmdutil_test

import (
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAddPaginationFlags(t *testing.T) {
	t.Parallel()

	t.Run("default page size", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{SupportsReverse: true, SupportsAll: true})

		flags := cmdutil.GetPaginationFlags(cmd)
		require.Equal(t, uint32(cmdutil.DefaultPageSize), flags.PageSize)
		require.Empty(t, flags.Cursor)
		require.False(t, flags.Reverse)
		require.False(t, flags.All)
	})

	t.Run("custom page size", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{DefaultPageSize: 25})

		flags := cmdutil.GetPaginationFlags(cmd)
		require.Equal(t, uint32(25), flags.PageSize)
	})

	t.Run("cursor flag always present", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})

		require.NotNil(t, cmd.Flags().Lookup("cursor"))
	})

	t.Run("reverse disabled", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})

		require.Nil(t, cmd.Flags().Lookup("reverse"))
	})

	t.Run("all disabled", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})

		require.Nil(t, cmd.Flags().Lookup("all"))
	})

	t.Run("parses user-supplied values", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{SupportsReverse: true, SupportsAll: true})

		require.NoError(t, cmd.Flags().Set("page-size", "50"))
		require.NoError(t, cmd.Flags().Set("cursor", "abc"))
		require.NoError(t, cmd.Flags().Set("reverse", "true"))
		require.NoError(t, cmd.Flags().Set("all", "true"))

		flags := cmdutil.GetPaginationFlags(cmd)
		require.Equal(t, uint32(50), flags.PageSize)
		require.Equal(t, "abc", flags.Cursor)
		require.True(t, flags.Reverse)
		require.True(t, flags.All)
	})
}

func TestBuildListOptions(t *testing.T) {
	t.Parallel()

	t.Run("all zero returns nil", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, cmdutil.BuildListOptions(cmdutil.PaginationFlags{}, cmdutil.ConsistencyFlags{}, nil))
	})

	t.Run("page size only", func(t *testing.T) {
		t.Parallel()
		opts := cmdutil.BuildListOptions(cmdutil.PaginationFlags{PageSize: 20}, cmdutil.ConsistencyFlags{}, nil)
		require.NotNil(t, opts)
		require.Equal(t, uint32(20), opts.GetPageSize())
		require.Nil(t, opts.GetRead())
	})

	t.Run("with consistency", func(t *testing.T) {
		t.Parallel()
		opts := cmdutil.BuildListOptions(
			cmdutil.PaginationFlags{Cursor: "abc"},
			cmdutil.ConsistencyFlags{CheckpointID: 7, MinLogSequence: 9},
			nil,
		)
		require.NotNil(t, opts)
		require.Equal(t, "abc", opts.GetCursor())
		require.Equal(t, uint64(7), opts.GetRead().GetCheckpointId())
		require.Equal(t, uint64(9), opts.GetRead().GetMinLogSequence())
	})

	t.Run("with filter", func(t *testing.T) {
		t.Parallel()
		f, err := cmdutil.BuildQueryFilter("", "users:", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.NoError(t, err)

		opts := cmdutil.BuildListOptions(cmdutil.PaginationFlags{}, cmdutil.ConsistencyFlags{}, f)
		require.NotNil(t, opts)
		require.NotNil(t, opts.GetFilter())
	})
}

func TestBuildReadOptions(t *testing.T) {
	t.Parallel()

	require.Nil(t, cmdutil.BuildReadOptions(cmdutil.ConsistencyFlags{}))

	opts := cmdutil.BuildReadOptions(cmdutil.ConsistencyFlags{CheckpointID: 3})
	require.NotNil(t, opts)
	require.Equal(t, uint64(3), opts.GetCheckpointId())
}

func TestAddConsistencyFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	cmdutil.AddConsistencyFlags(cmd)

	require.NotNil(t, cmd.Flags().Lookup("checkpoint-id"))
	require.NotNil(t, cmd.Flags().Lookup("min-log-sequence"))

	require.NoError(t, cmd.Flags().Set("checkpoint-id", "7"))
	require.NoError(t, cmd.Flags().Set("min-log-sequence", "12"))

	flags := cmdutil.GetConsistencyFlags(cmd)
	require.Equal(t, uint64(7), flags.CheckpointID)
	require.Equal(t, uint64(12), flags.MinLogSequence)
}

func TestAddFilterFlags(t *testing.T) {
	t.Parallel()

	t.Run("filter only", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{})

		require.NotNil(t, cmd.Flags().Lookup("filter"))
		require.Nil(t, cmd.Flags().Lookup("prefix"))
	})

	t.Run("with prefix", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{SupportsPrefix: true})

		require.NotNil(t, cmd.Flags().Lookup("filter"))
		require.NotNil(t, cmd.Flags().Lookup("prefix"))
	})

	t.Run("reads values", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddFilterFlags(cmd, cmdutil.FilterOptions{SupportsPrefix: true})

		require.NoError(t, cmd.Flags().Set("filter", "metadata[k] == v"))
		require.NoError(t, cmd.Flags().Set("prefix", "users:"))

		flags := cmdutil.GetFilterFlags(cmd)
		require.Equal(t, "metadata[k] == v", flags.Expr)
		require.Equal(t, "users:", flags.Prefix)
	})
}

func TestBuildQueryFilter(t *testing.T) {
	t.Parallel()

	t.Run("empty inputs return nil", func(t *testing.T) {
		t.Parallel()

		f, err := cmdutil.BuildQueryFilter("", "", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.NoError(t, err)
		require.Nil(t, f)
	})

	t.Run("prefix only", func(t *testing.T) {
		t.Parallel()

		f, err := cmdutil.BuildQueryFilter("", "users:", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.NoError(t, err)
		require.NotNil(t, f)

		addr := f.GetAddress()
		require.NotNil(t, addr)
		require.Equal(t, "users:", addr.GetHardcodedPrefix())
	})

	t.Run("filter only", func(t *testing.T) {
		t.Parallel()

		f, err := cmdutil.BuildQueryFilter(`metadata[k] == "v"`, "", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.NoError(t, err)
		require.NotNil(t, f)
		// filterexpr.Parse produces a FieldCondition oneof variant for metadata.
		require.NotNil(t, f.GetField())
	})

	t.Run("filter + prefix combined", func(t *testing.T) {
		t.Parallel()

		f, err := cmdutil.BuildQueryFilter(`metadata[k] == "v"`, "users:", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.NoError(t, err)
		require.NotNil(t, f)
		require.NotNil(t, f.GetAnd())
		require.Len(t, f.GetAnd().GetFilters(), 2)

		// Prefix first, parsed filter second.
		require.Equal(t, "users:", f.GetAnd().GetFilters()[0].GetAddress().GetHardcodedPrefix())
	})

	t.Run("invalid filter expression", func(t *testing.T) {
		t.Parallel()

		_, err := cmdutil.BuildQueryFilter("@@invalid@@", "", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid filter expression")
	})

	// EN-1549: the `ledgerctl audit list` path builds its filter through
	// BuildQueryFilter with QUERY_TARGET_AUDIT, so a bare audit field must resolve
	// to the audit arm. This is the CLI-path regression guard for the audit list
	// command — before threading the target it decoded as TRANSACTIONS and a bare
	// audit-only field (outcome) failed to parse.
	t.Run("bare audit field resolves on the audit target", func(t *testing.T) {
		t.Parallel()

		f, err := cmdutil.BuildQueryFilter("outcome == failure", "", commonpb.QueryTarget_QUERY_TARGET_AUDIT)
		require.NoError(t, err)
		require.NotNil(t, f)

		ac := f.GetAudit()
		require.NotNil(t, ac, "outcome must resolve to the audit arm on the audit target")
		require.Equal(t, commonpb.AuditField_AUDIT_FIELD_OUTCOME, ac.GetField())
		require.Equal(t, "failure", ac.GetStringCond().GetHardcoded())
	})

	// A bare shared field (ledger) must also resolve to the audit arm on the audit
	// target, not the top-level LedgerCondition arm.
	t.Run("bare ledger resolves to the audit arm on the audit target", func(t *testing.T) {
		t.Parallel()

		f, err := cmdutil.BuildQueryFilter("ledger == main", "", commonpb.QueryTarget_QUERY_TARGET_AUDIT)
		require.NoError(t, err)
		require.NotNil(t, f)
		require.NotNil(t, f.GetAudit(), "ledger must resolve to the audit arm on the audit target")
		require.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, f.GetAudit().GetField())
	})
}

// Compile-time sanity: ensure the QueryFilter helpers return the same proto
// shapes the existing transactions/accounts code already produces.
var (
	_ = commonpb.AddressMatch_HardcodedPrefix{}
	_ = commonpb.AndFilter{}
)

// fakePager hands DrainAllPages / FetchSinglePageOrAll a deterministic
// sequence of pages keyed by the incoming cursor string. The test fills
// `pages` with the responses it wants returned, in iteration order, and
// optionally `err` for the slot at which the chain should error out.
type fakePager struct {
	pages [][]int
	next  []string // next cursor for each page (empty string = end of chain)
	err   error
	calls []string // cursors observed, in order
}

func (p *fakePager) fetch(cursor string) ([]int, metadata.MD, error) {
	p.calls = append(p.calls, cursor)

	if p.err != nil && len(p.calls) > 1 {
		return nil, nil, p.err
	}

	idx := len(p.calls) - 1
	if idx >= len(p.pages) {
		return nil, metadata.MD{}, nil
	}

	tr := metadata.MD{}
	if idx < len(p.next) && p.next[idx] != "" {
		tr.Set("x-next-cursor", p.next[idx])
	}

	return p.pages[idx], tr, nil
}

func TestDrainAllPages(t *testing.T) {
	t.Parallel()

	t.Run("follows the trailer chain and stops on empty trailer", func(t *testing.T) {
		t.Parallel()

		p := &fakePager{
			pages: [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8}},
			next:  []string{"after-3", "after-6", ""},
		}

		items, err := cmdutil.DrainAllPages("", p.fetch)
		require.NoError(t, err)
		require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, items)
		require.Equal(t, []string{"", "after-3", "after-6"}, p.calls)
	})

	t.Run("seeds the chain with initialCursor", func(t *testing.T) {
		t.Parallel()

		p := &fakePager{
			pages: [][]int{{10, 11}},
			next:  []string{""},
		}

		_, err := cmdutil.DrainAllPages("start-here", p.fetch)
		require.NoError(t, err)
		require.Equal(t, []string{"start-here"}, p.calls)
	})

	t.Run("propagates a mid-chain error", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("upstream blew up")
		p := &fakePager{
			pages: [][]int{{1, 2, 3}},
			next:  []string{"keep-going"},
			err:   boom,
		}

		_, err := cmdutil.DrainAllPages("", p.fetch)
		require.ErrorIs(t, err, boom)
		// Verifies the loop made the second call (where the error fires),
		// not just bailed on the first.
		require.Len(t, p.calls, 2)
	})
}

func TestFetchSinglePageOrAll(t *testing.T) {
	t.Parallel()

	t.Run("--page-size triggers single-page mode", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})
		require.NoError(t, cmd.Flags().Set("page-size", "5"))

		p := &fakePager{
			pages: [][]int{{1, 2, 3}, {4, 5}},
			next:  []string{"next-cursor", ""},
		}

		items, nextCursor, err := cmdutil.FetchSinglePageOrAll(cmd, "", p.fetch)
		require.NoError(t, err)
		require.Equal(t, []int{1, 2, 3}, items, "single-page mode returns the first page only")
		require.Equal(t, "next-cursor", nextCursor, "surfaces the trailer cursor for the resume hint")
		require.Len(t, p.calls, 1, "single-page mode does not chain")
	})

	t.Run("--cursor also triggers single-page mode", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})
		require.NoError(t, cmd.Flags().Set("cursor", "user-token"))

		p := &fakePager{
			pages: [][]int{{42}},
			next:  []string{""},
		}

		_, _, err := cmdutil.FetchSinglePageOrAll(cmd, "user-token", p.fetch)
		require.NoError(t, err)
		require.Equal(t, []string{"user-token"}, p.calls,
			"resume token reaches the first fetch in single-page mode")
	})

	t.Run("no flag set drains every page", func(t *testing.T) {
		t.Parallel()

		cmd := &cobra.Command{}
		cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{})

		p := &fakePager{
			pages: [][]int{{1, 2}, {3, 4}, {5}},
			next:  []string{"a", "b", ""},
		}

		items, nextCursor, err := cmdutil.FetchSinglePageOrAll(cmd, "", p.fetch)
		require.NoError(t, err)
		require.Equal(t, []int{1, 2, 3, 4, 5}, items, "drain-mode returns every page")
		require.Empty(t, nextCursor, "drain-mode never surfaces a cursor")
		require.Equal(t, []string{"", "a", "b"}, p.calls)
	})
}

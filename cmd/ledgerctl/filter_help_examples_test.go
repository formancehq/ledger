package main

import (
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// filterHelpExample matches a --filter argument in a help example line, quoted
// with single or double quotes the way a shell user would copy it.
var filterHelpExample = regexp.MustCompile(`--filter\s+(?:'([^']*)'|"([^"]*)")`)

var queriesTargetFlag = regexp.MustCompile(`--target\s+(\w+)`)

// filterHelpParser parses one help example the way the command parses its
// --filter flag and returns the query targets the result must be valid on.
type filterHelpParser func(line, expr string) (*commonpb.QueryFilter, []commonpb.QueryTarget, error)

func buildQueryFilterFor(target commonpb.QueryTarget) filterHelpParser {
	return func(_, expr string) (*commonpb.QueryFilter, []commonpb.QueryTarget, error) {
		f, err := cmdutil.BuildQueryFilter(expr, "", target)

		return f, []commonpb.QueryTarget{target}, err
	}
}

// TestFilterHelpExamplesAreValid asserts that every --filter example in the
// help of a filter-taking command parses through the same path the command
// uses and is accepted by the server-side per-target validity gate. Help
// examples are copied verbatim by operators: a stale field ("amount"), an
// unquoted special character ("users:") or a condition the target rejects
// ("metadata" on logs) must fail here rather than at the user's terminal.
//
// Every command that registers a local --filter flag must be listed, so a new
// filter-taking command cannot ship unchecked examples.
func TestFilterHelpExamplesAreValid(t *testing.T) {
	t.Parallel()

	nonAuditTargets := []commonpb.QueryTarget{
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
	}

	parsers := map[string]filterHelpParser{
		"ledgerctl transactions list":          buildQueryFilterFor(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS),
		"ledgerctl accounts list":              buildQueryFilterFor(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS),
		"ledgerctl accounts aggregate-volumes": buildQueryFilterFor(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS),
		"ledgerctl logs list":                  buildQueryFilterFor(commonpb.QueryTarget_QUERY_TARGET_LOGS),
		"ledgerctl audit list":                 buildQueryFilterFor(commonpb.QueryTarget_QUERY_TARGET_AUDIT),
		"ledgerctl queries create": func(line, expr string) (*commonpb.QueryFilter, []commonpb.QueryTarget, error) {
			m := queriesTargetFlag.FindStringSubmatch(line)
			require.NotNilf(t, m, "queries create example without --target: %s", line)

			target, ok := map[string]commonpb.QueryTarget{
				"accounts":     commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
				"transactions": commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
				"logs":         commonpb.QueryTarget_QUERY_TARGET_LOGS,
			}[m[1]]
			require.Truef(t, ok, "unknown --target %q in example: %s", m[1], line)

			f, err := filterexpr.Parse(expr, target)

			return f, []commonpb.QueryTarget{target}, err
		},
		// update does not know the stored query's target from the example
		// line: the filter must be valid on at least one prepared-query target.
		"ledgerctl queries update": func(_, expr string) (*commonpb.QueryFilter, []commonpb.QueryTarget, error) {
			f, err := filterexpr.DecodeDualFormatStructuralOnly([]byte(expr), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)

			return f, nonAuditTargets, err
		},
	}

	seen := map[string]bool{}

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.LocalFlags().Lookup("filter") != nil {
			path := cmd.CommandPath()
			parse, ok := parsers[path]
			require.Truef(t, ok, "command %q has a --filter flag but its help examples are not checked: add it to this test", path)
			seen[path] = true

			for line := range strings.SplitSeq(cmd.Long, "\n") {
				m := filterHelpExample.FindStringSubmatch(line)
				if m == nil {
					continue
				}

				expr := m[1] + m[2]
				f, targets, err := parse(line, expr)
				require.NoErrorf(t, err, "%s: help example does not parse: %s", path, line)
				require.NotNilf(t, f, "%s: help example parses to an empty filter: %s", path, line)

				var rejections []string
				for _, target := range targets {
					verr := domain.ValidateFilterForTarget(f, target)
					if verr == nil {
						rejections = nil

						break
					}
					rejections = append(rejections, verr.Error())
				}
				require.Emptyf(t, rejections, "%s: help example is rejected by the server: %s", path, line)
			}
		}

		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	walk(newRootCommand())

	for path := range parsers {
		require.Truef(t, seen[path], "command %q listed in this test no longer has a --filter flag", path)
	}
}

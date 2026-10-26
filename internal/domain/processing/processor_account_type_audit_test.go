package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAuditModeAcceptsUnmatchedAccounts(t *testing.T) {
	t.Parallel()

	compiled := accounttype.CompileTypes(map[string]*commonpb.AccountType{
		"user": {Name: "user", Pattern: "users:{id}"},
	})
	require.Len(t, compiled, 1)

	postings := []*commonpb.Posting{{Source: "world", Destination: "merchants:shop"}}
	require.NoError(t, validatePostingsAgainstAccountTypes(postings, compiled, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT))
	require.NoError(t, validateAccountAgainstAccountTypes("merchants:shop", compiled, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT))

	require.Error(t, validatePostingsAgainstAccountTypes(postings, compiled, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT))
	require.Error(t, validateAccountAgainstAccountTypes("merchants:shop", compiled, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT))
}

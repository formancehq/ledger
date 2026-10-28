package auditintent

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestPolicyIntentBoundary(t *testing.T) {
	t.Parallel()
	raw := (&raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 3}}).MarshalDeterministicVT(nil)
	for name, item := range map[string]*auditpb.AuditItem{
		"mixed":          {SerializedOrder: []byte{1}, SerializedSetClusterPolicy: raw},
		"malformed":      {SerializedSetClusterPolicy: []byte{255}},
		"missing policy": {SerializedSetClusterPolicy: []byte{16, 1}},
	} {
		t.Run(name, func(t *testing.T) { t.Parallel(); _, err := Policy(item); require.Error(t, err) })
	}
	policy, err := Policy(&auditpb.AuditItem{SerializedSetClusterPolicy: raw})
	require.NoError(t, err)
	require.EqualValues(t, 3, policy.GetPolicy().GetRevision())
	policy, err = Policy(&auditpb.AuditItem{SerializedOrder: []byte{10, 0}})
	require.NoError(t, err)
	require.Nil(t, policy)
}

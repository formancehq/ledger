// Package auditintent reads immutable audit intent in current and historical
// encodings. It never converts native controls into historical orders.
package auditintent

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Policy decodes a native control, or returns nil for an execution order.
// Both intent fields at once are invalid; keeping the published order field
// outside a protobuf oneof preserves existing clients' generated APIs.
func Policy(item *auditpb.AuditItem) (*raftcmdpb.SetClusterPolicyProposal, error) {
	raw := item.GetSerializedSetClusterPolicy()
	if len(raw) == 0 {
		return nil, nil
	}
	if len(item.GetSerializedOrder()) != 0 {
		return nil, errors.New("audit item contains both an order and a policy control")
	}
	policy := &raftcmdpb.SetClusterPolicyProposal{}
	if err := policy.UnmarshalVT(raw); err != nil {
		return nil, fmt.Errorf("decoding audited policy control: %w", err)
	}
	if policy.GetPolicy() == nil {
		return nil, errors.New("audited policy control has no policy")
	}

	return policy, nil
}

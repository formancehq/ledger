package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
)

func TestValidateCapabilityActivationEvidenceRejectsIncompleteMembership(t *testing.T) {
	t.Parallel()

	err := validateCapabilityActivationEvidence(
		[]capabilities.CapabilityID{7},
		[]capabilities.CapabilityID{7},
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter}},
	)

	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
}

func TestValidateCapabilityActivationEvidenceAcceptsNoActivation(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateCapabilityActivationEvidence(nil, nil, nil))
}

func TestValidateCapabilityActivationEvidenceAcceptsConvergedMembership(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateCapabilityActivationEvidence(
		[]capabilities.CapabilityID{7},
		[]capabilities.CapabilityID{7},
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: []capabilities.CapabilityID{7}}},
	))
}

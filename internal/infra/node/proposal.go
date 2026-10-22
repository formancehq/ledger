package node

import (
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

type Proposal struct {
	*futures.Future[any]

	commandID         uint64
	data              []byte
	drainMarker       bool
	releaseMarker     bool
	membershipBarrier bool
	// expectedTerm fences a drain preflight against a later leadership term.
	expectedTerm         uint64
	requiredCapabilities []capabilities.CapabilityID
}

func newProposal(commandID uint64, data []byte, required []capabilities.CapabilityID) *Proposal {
	return &Proposal{
		commandID:            commandID,
		data:                 append([]byte(nil), data...),
		requiredCapabilities: append([]capabilities.CapabilityID(nil), required...),
		Future:               futures.New[any](),
	}
}

// NewPreparedProposal is the only public proposal factory. Callers must pass
// the opaque result returned by capabilities.PrepareProposal for the same
// command before serialization; the raw constructor remains package-private.
func NewPreparedProposal(commandID uint64, data []byte, prepared capabilities.PreparedProposal) *Proposal {
	return newProposal(commandID, data, prepared.RequiredCapabilities())
}

// Data returns the serialized proposal data.
func (p *Proposal) Data() []byte {
	return append([]byte(nil), p.data...)
}

// RequiredCapabilities returns the capabilities derived by the producer from
// the final command before it was serialized. Admission reuses this typed
// value instead of decoding the serialized proposal a second time.
func (p *Proposal) RequiredCapabilities() []capabilities.CapabilityID {
	return append([]capabilities.CapabilityID(nil), p.requiredCapabilities...)
}

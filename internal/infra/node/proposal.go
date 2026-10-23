package node

import (
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

type Proposal struct {
	*futures.Future[any]

	commandID            uint64
	data                 []byte
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

// NewProposal is the only public proposal factory. It derives the capability
// declaration and serializes the same final command, making it impossible for
// the typed admission metadata to diverge from the wire payload.
func NewProposal(commandID uint64, command *raftcmdpb.Proposal) (*Proposal, error) {
	prepared, err := capabilities.PrepareProposal(command)
	if err != nil {
		return nil, err
	}

	data, err := command.MarshalVT()
	if err != nil {
		return nil, err
	}

	return newProposal(commandID, data, prepared.RequiredCapabilities()), nil
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

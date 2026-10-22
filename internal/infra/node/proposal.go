package node

import (
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

type Proposal struct {
	*futures.Future[any]

	commandID            uint64
	data                 []byte
	requiredCapabilities []capabilities.CapabilityID
}

func NewProposal(commandID uint64, data []byte, required []capabilities.CapabilityID) *Proposal {
	return &Proposal{
		commandID:            commandID,
		data:                 data,
		requiredCapabilities: append([]capabilities.CapabilityID(nil), required...),
		Future:               futures.New[any](),
	}
}

// Data returns the serialized proposal data.
func (p *Proposal) Data() []byte {
	return p.data
}

// RequiredCapabilities returns the capabilities derived by the producer from
// the final command before it was serialized. Admission reuses this typed
// value instead of decoding the serialized proposal a second time.
func (p *Proposal) RequiredCapabilities() []capabilities.CapabilityID {
	return append([]capabilities.CapabilityID(nil), p.requiredCapabilities...)
}

package node

import (
	"slices"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

type Proposal struct {
	*futures.Future[any]

	commandID            uint64
	data                 []byte
	declaredCapabilities []capabilities.ID
}

// RequireCapabilities records the producer declaration that must match the
// final serialized proposal. It is intentionally separate from the bytes so a
// plan rebuild cannot silently retain stale capability metadata.
func (p *Proposal) RequireCapabilities(ids ...capabilities.ID) {
	p.declaredCapabilities = slices.Clone(ids)
}

func (p *Proposal) DeclaredCapabilities() []capabilities.ID {
	return slices.Clone(p.declaredCapabilities)
}

func NewProposal(commandID uint64, data []byte) *Proposal {
	return &Proposal{
		commandID: commandID,
		data:      data,
		Future:    futures.New[any](),
	}
}

// Data returns the serialized proposal data.
func (p *Proposal) Data() []byte {
	return p.data
}

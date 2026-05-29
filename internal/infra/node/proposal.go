package node

import "github.com/formancehq/ledger/v3/internal/pkg/futures"

type Proposal struct {
	*futures.Future[any]

	commandID uint64
	data      []byte
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

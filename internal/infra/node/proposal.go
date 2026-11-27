package node

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/proposals"
	"github.com/formancehq/ledger/v3/internal/pkg/protowireutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

var predictedIndexField = func() protowire.Number {
	fd := (&raftcmdpb.Proposal{}).ProtoReflect().Descriptor().Fields().ByName("predicted_index")
	if fd == nil {
		panic("invariant: Proposal has no predicted_index field")
	}

	return fd.Number()
}()

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
	hasBusinessOrders    bool
	// activePolicyCapabilities is populated by the proposal factory for
	// SetClusterPolicy proposals/orders and capability activation transitions.
	// Keeping the typed candidate alongside the
	// serialized command lets the leader validate activation evidence before
	// handing the bytes to Raft without decoding the command a second time.
	activePolicyCapabilities []capabilities.CapabilityID
}

func newProposal(commandID uint64, data []byte, required []capabilities.CapabilityID) *Proposal {
	// Reserve the wire tag and fixed64 value before the index tracker is held.
	// SetPredictedIndex must only append into this buffer in its critical section.
	buffer := make([]byte, len(data), len(data)+protowire.SizeTag(predictedIndexField)+8)
	copy(buffer, data)

	return &Proposal{
		commandID:            commandID,
		data:                 buffer,
		requiredCapabilities: append([]capabilities.CapabilityID(nil), required...),
		Future:               futures.New[any](),
	}
}

// NewProposal constructs a proposal without semantic dependencies. Producers
// of capability-dependent orders must choose from committed policy explicitly.
func NewProposal(commandID uint64, command *raftcmdpb.Proposal) (*Proposal, error) {
	required, err := capabilities.RequiredCapabilities(command)
	if err != nil {
		return nil, err
	}
	if len(required) != 0 {
		return nil, fmt.Errorf("%w: capability-dependent order requires an activation snapshot", capabilities.ErrInvalidDeclaration)
	}

	return NewProposalWithPolicy(commandID, command, nil)
}

// NewProposalWithPolicy serializes the final command once, with a marker
// selected from committed policy observed by the producer before this call.
func NewProposalWithPolicy(commandID uint64, command *raftcmdpb.Proposal, policy *commonpb.ClusterPolicy) (*Proposal, error) {
	candidate, err := capabilities.RequiredCapabilities(command)
	if err != nil {
		return nil, err
	}
	active := capabilities.FromPolicy(policy).Activations[capabilities.StrictAccountTypes]
	var selected []capabilities.CapabilityID
	command.RequiredCapabilities = nil
	if active {
		selected = candidate
		command.RequiredCapabilities = make([]uint32, len(selected))
		for i, id := range selected {
			command.RequiredCapabilities[i] = uint32(id)
		}
	}
	data, err := command.MarshalVT()
	if err != nil {
		return nil, err
	}
	proposal := newProposal(commandID, data, selected)
	if err := capabilities.ValidateFinalProposalForActivation(data, selected, active); err != nil {
		return nil, err
	}
	proposal.hasBusinessOrders = len(proposals.BatchOrders(command)) != 0
	if transition := command.GetCapabilityActivation(); transition != nil {
		proposal.activePolicyCapabilities = append(proposal.activePolicyCapabilities, capabilities.CapabilityID(transition.GetId()))
	}
	if policy := command.GetSetClusterPolicy().GetPolicy(); policy != nil {
		for _, activation := range policy.GetCapabilityActivations() {
			if activation != nil && activation.GetActive() {
				proposal.activePolicyCapabilities = append(proposal.activePolicyCapabilities, capabilities.CapabilityID(activation.GetId()))
			}
		}
	}
	for _, order := range command.GetOrders() {
		policy := order.GetSystemScoped().GetSetClusterPolicy().GetPolicy()
		if policy == nil {
			continue
		}
		for _, activation := range policy.GetCapabilityActivations() {
			if activation != nil && activation.GetActive() {
				proposal.activePolicyCapabilities = append(proposal.activePolicyCapabilities, capabilities.CapabilityID(activation.GetId()))
			}
		}
	}

	return proposal, nil
}

func (p *Proposal) HasBusinessOrders() bool {
	return p.hasBusinessOrders
}

// Data returns the serialized proposal data.
func (p *Proposal) Data() []byte {
	return append([]byte(nil), p.data...)
}

// SetPredictedIndex patches the predicted Raft index onto the already
// serialized proposal. NewProposal deliberately serializes with a zero index;
// callers can therefore perform the expensive payload serialization before
// acquiring the shared proposal lock and hold it only for this bounded append.
func (p *Proposal) SetPredictedIndex(index uint64) {
	if index == 0 {
		return
	}

	p.data = protowireutil.AppendFixed64(p.data, predictedIndexField, index)
}

// RequiredCapabilities returns the capabilities selected during preparation
// for the serialized proposal. Admission reuses this typed value instead
// of decoding the serialized proposal a second time.
func (p *Proposal) RequiredCapabilities() []capabilities.CapabilityID {
	return append([]capabilities.CapabilityID(nil), p.requiredCapabilities...)
}

// ActivePolicyCapabilities returns the active capabilities requested by a
// SetClusterPolicy proposal or order, if any. The values are derived by NewProposal from
// the typed command and are not read from the serialized payload.
func (p *Proposal) ActivePolicyCapabilities() []capabilities.CapabilityID {
	return append([]capabilities.CapabilityID(nil), p.activePolicyCapabilities...)
}

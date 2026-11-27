package node

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/proposals"
	"github.com/formancehq/ledger/v3/internal/pkg/protowireutil"
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
	// legacyData is the same business proposal without a capability marker.
	// The admission gate selects it while activation is not yet committed.
	legacyData         []byte
	markedData         []byte
	markedCapabilities []capabilities.CapabilityID
	legacySelected     bool
	hasBusinessOrders  bool
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

// NewProposal is the only public proposal factory. It derives the capability
// declaration and serializes both forms of a capability-dependent command.
// The admission gate selects one form from committed activation state, keeping
// the typed admission metadata aligned with the selected wire payload.
func NewProposal(commandID uint64, command *raftcmdpb.Proposal) (*Proposal, error) {
	prepared, err := capabilities.PrepareProposal(command)
	if err != nil {
		return nil, err
	}

	data, err := command.MarshalVT()
	if err != nil {
		return nil, err
	}

	proposal := newProposal(commandID, data, prepared.RequiredCapabilities())
	// Validate the complete serialized command before it enters the Raft queue.
	// SetPredictedIndex only appends a fixed-width index field afterward; it
	// cannot change the payload or its capability declaration.
	if err := capabilities.ValidateFinalProposal(data, proposal.requiredCapabilities); err != nil {
		return nil, err
	}
	if len(prepared.RequiredCapabilities()) > 0 {
		legacy := command.CloneVT()
		legacy.RequiredCapabilities = nil
		legacyData, err := legacy.MarshalVT()
		if err != nil {
			return nil, err
		}
		proposal.legacyData = make([]byte, len(legacyData), len(legacyData)+protowire.SizeTag(predictedIndexField)+8)
		copy(proposal.legacyData, legacyData)
		proposal.markedData = proposal.data
		proposal.markedCapabilities = prepared.RequiredCapabilities()
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

	if p.legacyData != nil {
		p.markedData = protowireutil.AppendFixed64(p.markedData, predictedIndexField, index)
		p.legacyData = protowireutil.AppendFixed64(p.legacyData, predictedIndexField, index)
		if p.legacySelected {
			p.data = p.legacyData
		} else {
			p.data = p.markedData
		}

		return
	}
	p.data = protowireutil.AppendFixed64(p.data, predictedIndexField, index)
}

// SelectActiveCapabilities chooses the persisted semantics at the serialized
// admission boundary. Both wire variants were built from the same final command
// before admission: normally before acquiring the proposal guard, or under the
// guard when stale preloads require rebuilding the execution plan.
func (p *Proposal) SelectActiveCapabilities(active bool) {
	if p.legacyData == nil {
		return
	}
	if active {
		p.legacySelected = false
		p.data = p.markedData
		p.requiredCapabilities = append([]capabilities.CapabilityID(nil), p.markedCapabilities...)
	} else {
		p.legacySelected = true
		p.data = p.legacyData
		p.requiredCapabilities = nil
	}
}

// RequiredCapabilities returns the capabilities selected by the admission
// gate for the serialized proposal. Admission reuses this typed value instead
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

package node

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
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

var predictedIndexEncodedSize = protowire.SizeTag(predictedIndexField) + protowire.SizeFixed64()

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
	// activePolicyCapabilities is populated by the proposal factory for
	// SetClusterPolicy orders.  Keeping the typed candidate alongside the
	// serialized command lets the leader validate activation evidence before
	// handing the bytes to Raft without decoding the command a second time.
	activePolicyCapabilities []capabilities.CapabilityID
}

func newProposal(commandID uint64, data []byte, required []capabilities.CapabilityID) *Proposal {
	// Reserve the complete predicted-index suffix before the proposal enters the
	// tracker critical section. SetPredictedIndex must stay allocation-free on
	// the common path; otherwise a capacity-boundary append copies the complete
	// payload while concurrent producers hold the shared tracker lock.
	prepared := make([]byte, len(data), len(data)+predictedIndexEncodedSize)
	copy(prepared, data)

	return &Proposal{
		commandID:            commandID,
		data:                 prepared,
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

	proposal := newProposal(commandID, data, prepared.RequiredCapabilities())
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

// RequiredCapabilities returns the capabilities derived by the producer from
// the final command before it was serialized. Admission reuses this typed
// value instead of decoding the serialized proposal a second time.
func (p *Proposal) RequiredCapabilities() []capabilities.CapabilityID {
	return append([]capabilities.CapabilityID(nil), p.requiredCapabilities...)
}

// ActivePolicyCapabilities returns the active capabilities requested by a
// SetClusterPolicy order, if any. The values are derived by NewProposal from
// the typed command and are not read from the serialized payload.
func (p *Proposal) ActivePolicyCapabilities() []capabilities.CapabilityID {
	return append([]capabilities.CapabilityID(nil), p.activePolicyCapabilities...)
}

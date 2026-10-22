// Package capabilities contains the deterministic contract used to gate
// rolling Raft protocol changes. It deliberately has no node-local state and
// no network or storage dependency: callers provide the committed policy and
// a snapshot of membership evidence.
package capabilities

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CapabilityID is the stable numeric identity of a consensus capability.
// The wire representation remains uint32 at protobuf and Raft JSON boundaries.
type CapabilityID uint32

// SupportedIDs is the immutable capability set implemented by this binary.
// No protocol feature is activated yet; this empty registry keeps the
// membership-evidence plumbing ready for the first real capability.
func SupportedIDs() []CapabilityID {
	return nil
}

type MemberRole uint8

const (
	Voter MemberRole = iota + 1
	Learner
)

type Member struct {
	NodeID       uint64
	InstanceID   []byte
	Role         MemberRole
	Reachable    bool
	Capabilities []CapabilityID
}

type State struct {
	Activations map[CapabilityID]bool
}

type Blocker struct {
	NodeID uint64
	Role   MemberRole
	Reason string
}

type Decision struct {
	Allowed  bool
	Reason   string
	Blockers []Blocker
}

func ValidateMembers(members []Member) error {
	seen := make(map[uint64][]byte, len(members))
	for _, member := range members {
		if member.NodeID == 0 || (member.Role != Voter && member.Role != Learner) {
			return fmt.Errorf("%w: invalid member %d", ErrInvalidState, member.NodeID)
		}
		identity := bytes.Clone(member.InstanceID)
		if previous, ok := seen[member.NodeID]; ok && !bytes.Equal(previous, identity) {
			return fmt.Errorf("%w: node %d has multiple incarnations", ErrInvalidState, member.NodeID)
		}
		if _, err := Normalize(member.Capabilities); err != nil {
			return fmt.Errorf("%w: node %d has invalid capability evidence: %w", ErrInvalidState, member.NodeID, err)
		}
		seen[member.NodeID] = identity
	}

	return nil
}

var (
	ErrInvalidState          = errors.New("invalid capability state")
	ErrInvalidDeclaration    = errors.New("invalid capability declaration")
	ErrMissingCapability     = errors.New("proposal is missing a required capability declaration")
	ErrCapabilityUnsupported = errors.New("capability is unsupported by a cluster member")
)

// RequiredCapabilities derives semantic protocol dependencies from the final
// proposal shape. The registry is deliberately colocated with the wire
// contract so a producer cannot make a feature-specific payload appear safe by
// omitting required_capabilities. Predicates inspect only replicated fields,
// which makes the result deterministic on every member.
func RequiredCapabilities(proposal *raftcmdpb.Proposal) ([]CapabilityID, error) {
	if proposal == nil {
		return nil, fmt.Errorf("%w: nil proposal", ErrInvalidDeclaration)
	}

	return nil, nil
}

// StampProposal replaces required_capabilities with the deterministic result
// of RequiredCapabilities. Producers call this after rebuilding the final
// execution plan and immediately before marshaling; repeated calls are safe on
// retry and rebuild paths.
func StampProposal(proposal *raftcmdpb.Proposal) error {
	_, err := PrepareProposal(proposal)

	return err
}

// PrepareProposal derives and stamps the semantic capability declaration and
// returns the typed result for the caller to pass alongside the serialized
// proposal. Keeping the result avoids decoding the same proposal again during
// admission.
func PrepareProposal(proposal *raftcmdpb.Proposal) ([]CapabilityID, error) {
	required, err := RequiredCapabilities(proposal)
	if err != nil {
		return nil, err
	}
	proposal.RequiredCapabilities = make([]uint32, len(required))
	for i, id := range required {
		proposal.RequiredCapabilities[i] = uint32(id)
	}

	return required, nil
}

func (s State) Validate() error {
	return nil
}

func FromPolicy(policy *commonpb.ClusterPolicy) State {
	s := State{Activations: make(map[CapabilityID]bool)}
	if policy == nil {
		return s
	}
	for _, activation := range policy.GetCapabilityActivations() {
		if activation != nil {
			s.Activations[CapabilityID(activation.GetId())] = activation.GetActive()
		}
	}

	return s
}

func Normalize(ids []CapabilityID) ([]CapabilityID, error) {
	result := slices.Clone(ids)
	slices.Sort(result)
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, fmt.Errorf("%w: duplicate id %d", ErrInvalidDeclaration, result[i])
		}
	}

	return result, nil
}

// Ready returns the capabilities implemented by this binary and acknowledged
// by every current cluster member. Membership evidence is the acknowledgement
// recorded by JoinAsLearner/RefreshMembership; an empty member set is never
// considered ready.
func Ready(supported []CapabilityID, members []Member) ([]CapabilityID, error) {
	if len(members) == 0 {
		return nil, nil
	}
	supported, err := Normalize(supported)
	if err != nil {
		return nil, err
	}
	if err := ValidateMembers(members); err != nil {
		return nil, err
	}
	ready := make([]CapabilityID, 0, len(supported))
	for _, id := range supported {
		acknowledged := true
		for _, member := range members {
			if !slices.Contains(member.Capabilities, id) {
				acknowledged = false

				break
			}
		}
		if acknowledged {
			ready = append(ready, id)
		}
	}

	return ready, nil
}

// Evaluate is pure. Reachability is reported separately from support: a
// temporarily unreachable voter is still a blocker for activation, while a
// learner is included because it must replay every committed entry.
func Evaluate(state State, required []CapabilityID, _ bool, members []Member) Decision {
	if err := state.Validate(); err != nil {
		return Decision{Reason: err.Error()}
	}
	required, err := Normalize(required)
	if err != nil {
		return Decision{Reason: err.Error()}
	}
	blockers := make([]Blocker, 0)
	// A required capability is not admitted merely because members advertise
	// support: the replicated activation must also have been committed.
	for _, id := range required {
		if !state.Activations[id] {
			blockers = append(blockers, Blocker{Reason: fmt.Sprintf("capability %d is inactive", id)})
		}
	}
	for _, member := range members {
		for _, id := range required {
			if !slices.Contains(member.Capabilities, id) {
				blockers = append(blockers, Blocker{NodeID: member.NodeID, Role: member.Role, Reason: fmt.Sprintf("missing capability %d", id)})

				continue
			}
			if !member.Reachable {
				blockers = append(blockers, Blocker{NodeID: member.NodeID, Role: member.Role, Reason: "member is unreachable"})
			}
		}
	}
	if len(blockers) > 0 {
		return Decision{Reason: ErrCapabilityUnsupported.Error(), Blockers: blockers}
	}

	return Decision{Allowed: true, Blockers: blockers}
}

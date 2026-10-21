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

type ID uint32

// IDs are protocol constants. Once published, an ID is never reused.
const (
	IDExecutionPlanV1  ID = 1
	IDSemanticReplayV1 ID = 2
)

// SupportedIDs is the immutable capability set implemented by this binary.
// It is used as membership evidence during join; callers must copy the result
// before attaching it to a mutable wire message.
func SupportedIDs() []ID {
	return []ID{IDExecutionPlanV1, IDSemanticReplayV1}
}

// CurrentReplayProtocol is the replay encoding understood by this binary.
// It is intentionally independent from the client-facing gRPC protocol and
// from the capability IDs above. Once a policy raises its replay floor, a
// binary whose supported floor is lower must refuse to open the store rather
// than decode state it cannot execute deterministically.
const CurrentReplayProtocol uint32 = 1

type AdmissionPolicy uint8

const (
	FeatureOnly AdmissionPolicy = iota + 1
	AllWrites
)

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
	Capabilities []ID
}

type State struct {
	Armed              bool
	Policy             AdmissionPolicy
	MinimumReplay      uint32
	MembershipRevision uint64
	Activations        map[ID]bool
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
		seen[member.NodeID] = identity
	}

	return nil
}

func ValidateMembershipRevision(observed, examined uint64) error {
	if examined == 0 || observed != examined {
		return fmt.Errorf("%w: membership revision changed from %d to %d", ErrInvalidState, examined, observed)
	}

	return nil
}

func AdvanceReplayFloor(current, next uint32) (uint32, error) {
	if next < current {
		return current, fmt.Errorf("%w: replay protocol floor cannot decrease from %d to %d", ErrInvalidState, current, next)
	}

	return next, nil
}

var (
	ErrInvalidState              = errors.New("invalid capability state")
	ErrInvalidDeclaration        = errors.New("invalid capability declaration")
	ErrMissingCapability         = errors.New("proposal is missing a required capability declaration")
	ErrCapabilityUnsupported     = errors.New("capability is unsupported by a cluster member")
	ErrReplayProtocolUnsupported = errors.New("replay protocol floor is unsupported")
)

// RequiredCapabilities derives semantic protocol dependencies from the final
// proposal shape. The registry is deliberately colocated with the wire
// contract so a producer cannot make a feature-specific payload appear safe by
// omitting required_capabilities. Predicates inspect only replicated fields,
// which makes the result deterministic on every member.
func RequiredCapabilities(proposal *raftcmdpb.Proposal) ([]ID, error) {
	if proposal == nil {
		return nil, fmt.Errorf("%w: nil proposal", ErrInvalidDeclaration)
	}

	var required []ID
	// ExecutionPlan is the admission/apply coverage contract. Its presence is
	// significant even when the message is empty (for no-read proposals): an
	// old binary cannot safely replay the envelope without understanding it.
	if proposal.GetExecutionPlan() != nil {
		required = append(required, IDExecutionPlanV1)
	}
	// Idempotency freezes a proposal's semantic outcome for replay. Attach the
	// dependency to the envelope rather than to the number of orders so an
	// empty or metadata-only proposal cannot evade the barrier.
	if proposal.GetIdempotency() != nil {
		required = append(required, IDSemanticReplayV1)
	}

	return Normalize(required)
}

// StampProposal replaces required_capabilities with the deterministic result
// of RequiredCapabilities. Producers call this after rebuilding the final
// execution plan and immediately before marshaling; repeated calls are safe on
// retry and rebuild paths.
func StampProposal(proposal *raftcmdpb.Proposal) error {
	required, err := RequiredCapabilities(proposal)
	if err != nil {
		return err
	}
	proposal.RequiredCapabilities = make([]uint32, len(required))
	for i, id := range required {
		proposal.RequiredCapabilities[i] = uint32(id)
	}

	return nil
}

// ValidateReplayFloor checks the persisted replay floor before any other
// replicated state is decoded. A zero floor is the legacy policy and is
// accepted by every binary; newer floors are safe only when this binary knows
// how to decode and execute the corresponding entries.
func ValidateReplayFloor(required, supported uint32) error {
	if required > supported {
		return fmt.Errorf("%w: persisted floor %d exceeds supported protocol %d", ErrReplayProtocolUnsupported, required, supported)
	}

	return nil
}

func (s State) Validate() error {
	if !s.Armed {
		return nil
	}
	if s.Policy != FeatureOnly && s.Policy != AllWrites {
		return fmt.Errorf("%w: admission policy %d", ErrInvalidState, s.Policy)
	}
	if s.MembershipRevision == 0 {
		return fmt.Errorf("%w: zero membership revision", ErrInvalidState)
	}

	return nil
}

func FromPolicy(policy *commonpb.ClusterPolicy) State {
	s := State{Activations: make(map[ID]bool)}
	if policy == nil {
		return s
	}
	s.Armed = policy.GetCapabilityActivationArmed()
	s.MinimumReplay = policy.GetMinimumReplayProtocol()
	s.MembershipRevision = policy.GetCapabilityMembershipRevision()
	if policy.GetCapabilityAdmissionPolicy() == commonpb.CapabilityAdmissionPolicy_CAPABILITY_ADMISSION_POLICY_ALL_WRITES {
		s.Policy = AllWrites
	} else if policy.GetCapabilityAdmissionPolicy() == commonpb.CapabilityAdmissionPolicy_CAPABILITY_ADMISSION_POLICY_FEATURE_ONLY {
		s.Policy = FeatureOnly
	}
	for _, activation := range policy.GetCapabilityActivations() {
		if activation != nil {
			s.Activations[ID(activation.GetId())] = activation.GetActive()
		}
	}

	return s
}

func Normalize(ids []ID) ([]ID, error) {
	result := slices.Clone(ids)
	slices.Sort(result)
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, fmt.Errorf("%w: duplicate id %d", ErrInvalidDeclaration, result[i])
		}
	}

	return result, nil
}

// Evaluate is pure. Reachability is reported separately from support: a
// temporarily unreachable voter is still a blocker for activation, while a
// learner is included because it must replay every committed entry.
func Evaluate(state State, required []ID, businessWrite bool, members []Member) Decision {
	if err := state.Validate(); err != nil {
		return Decision{Reason: err.Error()}
	}
	if !state.Armed {
		return Decision{Allowed: true}
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
	if len(blockers) > 0 && (businessWrite && state.Policy == AllWrites || len(required) > 0) {
		return Decision{Reason: ErrCapabilityUnsupported.Error(), Blockers: blockers}
	}
	if businessWrite && state.Policy == AllWrites && len(required) == 0 {
		return Decision{Reason: "business writes are blocked while capability activation is armed", Blockers: blockers}
	}

	return Decision{Allowed: true, Blockers: blockers}
}

// ValidateFinalProposal proves that the declaration belongs to the bytes that
// will be proposed. Callers must invoke it after rebuilding an execution plan.
func ValidateFinalProposal(data []byte, declared []ID) error {
	proposal := new(raftcmdpb.Proposal)
	if err := proposal.UnmarshalVT(data); err != nil {
		return fmt.Errorf("decode final proposal: %w", err)
	}
	expected, err := RequiredCapabilities(proposal)
	if err != nil {
		return err
	}
	wire := make([]ID, len(proposal.GetRequiredCapabilities()))
	for i, id := range proposal.GetRequiredCapabilities() {
		wire[i] = ID(id)
	}
	wire, err = Normalize(wire)
	if err != nil {
		return err
	}
	declared, err = Normalize(declared)
	if err != nil {
		return err
	}
	if !bytes.Equal(idsBytes(wire), idsBytes(expected)) {
		return fmt.Errorf("%w: wire declaration does not match semantic payload", ErrMissingCapability)
	}
	if !bytes.Equal(idsBytes(wire), idsBytes(declared)) {
		return fmt.Errorf("%w: declaration does not match final payload", ErrMissingCapability)
	}

	return nil
}

func idsBytes(ids []ID) []byte {
	result := make([]byte, len(ids)*4)
	for i, id := range ids {
		result[i*4] = byte(id)
		result[i*4+1] = byte(id >> 8)
		result[i*4+2] = byte(id >> 16)
		result[i*4+3] = byte(id >> 24)
	}

	return result
}

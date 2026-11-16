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

	"github.com/formancehq/ledger/v3/internal/pkg/proposals"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CapabilityID is the stable numeric identity of a consensus capability.
// The wire representation remains uint32 at protobuf and Raft JSON boundaries.
type CapabilityID uint32

// CreateLedgerAccountTypesV2 selects validation of initial account-type
// constraints and conflicts during CreateLedger apply. Unmarked proposals keep
// the released behavior, including when replayed after activation.
const CreateLedgerAccountTypesV2 CapabilityID = 1

// PreparedProposal is the opaque capability metadata produced while preparing
// a final proposal. Callers can inspect it through RequiredCapabilities, but
// cannot construct the declaration by passing an arbitrary slice to the node
// proposal factory. Semantic selectors must match a known proposal shape.
type PreparedProposal struct {
	required []CapabilityID
}

// RequiredCapabilities returns a defensive copy of the derived declaration.
func (p PreparedProposal) RequiredCapabilities() []CapabilityID {
	return slices.Clone(p.required)
}

// SupportedIDs is the immutable capability set implemented by this binary.
func SupportedIDs() []CapabilityID {
	return []CapabilityID{CreateLedgerAccountTypesV2}
}

func MembershipRequiredIDs(policy *commonpb.ClusterPolicy) []uint32 {
	var required []uint32
	for _, activation := range policy.GetCapabilityActivations() {
		if activation.GetActive() {
			required = append(required, activation.GetId())
		}
	}
	slices.Sort(required)

	return required
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
	ErrActivationPending     = errors.New("capability activation is pending")
	ErrCapabilityUnsupported = errors.New("capability is unsupported by a cluster member")
)

// RequiredCapabilities validates declared semantic dependencies against the
// final proposal shape. A CreateLedger proposal may be unmarked before policy
// activation; the final policy gate requires its marker once activated.
// Predicates inspect only replicated fields, keeping replay deterministic.
func RequiredCapabilities(proposal *raftcmdpb.Proposal) ([]CapabilityID, error) {
	if proposal == nil {
		return nil, fmt.Errorf("%w: nil proposal", ErrInvalidDeclaration)
	}

	declared := make([]CapabilityID, len(proposal.GetRequiredCapabilities()))
	for i, id := range proposal.GetRequiredCapabilities() {
		declared[i] = CapabilityID(id)
	}
	if len(declared) == 0 {
		return nil, nil
	}
	if len(declared) != 1 || declared[0] != CreateLedgerAccountTypesV2 || !HasCreateLedger(proposal) {
		return nil, fmt.Errorf("%w: capability declaration does not match proposal orders", ErrInvalidDeclaration)
	}

	return declared, nil
}

// HasCreateLedger inspects the final proposal, including batched orders.
func HasCreateLedger(proposal *raftcmdpb.Proposal) bool {
	if proposal == nil {
		return false
	}
	for _, order := range proposals.BatchOrders(proposal) {
		if order.GetLedgerScoped().GetCreateLedger() != nil {
			return true
		}
	}

	return false
}

// SelectCreateLedgerValidation stamps the proposal-scoped semantic selector
// from the committed policy observed by admission. The final Node.Propose gate
// rechecks the policy: an activation racing this read rejects an unmarked
// proposal instead of committing it with the wrong semantics.
func SelectCreateLedgerValidation(proposal *raftcmdpb.Proposal, policy *commonpb.ClusterPolicy) {
	if !HasCreateLedger(proposal) {
		return
	}
	if FromPolicy(policy).Activations[CreateLedgerAccountTypesV2] {
		proposal.RequiredCapabilities = []uint32{uint32(CreateLedgerAccountTypesV2)}
	}
}

// ValidateCreateLedgerDeclaration enforces the activated rule at the final
// serialized admission boundary, before Raft can commit a new order.
func ValidateCreateLedgerDeclaration(policy *commonpb.ClusterPolicy, hasCreateLedger bool, required []CapabilityID) error {
	if hasCreateLedger && FromPolicy(policy).Activations[CreateLedgerAccountTypesV2] && !slices.Contains(required, CreateLedgerAccountTypesV2) {
		return fmt.Errorf("%w: CreateLedger requires capability %d", ErrMissingCapability, CreateLedgerAccountTypesV2)
	}

	return nil
}

// PrepareProposal validates and stamps the semantic capability declaration and
// returns the typed result for the caller to pass alongside the serialized
// proposal. Keeping the result avoids decoding the same proposal again during
// admission.
func PrepareProposal(proposal *raftcmdpb.Proposal) (PreparedProposal, error) {
	required, err := RequiredCapabilities(proposal)
	if err != nil {
		return PreparedProposal{}, err
	}
	proposal.RequiredCapabilities = make([]uint32, len(required))
	for i, id := range required {
		proposal.RequiredCapabilities[i] = uint32(id)
	}

	return PreparedProposal{required: required}, nil
}

// ValidateFinalProposal verifies that the declaration carried by the
// serialized proposal matches both its semantic payload and the metadata
// captured by the proposal factory. The factory runs this before queueing the
// proposal, keeping full decoding off the Raft orchestrator.
func ValidateFinalProposal(data []byte, declared []CapabilityID) error {
	proposal := new(raftcmdpb.Proposal)
	if err := proposal.UnmarshalVT(data); err != nil {
		return fmt.Errorf("decode final proposal: %w", err)
	}
	expected, err := RequiredCapabilities(proposal)
	if err != nil {
		return err
	}
	wire := make([]CapabilityID, len(proposal.GetRequiredCapabilities()))
	for i, id := range proposal.GetRequiredCapabilities() {
		wire[i] = CapabilityID(id)
	}
	wire, err = Normalize(wire)
	if err != nil {
		return err
	}
	declared, err = Normalize(declared)
	if err != nil {
		return err
	}
	if !slices.Equal(wire, expected) {
		return fmt.Errorf("%w: wire declaration does not match semantic payload", ErrMissingCapability)
	}
	if !slices.Equal(wire, declared) {
		return fmt.Errorf("%w: declaration does not match final payload", ErrMissingCapability)
	}

	return nil
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

// ActiveIDs returns the active capability IDs from a committed policy in
// deterministic order. Nil activations and inactive entries are ignored so
// callers can pass the result directly to membership admission gates.
func ActiveIDs(policy *commonpb.ClusterPolicy) []uint32 {
	if policy == nil {
		return nil
	}

	active := make([]uint32, 0, len(policy.GetCapabilityActivations()))
	for _, activation := range policy.GetCapabilityActivations() {
		if activation != nil && activation.GetActive() {
			active = append(active, activation.GetId())
		}
	}
	slices.Sort(active)

	return active
}

// ValidateSupportedActive refuses active capabilities this binary
// cannot enforce. It is used at boot and after installing a leader checkpoint.
func ValidateSupportedActive(policy *commonpb.ClusterPolicy, supported []CapabilityID) error {
	for _, id := range ActiveIDs(policy) {
		if !slices.Contains(supported, CapabilityID(id)) {
			return fmt.Errorf("active capability %d is not supported by this binary", id)
		}
	}

	return nil
}

// IDsAsUint32 converts the typed capability identifiers to their protobuf wire
// representation at an API boundary.
func IDsAsUint32(ids []CapabilityID) []uint32 {
	result := make([]uint32, len(ids))
	for i, id := range ids {
		result[i] = uint32(id)
	}

	return result
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

// ValidateActive verifies that every active capability is acknowledged by
// every current member and is implemented by this binary.
func ValidateActive(supported, active []CapabilityID, members []Member) error {
	if len(active) == 0 {
		return nil
	}

	ready, err := Ready(supported, members)
	if err != nil {
		return fmt.Errorf("validating capability activation evidence: %w", err)
	}
	for _, required := range active {
		if !slices.Contains(ready, required) {
			return fmt.Errorf("%w: capability %d is not acknowledged by every current member", ErrCapabilityUnsupported, required)
		}
	}

	return nil
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

// Package attribution validates and freezes caller attribution at trust
// boundaries. A Capability can only be obtained from a structurally valid
// CallerSnapshot and always returns a clone, preventing later mutation of the
// value admitted into Raft.
package attribution

import (
	"errors"
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Capability is proof that a caller snapshot passed the deterministic
// attribution checks. Its zero value is invalid.
type Capability struct {
	snapshot *commonpb.CallerSnapshot
}

// SystemActor identifies an allowlisted internal producer.
type SystemActor string

// New validates and freezes a snapshot obtained from authenticated request
// state, a trusted peer forward, or an allowlisted system producer.
func New(snapshot *commonpb.CallerSnapshot) (Capability, error) {
	if err := Validate(snapshot); err != nil {
		return Capability{}, err
	}

	return Capability{snapshot: snapshot.CloneVT()}, nil
}

// NewSystem constructs attribution for an allowlisted internal producer.
func NewSystem(component SystemActor) (Capability, error) {
	return New(&commonpb.CallerSnapshot{
		Principal: &commonpb.CallerSnapshot_System{
			System: &commonpb.SystemCaller{Component: string(component)},
		},
	})
}

// Snapshot returns an isolated copy suitable for attaching to a proposal.
func (c Capability) Snapshot() *commonpb.CallerSnapshot {
	if c.snapshot == nil {
		return nil
	}

	return c.snapshot.CloneVT()
}

// Validate checks only replicated data and is therefore safe to run both at
// admission and in the FSM on every replica.
func Validate(snapshot *commonpb.CallerSnapshot) error {
	invalid := func(detail string) error {
		return &domain.ErrInvalidCallerAttribution{Detail: detail}
	}

	if snapshot == nil || snapshot.GetPrincipal() == nil {
		return invalid("missing principal")
	}

	switch principal := snapshot.GetPrincipal().(type) {
	case *commonpb.CallerSnapshot_Authenticated:
		caller := principal.Authenticated
		if caller == nil || caller.GetIdentity() == nil {
			return invalid("authenticated principal has no identity")
		}
		switch source := caller.GetIdentity().GetSource().(type) {
		case *commonpb.CallerIdentity_Issuer:
			if source.Issuer == "" {
				return invalid("authenticated issuer is empty")
			}
		case *commonpb.CallerIdentity_KeyId:
			if source.KeyId == "" {
				return invalid("authenticated key id is empty")
			}
		default:
			return invalid("authenticated credential source is missing")
		}
		if err := validateScopes(caller.GetScopes()); err != nil {
			return invalid(err.Error())
		}
	case *commonpb.CallerSnapshot_Anonymous:
		if principal.Anonymous == nil {
			return invalid("anonymous principal is missing")
		}
		if err := validateScopes(principal.Anonymous.GetScopes()); err != nil {
			return invalid(err.Error())
		}
	case *commonpb.CallerSnapshot_System:
		if principal.System == nil || !isAllowedSystemComponent(principal.System.GetComponent()) {
			return invalid("system component is missing or not allowlisted")
		}
	case *commonpb.CallerSnapshot_AuthDisabled:
		if principal.AuthDisabled == nil {
			return invalid("authentication-disabled principal is missing")
		}
	default:
		return invalid("unknown principal")
	}

	return nil
}

func validateScopes(scopes []string) error {
	if !slices.IsSorted(scopes) {
		return errors.New("scopes are not sorted")
	}
	for i, scope := range scopes {
		if scope == "" {
			return errors.New("scope is empty")
		}
		if i > 0 && scopes[i-1] == scope {
			return fmt.Errorf("scope %q is duplicated", scope)
		}
	}

	return nil
}

func isAllowedSystemComponent(component string) bool {
	switch SystemActor(component) {
	case ComponentQueryCheckpoint,
		ComponentMirror,
		ComponentEventsSink,
		ComponentClusterConfig,
		ComponentClusterPolicy,
		ComponentIdempotencyEvict,
		ComponentBackup:
		return true
	default:
		return false
	}
}

// System component identifiers are the complete allowlist of internal
// producers permitted to bypass user authentication.
const (
	ComponentQueryCheckpoint  SystemActor = "query-checkpoint-scheduler"
	ComponentMirror           SystemActor = "mirror"
	ComponentEventsSink       SystemActor = "events-sink"
	ComponentClusterConfig    SystemActor = "cluster-config"
	ComponentClusterPolicy    SystemActor = "cluster-policy"
	ComponentIdempotencyEvict SystemActor = "idempotency-eviction"
	ComponentBackup           SystemActor = "backup"
)

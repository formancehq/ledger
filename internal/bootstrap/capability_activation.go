package bootstrap

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/reconciler"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type capabilityActivationReconciler struct {
	*reconciler.Reconciler
}

func activateReadyCapabilities(ctx context.Context, admit func(context.Context, *commonpb.CapabilityActivation) (*domain.ApplyResult, error), store *dal.Store, isLeader func() bool, members func() []capabilities.Member, supported []capabilities.CapabilityID) error {
	if !isLeader() {
		return nil
	}
	policy, err := query.ReadClusterPolicy(store)
	if err != nil {
		return fmt.Errorf("reading committed policy for capability activation: %w", err)
	}
	states := make(map[uint32]bool)
	for _, activation := range policy.GetCapabilityActivations() {
		states[activation.GetId()] = activation.GetActive() || activation.GetRequested()
	}
	ready, err := capabilities.Ready(supported, members())
	if err != nil {
		return err
	}
	for _, id := range ready {
		if !states[uint32(id)] {
			if _, err := admit(ctx, &commonpb.CapabilityActivation{Id: uint32(id), Requested: true}); err != nil {
				return err
			}
		}
	}
	policy, err = query.ReadClusterPolicy(store)
	if err != nil {
		return err
	}
	for _, activation := range policy.GetCapabilityActivations() {
		if activation.GetActive() || !activation.GetRequested() {
			continue
		}
		if err := capabilities.ValidateActive(supported, []capabilities.CapabilityID{capabilities.CapabilityID(activation.GetId())}, members()); err != nil {
			return err
		}
		if _, err := admit(ctx, &commonpb.CapabilityActivation{Id: activation.GetId(), Active: true}); err != nil {
			return err
		}
	}

	return nil
}

package ledger

import (
	"context"

	"github.com/formancehq/ledger/internal/machine"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/pkg/errors"
)

type MachineResult struct {
	Postings        ledger.Postings   `json:"postings"`
	Metadata        metadata.Metadata `json:"metadata"`
	AccountMetadata map[string]metadata.Metadata
}

//go:generate mockgen -source machine.go -destination machine_generated.go -package ledger . Machine
type Machine interface {
	Execute(context.Context, vm.Store, map[string]string) (*MachineResult, error)
}

type DefaultMachineAdapter struct {
	program program.Program
	machine *vm.Machine
}

func (d *DefaultMachineAdapter) Execute(ctx context.Context, store vm.Store, vars map[string]string) (*MachineResult, error) {

	d.machine = vm.NewMachine(d.program)

	// notes(gfyrag): machines modify the map, copy it to keep our original parameters unchanged
	varsCopy := make(map[string]string)
	for k, v := range vars {
		varsCopy[k] = v
	}

	if err := d.machine.SetVarsFromJSON(varsCopy); err != nil {
		return nil, errors.Wrap(err, "failed to set vars from JSON")
	}
	err := d.machine.ResolveResources(ctx, store)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve resources")
	}

	if err := d.machine.ResolveBalances(ctx, store); err != nil {
		return nil, errors.Wrap(err, "failed to resolve balances")
	}

	if err := d.machine.Execute(); err != nil {
		switch {
		case errors.Is(err, &machine.ErrMetadataOverride{}):
			errMetadataOverride := &machine.ErrMetadataOverride{}
			_ = errors.As(err, &errMetadataOverride)
			return nil, newErrMetadataOverride(errMetadataOverride.Key())
		default:
			return nil, errors.Wrap(err, "failed to execute machine")
		}
	}

	return &MachineResult{
		Postings: collectionutils.Map(d.machine.Postings, func(from vm.Posting) ledger.Posting {
			return ledger.Posting{
				Source:      from.Source,
				Destination: from.Destination,
				Amount:      from.Amount.ToBigInt(),
				Asset:       from.Asset,
			}
		}),
		Metadata:        d.machine.GetTxMetaJSON(),
		AccountMetadata: d.machine.GetAccountsMetaJSON(),
	}, nil
}

func NewDefaultMachine(p program.Program) *DefaultMachineAdapter {
	return &DefaultMachineAdapter{
		program: p,
	}
}

var _ Machine = (*DefaultMachineAdapter)(nil)

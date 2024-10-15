package ledger

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/internal/machine"

	"errors"

	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/ledger/internal/machine/vm/program"
)

type NumscriptExecutionResult struct {
	Postings        ledger.Postings   `json:"postings"`
	Metadata        metadata.Metadata `json:"metadata"`
	AccountMetadata map[string]metadata.Metadata
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source machine.go -destination machine_generated_test.go -package ledger . Machine
type NumscriptRuntime interface {
	Execute(context.Context, TX, map[string]string) (*NumscriptExecutionResult, error)
}

type MachineNumscriptRuntimeAdapter struct {
	program program.Program
	machine *vm.Machine
}

func (d *MachineNumscriptRuntimeAdapter) Execute(ctx context.Context, tx TX, vars map[string]string) (*NumscriptExecutionResult, error) {
	store := newVmStoreAdapter(tx)

	d.machine = vm.NewMachine(d.program)

	// notes(gfyrag): machines modify the map, copy it to keep our original parameters unchanged
	varsCopy := make(map[string]string)
	for k, v := range vars {
		varsCopy[k] = v
	}

	if err := d.machine.SetVarsFromJSON(varsCopy); err != nil {
		return nil, fmt.Errorf("failed to set vars from JSON: %w", err)
	}
	err := d.machine.ResolveResources(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve resources: %w", err)
	}

	if err := d.machine.ResolveBalances(ctx, store); err != nil {
		return nil, fmt.Errorf("failed to resolve balances: %w", err)
	}

	if err := d.machine.Execute(); err != nil {
		switch {
		case errors.Is(err, &machine.ErrMetadataOverride{}):
			errMetadataOverride := &machine.ErrMetadataOverride{}
			_ = errors.As(err, &errMetadataOverride)
			return nil, newErrMetadataOverride(errMetadataOverride.Key())
		default:
			return nil, fmt.Errorf("failed to execute machine: %w", err)
		}
	}

	return &NumscriptExecutionResult{
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

func NewMachineNumscriptRuntimeAdapter(p program.Program) *MachineNumscriptRuntimeAdapter {
	return &MachineNumscriptRuntimeAdapter{
		program: p,
	}
}

var _ NumscriptRuntime = (*MachineNumscriptRuntimeAdapter)(nil)

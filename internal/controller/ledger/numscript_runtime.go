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

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source numscript_runtime.go -destination numscript_runtime_generated_test.go -package ledger . NumscriptRuntime
type NumscriptRuntime interface {
	Execute(context.Context, TX, map[string]string) (*NumscriptExecutionResult, error)
}

type MachineNumscriptRuntimeAdapter struct {
	program program.Program
}

func (d *MachineNumscriptRuntimeAdapter) Execute(ctx context.Context, tx TX, vars map[string]string) (*NumscriptExecutionResult, error) {
	store := newVmStoreAdapter(tx)

	machineInstance := vm.NewMachine(d.program)

	// notes(gfyrag): machines modify the map, copy it to keep our original parameters unchanged
	varsCopy := make(map[string]string)
	for k, v := range vars {
		varsCopy[k] = v
	}

	if err := machineInstance.SetVarsFromJSON(varsCopy); err != nil {
		return nil, fmt.Errorf("failed to set vars from JSON: %w", err)
	}
	err := machineInstance.ResolveResources(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve resources: %w", err)
	}

	if err := machineInstance.ResolveBalances(ctx, store); err != nil {
		return nil, fmt.Errorf("failed to resolve balances: %w", err)
	}

	if err := machineInstance.Execute(); err != nil {
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
		Postings: collectionutils.Map(machineInstance.Postings, func(from vm.Posting) ledger.Posting {
			return ledger.Posting{
				Source:      from.Source,
				Destination: from.Destination,
				Amount:      from.Amount.ToBigInt(),
				Asset:       from.Asset,
			}
		}),
		Metadata:        machineInstance.GetTxMetaJSON(),
		AccountMetadata: machineInstance.GetAccountsMetaJSON(),
	}, nil
}

func NewMachineNumscriptRuntimeAdapter(p program.Program) *MachineNumscriptRuntimeAdapter {
	return &MachineNumscriptRuntimeAdapter{
		program: p,
	}
}

var _ NumscriptRuntime = (*MachineNumscriptRuntimeAdapter)(nil)

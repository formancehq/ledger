package ledger

import (
	"context"
	"fmt"

	"errors"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/formancehq/numscript"
)

type NumscriptExecutionResult struct {
	Postings        ledger.Postings   `json:"postings"`
	Metadata        metadata.Metadata `json:"metadata"`
	AccountMetadata map[string]metadata.Metadata
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source numscript_runtime.go -destination numscript_runtime_generated_test.go -package ledger . NumscriptRuntime
type NumscriptRuntime interface {
	Execute(context.Context, Store, map[string]string) (*NumscriptExecutionResult, error)
}

type MachineNumscriptRuntimeAdapter struct {
	program program.Program
}

func (d *MachineNumscriptRuntimeAdapter) Execute(ctx context.Context, store Store, vars map[string]string) (*NumscriptExecutionResult, error) {
	storeAdapter := newVmStoreAdapter(store)

	machineInstance := vm.NewMachine(d.program)

	// notes(gfyrag): machines modify the map, copy it to keep our original parameters unchanged
	varsCopy := make(map[string]string)
	for k, v := range vars {
		varsCopy[k] = v
	}

	if err := machineInstance.SetVarsFromJSON(varsCopy); err != nil {
		return nil, fmt.Errorf("failed to set vars from JSON: %w", err)
	}
	err := machineInstance.ResolveResources(ctx, storeAdapter)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve resources: %w", err)
	}

	if err := machineInstance.ResolveBalances(ctx, storeAdapter); err != nil {
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

// numscript rewrite implementation
var _ NumscriptRuntime = (*DefaultInterpreterMachineAdapter)(nil)

type DefaultInterpreterMachineAdapter struct {
	parseResult  numscript.ParseResult
	featureFlags map[string]struct{}
}

func NewDefaultInterpreterMachineAdapter(parseResult numscript.ParseResult, featureFlags map[string]struct{}) *DefaultInterpreterMachineAdapter {
	return &DefaultInterpreterMachineAdapter{
		parseResult:  parseResult,
		featureFlags: featureFlags,
	}
}

func (d *DefaultInterpreterMachineAdapter) Execute(ctx context.Context, store Store, vars map[string]string) (*NumscriptExecutionResult, error) {
	execResult, err := d.parseResult.RunWithFeatureFlags(ctx, vars, newNumscriptRewriteAdapter(store), d.featureFlags)
	if err != nil {
		return nil, ErrRuntime{
			Source: d.parseResult.GetSource(),
			Inner:  err,
		}
	}

	return &NumscriptExecutionResult{
		Postings: collectionutils.Map(execResult.Postings, func(posting numscript.Posting) ledger.Posting {
			return ledger.Posting(posting)
		}),
		Metadata:        castMetadata(execResult.Metadata),
		AccountMetadata: castAccountsMetadata(execResult.AccountsMetadata),
	}, nil
}

func castMetadata(numscriptMeta numscript.Metadata) metadata.Metadata {
	meta := metadata.Metadata{}
	for k, v := range numscriptMeta {
		meta[k] = v.String()
	}
	return meta
}

func castAccountsMetadata(numscriptAccountsMetadata numscript.AccountsMetadata) map[string]metadata.Metadata {
	m := make(map[string]metadata.Metadata)
	for k, v := range numscriptAccountsMetadata {
		m[k] = v
	}
	return m

}

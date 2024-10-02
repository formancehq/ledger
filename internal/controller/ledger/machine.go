package ledger

import (
	"context"
	"fmt"

	"errors"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/formancehq/numscript"
)

type MachineResult struct {
	Postings        ledger.Postings   `json:"postings"`
	Metadata        metadata.Metadata `json:"metadata"`
	AccountMetadata map[string]metadata.Metadata
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source machine.go -destination machine_generated_test.go -package ledger . Machine
type Machine interface {
	Execute(context.Context, TX, map[string]string) (*MachineResult, error)
}

type DefaultMachineAdapter struct {
	program program.Program
	machine *vm.Machine
}

func (d *DefaultMachineAdapter) Execute(ctx context.Context, tx TX, vars map[string]string) (*MachineResult, error) {
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

// numscript rewrite implementation
var _ Machine = (*DefaultInterpreterMachineAdapter)(nil)

type DefaultInterpreterMachineAdapter struct {
	parseResult numscript.ParseResult
}

func NewDefaultInterpreterMachineAdapter(parseResult numscript.ParseResult) *DefaultInterpreterMachineAdapter {
	return &DefaultInterpreterMachineAdapter{
		parseResult: parseResult,
	}
}

func (d *DefaultInterpreterMachineAdapter) Execute(ctx context.Context, tx TX, vars map[string]string) (*MachineResult, error) {
	execResult, err := d.parseResult.Run(ctx, vars, newNumscriptRewriteAdapter(tx))
	if err != nil {
		return nil, err
	}

	return &MachineResult{
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

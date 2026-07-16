package processing

import (
	"context"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// numscriptVMStoreAdapter adapts the coverage-gated Scope to numscript's
// bytecode VM store interface (numscriptlib.VMStore). Unlike the interpreter
// store (batched queries), the VM fetches balances and metadata lazily, one
// (account, asset) / (account, key) at a time. The reads go through the same
// coverage-gated Scope accessors as the interpreter adapter, so the FSM read
// horizon is identical.
type numscriptVMStoreAdapter struct {
	store      Scope
	ledgerName string
	force      bool // When true, return unlimited balances to bypass balance checks
}

func (s *numscriptVMStoreAdapter) GetBalance(_ context.Context, account, asset, _ string) (*big.Int, error) {
	// The color argument has no ledger equivalent (volumes are single-color),
	// so it is ignored — the volume is keyed by (account, asset) only.
	return numscriptBalance(s.store, s.ledgerName, account, asset, s.force)
}

func (s *numscriptVMStoreAdapter) GetMetadata(_ context.Context, account, key string) (string, bool, error) {
	return numscriptAccountMetadata(s.store, s.ledgerName, account, key)
}

// numscriptVMPostingProducer produces postings by executing the bytecode-
// compiled program on numscript's register VM, as an alternative to the
// tree-walking interpreter (numscriptPostingProducer). It is a drop-in
// postingProducer: the two share applyNumscriptResult for the posting→volume
// conversion, so only the execution engine differs. Compilation is memoized in
// the NumscriptCache (GetOrCompile), mirroring the interpreter's memoized parse.
type numscriptVMPostingProducer struct {
	cache      *numscript.NumscriptCache
	ledgerName string
}

func (p *numscriptVMPostingProducer) produce(s Scope, ledgerName string, order *raftcmdpb.CreateTransactionOrder, script *commonpb.Script) (*produceResult, domain.Describable) {
	if script == nil || script.GetPlain() == "" {
		return nil, domain.ErrScriptRequired
	}

	// Compile the script (memoized in the cache) into bytecode + a vars encoder.
	encoder, program, err := p.cache.GetOrCompile(script.GetPlain())
	if err != nil {
		return nil, err
	}

	// Encode the caller-provided vars into the VM's packed representation.
	vars, encErr := encoder.Encode(script.GetVars())
	if encErr != nil {
		return nil, &domain.ErrNumscriptRuntime{Detail: encErr.Error()}
	}

	// Create the store adapter.
	// When Force is true, the adapter returns unlimited balances to bypass balance checks.
	storeAdapter := &numscriptVMStoreAdapter{
		store:      s,
		ledgerName: ledgerName,
		force:      order.GetForce(),
	}

	// A fresh Vm per call: its register banks hold mutable per-run state and are
	// not safe to share across concurrent executions.
	machine := numscriptlib.NewVm(program)

	result, runErr := numscript.SafeExecVM(context.Background(), machine, &vars, storeAdapter)
	if runErr != nil {
		return nil, runErr
	}

	return applyNumscriptResult(s, ledgerName, result)
}

package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
	"github.com/pkg/errors"
)

type Store interface {
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
}

type Result struct {
	Postings        core.Postings
	Metadata        core.Metadata
	AccountMetadata map[string]core.Metadata
}

func Run(ctx context.Context, store Store, prog *program.Program, script core.RunScript) (*Result, error) {

	m := vm.NewMachine(*prog)

	if err := m.SetVarsFromJSON(script.Vars); err != nil {
		return nil, NewScriptError(ScriptErrorCompilationFailed,
			errors.Wrap(err, "could not set variables").Error())
	}

	resourcesChan, err := m.ResolveResources()
	if err != nil {
		return nil, errors.Wrap(err, "could not resolve program resources")
	}
	for req := range resourcesChan {
		if req.Error != nil {
			return nil, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(req.Error, "could not resolve program resources").Error())
		}
		account, err := store.GetAccountWithVolumes(ctx, req.Account)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("could not get account %q", req.Account))
		}
		if req.Key != "" {
			entry, ok := account.Metadata[req.Key]
			if !ok {
				return nil, NewScriptError(ScriptErrorCompilationFailed,
					fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
			}
			data, err := json.Marshal(entry)
			if err != nil {
				return nil, errors.Wrap(err, "marshaling metadata")
			}
			value, err := core.NewValueFromTypedJSON(data)
			if err != nil {
				return nil, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(err, fmt.Sprintf("invalid format for metadata at key %v for account %v",
						req.Key, req.Account)).Error())
			}
			req.Response <- *value
		} else if req.Asset != "" {
			amt := account.Volumes[req.Asset].Balance().OrZero()
			resp := *amt
			req.Response <- &resp
		} else {
			return nil, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(err, fmt.Sprintf("invalid ResourceRequest: %+v", req)).Error())
		}
	}

	balanceCh, err := m.ResolveBalances()
	if err != nil {
		return nil, errors.Wrap(err, "could not resolve balances")
	}
	for req := range balanceCh {
		if req.Error != nil {
			return nil, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(req.Error, "could not resolve program balances").Error())
		}
		var amt *core.MonetaryInt
		account, err := store.GetAccountWithVolumes(ctx, req.Account)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("could not get account %q", req.Account))
		}
		amt = account.Volumes[req.Asset].Balance().OrZero()
		resp := *amt
		req.Response <- &resp
	}

	exitCode, err := m.Execute()
	if err != nil {
		return nil, errors.Wrap(err, "script execution failed")
	}

	if exitCode != vm.EXIT_OK {
		switch exitCode {
		case vm.EXIT_FAIL:
			return nil, errors.New("script exited with error code EXIT_FAIL")
		case vm.EXIT_FAIL_INVALID:
			return nil, errors.New("internal error: compiled script was invalid")
		case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
			// TODO: If the machine can provide the asset which is failing
			// we should be able to use InsufficientFundError{} instead of error code
			return nil, NewScriptError(ScriptErrorInsufficientFund,
				"account had insufficient funds")
		default:
			return nil, errors.New("script execution failed")
		}
	}

	result := Result{
		Postings:        make([]core.Posting, len(m.Postings)),
		Metadata:        map[string]any{},
		AccountMetadata: map[string]core.Metadata{},
	}

	for j, posting := range m.Postings {
		result.Postings[j] = core.Posting{
			Source:      posting.Source,
			Destination: posting.Destination,
			Amount:      posting.Amount,
			Asset:       posting.Asset,
		}
	}

	for k, v := range m.GetTxMetaJSON() {
		asMapAny := make(map[string]any)
		if err := json.Unmarshal(v.([]byte), &asMapAny); err != nil {
			return nil, errors.Wrap(err, "unmarshaling transaction metadata")
		}
		result.Metadata[k] = asMapAny
	}

	for k, v := range script.Metadata {
		_, ok := result.Metadata[k]
		if ok {
			return nil, NewScriptError(ScriptErrorMetadataOverride, "cannot override metadata from script")
		}
		result.Metadata[k] = v
	}

	for account, meta := range m.GetAccountsMetaJSON() {
		meta := meta.(map[string][]byte)
		for k, v := range meta {
			asMapAny := make(map[string]any)
			if err := json.Unmarshal(v, &asMapAny); err != nil {
				return nil, errors.Wrap(err, "unmarshaling account metadata")
			}
			if account[0] == '@' {
				account = account[1:]
			}
			if _, ok := result.AccountMetadata[account]; !ok {
				result.AccountMetadata[account] = core.Metadata{}
			}
			result.AccountMetadata[account][k] = asMapAny
		}
	}

	return &result, nil
}

package machine

import (
	"context"
	"encoding/json"
	"math/big"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
)

type Store interface {
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
	LockAccounts(ctx context.Context, address ...string) (func(), error)
}

type Result struct {
	Postings        core.Postings
	Metadata        core.Metadata
	AccountMetadata map[string]core.Metadata
}

func Run(m *vm.Machine, script core.RunScript) (*Result, error) {
	err := m.Execute()
	if err != nil {
		return nil, errors.Wrap(err, "script execution failed")
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
			Amount:      (*big.Int)(posting.Amount),
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
			return nil, errorsutil.NewError(vm.ErrMetadataOverride,
				errors.New("cannot override metadata from script"))
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

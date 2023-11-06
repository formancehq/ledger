package machine

import (
	"math/big"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Result struct {
	Postings        ledger.Postings
	Metadata        metadata.Metadata
	AccountMetadata map[string]metadata.Metadata
}

func Run(m *vm.Machine, script ledger.RunScript) (*Result, error) {
	err := m.Execute()
	if err != nil {
		return nil, errors.Wrap(err, "script execution failed")
	}

	result := Result{
		Postings:        make([]ledger.Posting, len(m.Postings)),
		Metadata:        m.GetTxMetaJSON(),
		AccountMetadata: m.GetAccountsMetaJSON(),
	}

	for j, posting := range m.Postings {
		result.Postings[j] = ledger.Posting{
			Source:      posting.Source,
			Destination: posting.Destination,
			Amount:      (*big.Int)(posting.Amount),
			Asset:       posting.Asset,
		}
	}

	for k, v := range script.Metadata {
		_, ok := result.Metadata[k]
		if ok {
			return nil, errorsutil.NewError(vm.ErrMetadataOverride,
				errors.New("cannot override metadata from script"))
		}
		result.Metadata[k] = v
	}

	return &result, nil
}

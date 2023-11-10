package vm

import (
	"math/big"

	"github.com/formancehq/ledger/internal/machine"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Result struct {
	Postings        ledger.Postings
	Metadata        metadata.Metadata
	AccountMetadata map[string]metadata.Metadata
}

func Run(m *Machine, script ledger.RunScript) (*Result, error) {
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
			return nil, machine.NewErrMetadataOverride(k)
		}
		result.Metadata[k] = v
	}

	return &result, nil
}

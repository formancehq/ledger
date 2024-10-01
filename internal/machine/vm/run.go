package vm

import (
	"fmt"
	"github.com/formancehq/go-libs/time"
	"math/big"

	"github.com/formancehq/ledger/internal/machine"

	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
)

type RunScript struct {
	Script
	Timestamp time.Time         `json:"timestamp"`
	Metadata  metadata.Metadata `json:"metadata"`
	Reference string            `json:"reference"`
}

type Script struct {
	Plain string            `json:"plain"`
	Vars  map[string]string `json:"vars" swaggertype:"object"`
}

type ScriptV1 struct {
	Script
	Vars map[string]any `json:"vars"`
}

func (s ScriptV1) ToCore() Script {
	s.Script.Vars = map[string]string{}
	for k, v := range s.Vars {
		switch v := v.(type) {
		case string:
			s.Script.Vars[k] = v
		case map[string]any:
			s.Script.Vars[k] = fmt.Sprintf("%s %v", v["asset"], v["amount"])
		default:
			s.Script.Vars[k] = fmt.Sprint(v)
		}
	}
	return s.Script
}

type Result struct {
	Postings        ledger.Postings
	Metadata        metadata.Metadata
	AccountMetadata map[string]metadata.Metadata
}

func Run(m *Machine, script RunScript) (*Result, error) {
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

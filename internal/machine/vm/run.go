package vm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine"
)

type RunScript struct {
	Script
	Timestamp time.Time         `json:"timestamp"`
	Metadata  metadata.Metadata `json:"metadata"`
	Reference string            `json:"reference"`
}

type Script struct {
	Plain    string            `json:"plain,omitempty"`
	Template string            `json:"template,omitempty"`
	Vars     map[string]string `json:"vars" swaggertype:"object"`
}

type ScriptV1 struct {
	Script
	Vars map[string]any `json:"vars"`
}

// ErrInvalidMonetaryAmount is returned when a v1 monetary amount cannot be
// represented as an exact integer.
var ErrInvalidMonetaryAmount = errors.New("invalid monetary amount")

func (s *ScriptV1) UnmarshalJSON(data []byte) error {
	type scriptV1Alias ScriptV1
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var decoded scriptV1Alias
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*s = ScriptV1(decoded)
	return nil
}

func (s ScriptV1) ToCore() (Script, error) {
	s.Script.Vars = map[string]string{}
	for k, v := range s.Vars {
		switch v := v.(type) {
		case string:
			s.Script.Vars[k] = v
		case map[string]any:
			switch amount := v["amount"].(type) {
			case string:
				s.Script.Vars[k] = fmt.Sprintf("%s %s", v["asset"], amount)
			case json.Number:
				rational, ok := new(big.Rat).SetString(string(amount))
				if !ok || !rational.IsInt() {
					return Script{}, fmt.Errorf("%w for variable %q: amount must be an integer", ErrInvalidMonetaryAmount, k)
				}
				s.Script.Vars[k] = fmt.Sprintf("%s %s", v["asset"], rational.Num().String())
			case float64:
				const maxSafeInteger = 1<<53 - 1
				if amount != math.Trunc(amount) || math.Abs(amount) > maxSafeInteger {
					return Script{}, fmt.Errorf("%w for variable %q: floating-point amount must be an integer within the safe range", ErrInvalidMonetaryAmount, k)
				}
				s.Script.Vars[k] = fmt.Sprintf("%s %s", v["asset"], strconv.FormatInt(int64(amount), 10))
			}
		default:
			s.Script.Vars[k] = fmt.Sprint(v)
		}
	}
	return s.Script, nil
}

type Result struct {
	Postings        ledger.Postings
	Metadata        metadata.Metadata
	AccountMetadata map[string]metadata.Metadata
}

func Run(m *Machine, script RunScript) (*Result, error) {
	err := m.Execute()
	if err != nil {
		return nil, fmt.Errorf("script execution failed: %w", err)
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

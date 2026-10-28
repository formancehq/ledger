package vm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

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

// The former float64 JSON decoder accepted at most 309 decimal integer digits.
// Keep exponent expansion bounded to that scale; decimal strings remain available
// for larger amounts. Never construct an exponent-sized big.Rat or denominator.
const maxLegacyJSONIntegerDigits = 309

var legacyJSONNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func legacyJSONInteger(value json.Number) (string, error) {
	raw := string(value)
	if !legacyJSONNumberPattern.MatchString(raw) {
		return "", errors.New("amount must be an integer")
	}
	negative := strings.HasPrefix(raw, "-")
	raw = strings.TrimPrefix(raw, "-")
	mantissa, exponentText := raw, "0"
	if index := strings.IndexAny(raw, "eE"); index >= 0 {
		mantissa, exponentText = raw[:index], raw[index+1:]
	}
	fractionDigits := 0
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		fractionDigits = len(mantissa) - index - 1
		mantissa = mantissa[:index] + mantissa[index+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return "0", nil
	}
	exponent, err := strconv.ParseInt(exponentText, 10, 64)
	if err != nil || exponent > int64(len(raw))+maxLegacyJSONIntegerDigits || exponent < -int64(len(raw)) {
		return "", errors.New("amount must be an integer within the JSON numeric size limit")
	}
	scale := exponent - int64(fractionDigits)
	integerDigits := int64(len(digits)) + scale
	if integerDigits > maxLegacyJSONIntegerDigits {
		return "", fmt.Errorf("amount exceeds %d decimal digits; use a decimal string", maxLegacyJSONIntegerDigits)
	}
	if integerDigits <= 0 {
		return "", errors.New("amount must be an integer")
	}
	if scale < 0 {
		if strings.Trim(digits[integerDigits:], "0") != "" {
			return "", errors.New("amount must be an integer")
		}
		digits = digits[:integerDigits]
	} else {
		// The digit-count check above bounds this allocation before it happens.
		digits += strings.Repeat("0", int(scale))
	}
	if negative {
		digits = "-" + digits
	}
	return digits, nil
}

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
		case json.Number:
			// Preserve the old scalar representation for non-integral values,
			// while accepting exact integral decimal and exponent notation.
			s.Script.Vars[k] = string(v)
			if integer, err := legacyJSONInteger(v); err == nil {
				s.Script.Vars[k] = integer
			}
		case map[string]any:
			switch amount := v["amount"].(type) {
			case string:
				s.Script.Vars[k] = fmt.Sprintf("%s %s", v["asset"], amount)
			case json.Number:
				integer, err := legacyJSONInteger(amount)
				if err != nil {
					return Script{}, fmt.Errorf("%w for variable %q: %s", ErrInvalidMonetaryAmount, k, err)
				}
				s.Script.Vars[k] = fmt.Sprintf("%s %s", v["asset"], integer)
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

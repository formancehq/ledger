package numscript

import (
	"encoding/json"

	numscriptlib "github.com/formancehq/numscript"
)

// ValueToString converts a Numscript value produced by set_account_meta /
// set_tx_meta into the raw string Ledger stores as metadata.
//
// The upstream library serialises values in a tagged-JSON discriminated union
// ({"type":"string","value":"abc"}, {"type":"number","value":"42"}, …) and its
// Value.String() wraps string values in quotes ("abc"). Ledger stores the raw
// client-facing bytes, so for scalar (string/number) values we return the inner
// `value` field verbatim; for every other shape (monetary, portion, asset,
// account) the canonical String() form is used, matching the pre-EN-1406
// behaviour of the interpreter's per-type String().
func ValueToString(v numscriptlib.Value) (string, error) {
	if v == nil {
		return "", nil
	}

	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	var tagged struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &tagged); err != nil {
		return "", err
	}

	switch tagged.Type {
	case "string", "number":
		return tagged.Value, nil
	default:
		return v.String(), nil
	}
}

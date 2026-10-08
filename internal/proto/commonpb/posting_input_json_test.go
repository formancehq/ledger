package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

func TestPostingAmountJSONInput(t *testing.T) {
	t.Parallel()

	for _, decimal := range []string{
		"0", "9007199254740991", "9007199254740992", "9007199254740993",
		"123456789012345678901234567890",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935",
	} {
		for _, quoted := range []bool{false, true} {
			t.Run(decimal+map[bool]string{false: "/number", true: "/string"}[quoted], func(t *testing.T) {
				t.Parallel()
				token := decimal
				if quoted {
					token = `"` + decimal + `"`
				}
				var posting Posting
				err := json.Unmarshal([]byte(`{"source":"world","destination":"bank","asset":"USD","color":"blue","amount":`+token+`}`), &posting)
				require.NoError(t, err)
				require.Equal(t, decimal, posting.GetAmount().Dec())
				require.Equal(t, "world", posting.GetSource())
				require.Equal(t, "bank", posting.GetDestination())
				require.Equal(t, "USD", posting.GetAsset())
				require.Equal(t, "blue", posting.GetColor())
				for _, stringResponse := range []bool{false, true} {
					options := MonetaryAmountsAsNumbers()
					amountJSON := decimal
					if stringResponse {
						options = MonetaryAmountsAsStrings()
						amountJSON = `"` + decimal + `"`
					}
					encoded, err := json.MarshalWithOptions(&posting, options)
					require.NoError(t, err)
					require.Contains(t, string(encoded), `"amount":`+amountJSON)
					var roundTrip Posting
					require.NoError(t, json.Unmarshal(encoded, &roundTrip))
					require.Equal(t, decimal, roundTrip.GetAmount().Dec())
				}
			})
		}
	}
}

func TestPostingAmountJSONRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	for _, token := range []string{
		`-1`, `1.5`, `1e2`, `"-1"`, `"+1"`, `"01"`, `"1.0"`, `"1e2"`,
		`"0x10"`, `""`, `" 1"`, `"1 "`, `true`, `{}`, `[]`,
		`115792089237316195423570985008687907853269984665640564039457584007913129639936`,
		`"115792089237316195423570985008687907853269984665640564039457584007913129639936"`,
	} {
		t.Run(token, func(t *testing.T) {
			t.Parallel()
			var posting Posting
			require.Error(t, json.Unmarshal([]byte(`{"amount":`+token+`}`), &posting))
		})
	}
}

package commonpb

import (
	stdjson "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNestedPostingMarshalOption(t *testing.T) {
	t.Parallel()

	posting := &Posting{Source: "world", Destination: "bank", Asset: "USD", Amount: NewUint256FromUint64(9007199254740993)}
	transaction := &Transaction{Postings: []*Posting{posting}}
	options := jsonv2.WithMarshalers(jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, p *Posting) error {
		return enc.WriteValue(jsontext.Value(`{"amount":"` + p.GetAmount().Dec() + `"}`))
	}))

	for name, value := range map[string]any{
		"transaction": transaction,
		"map":         map[string]any{"nested": []*Transaction{transaction}},
		"cursor":      &PreparedQueryCursor{TransactionData: []*Transaction{transaction}},
		"ledgerLog":   NewLedgerLog(&LedgerLogPayload{Payload: &LedgerLogPayload_CreatedTransaction{CreatedTransaction: &CreatedTransaction{Transaction: transaction}}}),
		"systemLog": &Log{Payload: &LogPayload{Type: &LogPayload_Apply{Apply: &ApplyLedgerLog{
			Log: NewLedgerLog(&LedgerLogPayload{Payload: &LedgerLogPayload_CreatedTransaction{CreatedTransaction: &CreatedTransaction{Transaction: transaction}}}),
		}}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, err := jsonv2.Marshal(value, stdjson.DefaultOptionsV1(), options)
			require.NoError(t, err)
			require.Contains(t, string(encoded), `"amount":"9007199254740993"`)
			defaultEncoded, err := jsonv2.Marshal(value, stdjson.DefaultOptionsV1())
			require.NoError(t, err)
			require.Contains(t, string(defaultEncoded), `"amount":9007199254740993`)
		})
	}
}

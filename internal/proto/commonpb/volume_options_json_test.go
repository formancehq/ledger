package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

func TestVolumeAmountEncodingOptions(t *testing.T) {
	t.Parallel()
	const huge = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	input := MustBigUintFromDecimal(huge)
	output := MustBigUintFromDecimal(huge + "0")
	volumes := &Volumes{Input: input, Output: output}
	balance, err := volumes.Balance()
	require.NoError(t, err)
	withBalance := &VolumesWithBalance{Input: input, Output: output, Balance: NewSignedBigInt(balance)}
	postcommit := &PostCommitVolumes{VolumesByAccount: map[string]*VolumesByAssets{"world": {Volumes: []*VolumeEntry{{Asset: "USD", Color: "", Volumes: volumes}}}}}
	account := &Account{Address: "world", Volumes: []*AccountVolume{{Asset: "USD", Color: "", Volumes: withBalance}}}
	transaction := &Transaction{PostCommitVolumes: postcommit, Postings: []*Posting{{Amount: NewUint256FromUint64(9007199254740993)}}}
	ledgerLog := NewLedgerLog(&LedgerLogPayload{Payload: &LedgerLogPayload_CreatedTransaction{CreatedTransaction: &CreatedTransaction{Transaction: transaction}}})
	for name, value := range map[string]any{
		"volumes": volumes, "balance": withBalance, "account": account,
		"accountCursor": &PreparedQueryCursor{AccountData: []*Account{account}},
		"postcommit":    postcommit, "transaction": transaction,
		"transactionCursor": &PreparedQueryCursor{TransactionData: []*Transaction{transaction}},
		"ledgerLog":         ledgerLog,
		"systemLog":         &Log{Payload: &LogPayload{Type: &LogPayload_Apply{Apply: &ApplyLedgerLog{Log: ledgerLog}}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			numeric, err := json.MarshalWithOptions(value, MonetaryAmountsAsNumbers())
			require.NoError(t, err)
			require.Contains(t, string(numeric), `"input":`+huge)
			require.Contains(t, string(numeric), `"output":`+huge+"0")
			quoted, err := json.MarshalWithOptions(value, MonetaryAmountsAsStrings())
			require.NoError(t, err)
			require.Contains(t, string(quoted), `"input":"`+huge+`"`)
			if name == "transaction" || name == "transactionCursor" || name == "ledgerLog" || name == "systemLog" {
				require.Contains(t, string(numeric), `"amount":9007199254740993`)
				require.Contains(t, string(quoted), `"amount":"9007199254740993"`)
			}
			legacy, err := json.Marshal(value)
			require.NoError(t, err)
			if name != "transaction" && name != "transactionCursor" && name != "ledgerLog" && name != "systemLog" {
				require.JSONEq(t, string(legacy), string(quoted))
			} else {
				require.Contains(t, string(legacy), `"input":"`+huge+`"`)
				require.Contains(t, string(legacy), `"amount":9007199254740993`)
			}
			if name == "balance" || name == "account" || name == "accountCursor" {
				require.Contains(t, string(numeric), `"balance":`+balance.String())
				require.Contains(t, string(quoted), `"balance":"`+balance.String()+`"`)
			}
		})
	}
}

func TestVolumeAmountEncodingValidation(t *testing.T) {
	t.Parallel()
	for name, value := range map[string]any{
		"missingContainer": &VolumeEntry{},
		"missingInput":     &Volumes{Output: &BigUint{}},
		"invalidMagnitude": &BigUint{Magnitude: []byte{0, 1}},
		"negativeZero":     &SignedBigInt{Negative: true},
		"wrongBalance":     &VolumesWithBalance{Input: MustBigUintFromDecimal("1"), Output: &BigUint{}, Balance: &SignedBigInt{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := json.MarshalWithOptions(value, MonetaryAmountsAsNumbers())
			require.Error(t, err)
			_, err = json.MarshalWithOptions(value, MonetaryAmountsAsStrings())
			require.Error(t, err)
		})
	}
	for name, value := range map[string]any{"unsigned": (*BigUint)(nil), "signed": (*SignedBigInt)(nil), "volumes": (*Volumes)(nil), "balance": (*VolumesWithBalance)(nil)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.MarshalWithOptions(value, MonetaryAmountsAsNumbers())
			require.NoError(t, err)
			require.Equal(t, "null", string(encoded))
		})
	}
}

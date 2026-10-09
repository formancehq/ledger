package commonpb

import (
	jsonv2 "encoding/json/v2"
	"io"
	"runtime"
	"testing"

	"github.com/bytedance/sonic"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// BenchmarkMonetaryResponse compares native Sonic and the scoped v2 path using
// the same public transaction/log/cursor projections and exact amount values.
func BenchmarkMonetaryResponse(b *testing.B) {
	b.Logf("toolchain=%s platform=%s/%s sonic.APIKind=%d", runtime.Version(), runtime.GOOS, runtime.GOARCH, sonic.APIKind)
	if sonic.APIKind != sonic.UseSonicJSON {
		b.Fatal("expected native Sonic baseline")
	}

	transaction := &Transaction{
		Id: 42, Reference: "benchmark", Timestamp: &Timestamp{Data: 1700000000000000},
		Metadata: map[string]*MetadataValue{"name": {Type: &MetadataValue_StringValue{StringValue: "<bank>"}}},
		Postings: []*Posting{{Source: "world", Destination: "bank", Asset: "USD", Amount: NewUint256FromUint64(9007199254740993)}},
		PostCommitVolumes: &PostCommitVolumes{VolumesByAccount: map[string]*VolumesByAssets{
			"bank": {Volumes: []*VolumeEntry{{Asset: "USD", Volumes: &Volumes{
				Input: MustBigUintFromDecimal("123456789012345678901234567890"), Output: MustBigUintFromDecimal("0"),
			}}}},
		}},
	}
	transactions := make([]*Transaction, 100)
	for i := range transactions {
		transactions[i] = transaction
	}
	log := &Log{Payload: &LogPayload{Type: &LogPayload_Apply{Apply: &ApplyLedgerLog{
		Log: NewLedgerLog(&LedgerLogPayload{Payload: &LedgerLogPayload_CreatedTransaction{CreatedTransaction: &CreatedTransaction{Transaction: transaction}}}),
	}}}}
	for _, payload := range []struct {
		name  string
		value any
	}{
		{"Transaction", transaction}, {"TransactionList100", transactions}, {"SystemLog", log},
		{"PreparedCursor100", &PreparedQueryCursor{PageSize: 100, TransactionData: transactions}},
	} {
		b.Run(payload.name, func(b *testing.B) {
			for _, codec := range []struct {
				name   string
				option func() jsonv2.Options
			}{
				{"SonicLegacy", nil}, {"V2Numbers", MonetaryAmountsAsNumbers}, {"V2Strings", MonetaryAmountsAsStrings},
			} {
				b.Run(codec.name, func(b *testing.B) {
					b.Run("Marshal", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							var err error
							if codec.option == nil {
								_, err = json.Marshal(payload.value)
							} else {
								_, err = json.MarshalWithOptions(payload.value, codec.option())
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					})
					b.Run("Write", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							var err error
							if codec.option == nil {
								err = json.MarshalWrite(io.Discard, payload.value)
							} else {
								err = json.MarshalWriteWithOptions(io.Discard, payload.value, codec.option())
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					})
				})
			}
		})
	}
}

//go:build clickhouse

package events

import (
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// eventToClickHouseJSON quotes only posting amounts in the analytical projection.
// The native driver sends this document as a JSON string: ClickHouse 24.x can
// reject integer tokens above UInt64 before applying the UInt256 column hint.
// Quoted decimals bypass that parser limit and retain the numeric column type.
func eventToClickHouseJSON(event *eventspb.Event) ([]byte, error) {
	return jsonv2.Marshal(eventToSinkData(event), json.DefaultOptionsV1(),
		jsonv2.WithMarshalers(jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, amount *big.Int) error {
			return enc.WriteToken(jsontext.String(amount.String()))
		})))
}

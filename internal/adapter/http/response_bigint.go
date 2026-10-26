package http

import (
	jsonv2 "encoding/json/v2"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// HeaderBigIntAsString selects canonical decimal strings for all HTTP response
// Uint256, BigUint and SignedBigInt values, independently of their business use.
const HeaderBigIntAsString = "Formance-Bigint-As-String"

func bigIntegerEncoding(r *http.Request) jsonv2.Options {
	if r != nil {
		// Match v2 exactly, including its lack of whitespace trimming.
		switch strings.ToLower(r.Header.Get(HeaderBigIntAsString)) {
		case "true", "yes", "y", "1":
			return commonpb.BigIntegersAsStrings()
		}
	}

	return commonpb.BigIntegersAsNumbers()
}

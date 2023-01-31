package searchhttp

import (
	"testing"

	"github.com/aquasecurity/esquery"
	"github.com/formancehq/search/pkg/searchengine"
	"github.com/stretchr/testify/assert"
)

func TestNextToken(t *testing.T) {
	nti := cursorTokenInfo{
		Target: "ACCOUNT",
		Sort: []searchengine.Sort{
			{
				Key:   "slug",
				Order: esquery.OrderDesc,
			},
		},
		SearchAfter: []interface{}{
			"ACCOUNT-2",
		},
		Ledgers: []string{"quickstart"},
	}
	tok := EncodeCursorToken(nti)
	decoded := cursorTokenInfo{}
	if !assert.NoError(t, DecodeCursorToken(tok, &decoded)) {
		return
	}
	if !assert.EqualValues(t, nti, decoded) {
		return
	}
}

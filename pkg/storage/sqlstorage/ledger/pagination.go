package ledger

import (
	"encoding/base64"
	"encoding/json"
)

func encodePaginationToken(t any) string {
	data, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

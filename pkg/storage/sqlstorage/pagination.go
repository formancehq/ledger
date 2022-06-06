package sqlstorage

import (
	"encoding/base64"
	"encoding/json"
)

type PaginationToken struct {
	ID uint64 `json:"txid"`
}

func tokenMarshal(i interface{}) (string, error) {
	raw, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

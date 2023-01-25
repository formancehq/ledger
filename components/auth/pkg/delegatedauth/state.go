package delegatedauth

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
)

type DelegatedState struct {
	AuthRequestID string `json:"authRequestID"`
}

func (s DelegatedState) EncodeAsUrlParam() string {
	buf := bytes.NewBufferString("")
	if err := json.NewEncoder(base64.NewEncoder(base64.URLEncoding, buf)).Encode(s); err != nil {
		panic(err)
	}
	return buf.String()
}

func DecodeDelegatedState(v string) (*DelegatedState, error) {
	ret := &DelegatedState{}
	if err := json.NewDecoder(base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(v))).Decode(ret); err != nil {
		return nil, err
	}
	return ret, nil
}

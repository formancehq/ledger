package common

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const MaxJSONBodySize int64 = 4 << 20

var ErrBodyTooLarge = errors.New("request body exceeds 4 MiB")

func ReadBody(r *http.Request) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r.Body, MaxJSONBodySize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > MaxJSONBodySize {
		return nil, ErrBodyTooLarge
	}
	return data, nil
}

// DecodeBody reads and decodes one bounded JSON request body. Streaming routes
// intentionally use their own decoders and are not subject to this limit.
func DecodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeBody(w, r, v, false)
}

// DecodeOptionalBody accepts an empty body, including when its length is unknown.
// Non-empty bodies have the same size and JSON validation as DecodeBody.
func DecodeOptionalBody(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeBody(w, r, v, true)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any, optional bool) bool {
	data, err := ReadBody(r)
	if err != nil {
		HandleRequestParsingErrors(w, err)
		return false
	}
	if optional && len(data) == 0 {
		return true
	}
	if err := json.Unmarshal(data, v); err != nil {
		HandleRequestParsingErrors(w, err)
		return false
	}
	return true
}

func WithBody[V any](w http.ResponseWriter, r *http.Request, fn func(v V)) {
	var v V
	if !DecodeBody(w, r, &v) {
		return
	}

	fn(v)
}

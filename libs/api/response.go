package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/pkg/errors"
)

type BaseResponse[T any] struct {
	Data   *T         `json:"data,omitempty"`
	Cursor *Cursor[T] `json:"cursor,omitempty"`
}

type Cursor[T any] struct {
	PageSize int    `json:"pageSize,omitempty"`
	HasMore  bool   `json:"hasMore"`
	Previous string `json:"previous,omitempty"`
	Next     string `json:"next,omitempty"`
	Data     []T    `json:"data"`
}

func MapCursor[FROM any, TO any](cursor *Cursor[FROM], mapper func(FROM) TO) *Cursor[TO] {
	return &Cursor[TO]{
		PageSize: cursor.PageSize,
		HasMore:  cursor.HasMore,
		Previous: cursor.Previous,
		Next:     cursor.Next,
		Data:     collectionutils.Map(cursor.Data, mapper),
	}
}

type ErrorResponse struct {
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	Details      string `json:"details,omitempty"`
}

func FetchAllPaginated[T any](ctx context.Context, client *http.Client, _url string, queryParams url.Values) ([]T, error) {
	ret := make([]T, 0)

	var nextToken string
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, _url, nil)
		if err != nil {
			return nil, err
		}
		if nextToken == "" {
			req.URL.RawQuery = queryParams.Encode()
		} else {
			req.URL.RawQuery = url.Values{
				"cursor": []string{nextToken},
			}.Encode()
		}
		rsp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if rsp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status code %d while waiting for %d", rsp.StatusCode, http.StatusOK)
		}
		apiResponse := BaseResponse[T]{}
		if err := json.NewDecoder(rsp.Body).Decode(&apiResponse); err != nil {
			return nil, errors.Wrap(err, "decoding cursir")
		}
		ret = append(ret, apiResponse.Cursor.Data...)
		if !apiResponse.Cursor.HasMore {
			break
		}
		nextToken = apiResponse.Cursor.Next
	}
	return ret, nil
}

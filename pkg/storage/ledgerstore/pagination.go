package ledgerstore

import (
	"encoding/base64"
	"encoding/json"
)

const (
	OrderAsc = iota
	OrderDesc

	QueryDefaultPageSize = 15
)

type Order int

func (o Order) String() string {
	switch o {
	case OrderAsc:
		return "ASC"
	case OrderDesc:
		return "DESC"
	}
	panic("should not happen")
}

func (o Order) Reverse() Order {
	return (o + 1) % 2
}

type ColumnPaginatedQuery[FILTERS any] struct {
	PageSize     uint64  `json:"pageSize"`
	Bottom       *uint64 `json:"bottom"`
	Column       string  `json:"column"`
	PaginationID *uint64 `json:"paginationID"`
	Order        Order   `json:"order"`
	Filters      FILTERS `json:"filters"`
	Reverse      bool    `json:"reverse"`
}

func (q *ColumnPaginatedQuery[PAYLOAD]) EncodeAsCursor() string {
	return encodeCursor(q)
}

type OffsetPaginatedQuery[FILTERS any] struct {
	Offset   uint64  `json:"offset"`
	Order    Order   `json:"order"`
	PageSize uint64  `json:"pageSize"`
	Filters  FILTERS `json:"filters"`
}

func (q *OffsetPaginatedQuery[PAYLOAD]) EncodeAsCursor() string {
	return encodeCursor(q)
}

func encodeCursor[T any](v *T) string {
	if v == nil {
		return ""
	}
	return EncodeCursor(*v)
}

func EncodeCursor[T any](v T) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func UnmarshalCursor(v string, to any) error {
	res, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(res, &to); err != nil {
		return err
	}

	return nil
}

package paginate

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
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

type ColumnPaginatedQuery[OPTIONS any] struct {
	PageSize     uint64   `json:"pageSize"`
	Bottom       *big.Int `json:"bottom"`
	Column       string   `json:"column"`
	PaginationID *big.Int `json:"paginationID"`
	Order        Order    `json:"order"`
	Options      OPTIONS  `json:"filters"`
	Reverse      bool     `json:"reverse"`
}

func (q *ColumnPaginatedQuery[PAYLOAD]) EncodeAsCursor() string {
	return encodeCursor(q)
}

func (a *ColumnPaginatedQuery[PAYLOAD]) WithPageSize(pageSize uint64) *ColumnPaginatedQuery[PAYLOAD] {
	if pageSize != 0 {
		a.PageSize = pageSize
	}

	return a
}

type OffsetPaginatedQuery[OPTIONS any] struct {
	Offset   uint64  `json:"offset"`
	Order    Order   `json:"order"`
	PageSize uint64  `json:"pageSize"`
	Options  OPTIONS `json:"filters"`
}

func (q *OffsetPaginatedQuery[PAYLOAD]) EncodeAsCursor() string {
	return encodeCursor(q)
}

func (a *OffsetPaginatedQuery[PAYLOAD]) WithPageSize(pageSize uint64) *OffsetPaginatedQuery[PAYLOAD] {
	if pageSize != 0 {
		a.PageSize = pageSize
	}

	return a
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

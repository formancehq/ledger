package bunpaginate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
)

const (
	OrderAsc = iota
	OrderDesc

	QueryDefaultPageSize = 15
	MaxPageSize          = 100

	QueryKeyPageSize = "pageSize"
	QueryKeyCursor   = "cursor"
)

var (
	ErrInvalidPageSize = errors.New("invalid 'pageSize' query param")
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

type pageSizeConfiguration struct {
	defaultPageSize uint64
	maxPageSize     uint64
}

func WithDefaultPageSize(v uint64) func(configuration *pageSizeConfiguration) {
	return func(configuration *pageSizeConfiguration) {
		configuration.defaultPageSize = v
	}
}

func WithMaxPageSize(v uint64) func(configuration *pageSizeConfiguration) {
	return func(configuration *pageSizeConfiguration) {
		configuration.maxPageSize = v
	}
}

func GetPageSize(r *http.Request, opts ...func(*pageSizeConfiguration)) (uint64, error) {
	cfg := pageSizeConfiguration{
		defaultPageSize: QueryDefaultPageSize,
		maxPageSize:     MaxPageSize,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	pageSizeParam := r.URL.Query().Get(QueryKeyPageSize)
	if pageSizeParam == "" {
		return cfg.defaultPageSize, nil
	}

	var pageSize uint64
	var err error
	if pageSizeParam != "" {
		pageSize, err = strconv.ParseUint(pageSizeParam, 10, 32)
		if err != nil {
			return 0, ErrInvalidPageSize
		}
	}

	if pageSize == 0 {
		return cfg.defaultPageSize, nil
	}

	if pageSize > cfg.maxPageSize {
		return cfg.maxPageSize, nil
	}

	return pageSize, nil
}

func Extract[Q any](r *http.Request, defaulter func() (*Q, error)) (*Q, error) {
	var query Q
	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
		if err != nil {
			return nil, fmt.Errorf("invalid '%s' query param", QueryKeyCursor)
		}
		return &query, nil
	} else {
		return defaulter()
	}
}

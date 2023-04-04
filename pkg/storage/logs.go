package storage

import (
	"github.com/formancehq/ledger/pkg/core"
)

type LogsQueryFilters struct {
	EndTime   core.Time `json:"endTime"`
	StartTime core.Time `json:"startTime"`
	AfterID   uint64    `json:"afterID"`
}

type LogsQuery ColumnPaginatedQuery[LogsQueryFilters]

func NewLogsQuery() LogsQuery {
	return LogsQuery{
		PageSize: QueryDefaultPageSize,
		Column:   "id",
		Order:    OrderDesc,
		Filters:  LogsQueryFilters{},
	}
}

func (a LogsQuery) WithPaginationID(id uint64) LogsQuery {
	a.PaginationID = &id
	return a
}

func (l LogsQuery) WithPageSize(pageSize uint64) LogsQuery {
	if pageSize != 0 {
		l.PageSize = pageSize
	}

	return l
}

func (l LogsQuery) WithStartTimeFilter(start core.Time) LogsQuery {
	if !start.IsZero() {
		l.Filters.StartTime = start
	}

	return l
}

func (l LogsQuery) WithEndTimeFilter(end core.Time) LogsQuery {
	if !end.IsZero() {
		l.Filters.EndTime = end
	}

	return l
}

func (q LogsQuery) WithAfterID(id uint64) LogsQuery {
	q.Filters.AfterID = id
	return q
}

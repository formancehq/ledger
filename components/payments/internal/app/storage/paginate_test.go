package storage

import (
	"reflect"
	"testing"

	"github.com/uptrace/bun"
)

func TestPaginate(t *testing.T) {
	t.Parallel()

	type args struct {
		pageSize int
		token    string
		sorter   Sorter
	}

	token, err := baseCursor{
		Reference: "",
		Sorter:    nil,
		Next:      false,
	}.Encode()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		args    args
		want    Paginator
		wantErr bool
	}{
		{
			name:    "valid",
			args:    args{pageSize: 10, token: "", sorter: nil},
			want:    Paginator{pageSize: 10, token: "", cursor: baseCursor{}, sorter: nil},
			wantErr: false,
		},
		{
			name:    "invalid page size",
			args:    args{pageSize: 0, token: "", sorter: nil},
			want:    Paginator{pageSize: defaultPageSize, token: "", cursor: baseCursor{}, sorter: nil},
			wantErr: false,
		},
		{
			name:    "exceeding max page size",
			args:    args{pageSize: maxPageSize + 1, token: "", sorter: nil},
			want:    Paginator{pageSize: maxPageSize, token: "", cursor: baseCursor{}, sorter: nil},
			wantErr: false,
		},
		{
			name:    "token decode",
			args:    args{pageSize: 10, token: token, sorter: nil},
			want:    Paginator{pageSize: 10, token: token, cursor: baseCursor{}, sorter: nil},
			wantErr: false,
		},
		{
			name:    "invalid token",
			args:    args{pageSize: 10, token: "abc", sorter: nil},
			want:    Paginator{pageSize: 0, token: "", cursor: baseCursor{}, sorter: nil},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Paginate(tt.args.pageSize, tt.args.token, tt.args.sorter)
			if (err != nil) != tt.wantErr {
				t.Errorf("Paginate() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Paginate() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPaginatorApply(t *testing.T) {
	t.Parallel()

	type fields struct {
		pageSize int
		token    string
		cursor   baseCursor
		sorter   Sorter
	}
	type args struct {
		query  *bun.SelectQuery
		column string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *bun.SelectQuery
	}{
		{
			name:   "no cursor",
			fields: fields{pageSize: 10, token: "", cursor: baseCursor{}, sorter: nil},
			args:   args{query: &bun.SelectQuery{}, column: "id"},
			want:   nil,
		},
		{
			name:   "with cursor",
			fields: fields{pageSize: 10, token: "", cursor: baseCursor{Reference: "id", Sorter: Sorter{{Column: "id", Order: SortOrderDesc}}}, sorter: nil},
			args:   args{query: &bun.SelectQuery{}, column: "id"},
			want:   nil,
		},
		{
			name:   "with cursor next",
			fields: fields{pageSize: 10, token: "", cursor: baseCursor{Reference: "id", Next: true, Sorter: Sorter{{Column: "id", Order: SortOrderDesc}}}, sorter: nil},
			args:   args{query: &bun.SelectQuery{}, column: "id"},
			want:   nil,
		},
		{
			name:   "with cursor no ref",
			fields: fields{pageSize: 10, token: "", cursor: baseCursor{}, sorter: Sorter{{Column: "id", Order: SortOrderDesc}}},
			args:   args{query: &bun.SelectQuery{}, column: "id"},
			want:   nil,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := Paginator{
				pageSize: tt.fields.pageSize,
				token:    tt.fields.token,
				cursor:   tt.fields.cursor,
				sorter:   tt.fields.sorter,
			}

			if got := p.apply(tt.args.query, tt.args.column); tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPaginatorPaginationDetails(t *testing.T) {
	t.Parallel()

	type fields struct {
		pageSize int
		token    string
		cursor   baseCursor
		sorter   Sorter
	}

	type args struct {
		hasMore        bool
		firstReference string
		lastReference  string
	}

	cursor := baseCursor{
		Reference: "abc",
		Sorter:    nil,
		Next:      false,
	}

	token, err := cursor.Encode()
	if err != nil {
		t.Fatal(err)
	}

	tokenNext, err := baseCursor{
		Reference: "",
		Sorter:    nil,
		Next:      true,
	}.Encode()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    PaginationDetails
		wantErr bool
	}{
		{
			name:    "no cursor",
			fields:  fields{pageSize: 10, token: "", cursor: baseCursor{}, sorter: nil},
			args:    args{hasMore: false, firstReference: "", lastReference: ""},
			want:    PaginationDetails{PageSize: 10, HasMore: false},
			wantErr: false,
		},
		{
			name:    "with cursor",
			fields:  fields{pageSize: 10, token: "", cursor: cursor, sorter: nil},
			args:    args{hasMore: false, firstReference: "abc", lastReference: ""},
			want:    PaginationDetails{PageSize: 10, HasMore: false, PreviousPage: token},
			wantErr: false,
		},
		{
			name:    "has more",
			fields:  fields{pageSize: 10, token: "", cursor: baseCursor{}, sorter: nil},
			args:    args{hasMore: true, firstReference: "", lastReference: ""},
			want:    PaginationDetails{PageSize: 10, HasMore: true, NextPage: tokenNext},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := Paginator{
				pageSize: tt.fields.pageSize,
				token:    tt.fields.token,
				cursor:   tt.fields.cursor,
				sorter:   tt.fields.sorter,
			}

			got, err := p.paginationDetails(tt.args.hasMore, tt.args.firstReference, tt.args.lastReference)
			if (err != nil) != tt.wantErr {
				t.Errorf("paginationDetails() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("paginationDetails() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}

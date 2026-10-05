package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

func decodeLink(t *testing.T, token string) pagecursor.Cursor {
	t.Helper()

	c, err := pagecursor.Decode(token)
	require.NoError(t, err)

	return c
}

// TestExecute_PreviousWalksBackToTheHead pages forward to the end, then
// follows previous back to the first page: every back page must equal the
// forward page it mirrors, in query order.
func TestExecute_PreviousWalksBackToTheHead(t *testing.T) {
	t.Parallel()

	for _, reverse := range []bool{false, true} {
		f := newReverseFixture(t, nil, "a", "b", "c", "d", "e")

		var (
			forward [][]string
			token   string
		)

		for {
			cursor := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 2, Cursor: token, Reverse: reverse})
			forward = append(forward, pageAddresses(cursor))

			if len(forward) == 1 {
				require.Empty(t, cursor.GetPrevious(), "reverse=%v: the first page has nothing before it", reverse)
			}

			if cursor.GetNext() == "" {
				token = cursor.GetPrevious()

				break
			}

			token = cursor.GetNext()
		}

		require.Len(t, forward, 3, "reverse=%v", reverse)

		for i := len(forward) - 2; i >= 0; i-- {
			cursor := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 2, Cursor: token, Reverse: reverse})
			require.Equal(t, forward[i], pageAddresses(cursor), "reverse=%v: back page %d", reverse, i)
			require.True(t, cursor.GetHasMore(), "reverse=%v: a back page always has a next page", reverse)

			if i == 0 {
				require.Empty(t, cursor.GetPrevious(), "reverse=%v: the head has nothing before it", reverse)

				break
			}

			token = cursor.GetPrevious()
		}
	}
}

func TestExecute_BackPageLinks(t *testing.T) {
	t.Parallel()

	f := newReverseFixture(t, nil, "a", "b", "c", "d", "e")

	cursor := f.page(&servicepb.ExecutePreparedQueryRequest{
		PageSize: 2,
		Cursor:   pagecursor.Cursor{Key: "d", Back: true}.Encode(),
	})
	require.Equal(t, []string{"b", "c"}, pageAddresses(cursor))
	require.Equal(t, pagecursor.Cursor{Key: "c"}, decodeLink(t, cursor.GetNext()))
	require.Equal(t, pagecursor.Cursor{Key: "b", Back: true}, decodeLink(t, cursor.GetPrevious()))

	last := f.page(&servicepb.ExecutePreparedQueryRequest{
		PageSize: 2,
		Cursor:   pagecursor.Cursor{Back: true}.Encode(),
	})
	require.Equal(t, []string{"d", "e"}, pageAddresses(last), "an empty back key serves the last page")
	require.Empty(t, last.GetNext())
	require.False(t, last.GetHasMore())
}

func TestExecute_EmptyResumedPageLinksToTheLastPage(t *testing.T) {
	t.Parallel()

	f := newReverseFixture(t, nil, "a", "b")

	cursor := f.page(&servicepb.ExecutePreparedQueryRequest{
		PageSize: 2,
		Cursor:   pagecursor.Cursor{Key: "z"}.Encode(),
	})
	require.Empty(t, pageAddresses(cursor))
	require.Empty(t, cursor.GetNext())
	require.Equal(t, uint32(2), cursor.GetPageSize())
	require.Equal(t, pagecursor.Cursor{Back: true}, decodeLink(t, cursor.GetPrevious()))
}

func TestExecute_InvalidCursorIsAValidationError(t *testing.T) {
	t.Parallel()

	f := newReverseFixture(t, nil, "a")

	_, err := f.execute(&servicepb.ExecutePreparedQueryRequest{PageSize: 2, Cursor: "not-a-token"})

	var invalid *query.ErrInvalidCursor
	require.ErrorAs(t, err, &invalid)
	require.Equal(t, domain.KindValidation, invalid.Kind())
}

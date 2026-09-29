package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// nextCursorTrailer is the gRPC trailer the streaming list handlers publish the
// following page's resume token under (adapter/grpc/stream_helper.go).
const nextCursorTrailer = "x-next-cursor"

// cursorMore is the model's verdict on whether a row follows the page a token
// accompanies.
type cursorMore uint8

const (
	// cursorForbidden: the page exhausted the rows its base holds.
	cursorForbidden cursorMore = iota
	// cursorRequired: a row the base requires is waiting past the page.
	cursorRequired
	// cursorEither: the only rows left hinge on a server stamp the model has
	// not learned, so serving them or stopping are both legal.
	cursorEither
)

// nextCursorOf reads the resume token a drained stream published. Empty is the
// server saying the page it just sent is the last one.
func nextCursorOf[T any](stream grpc.ServerStreamingClient[T]) string {
	values := stream.Trailer().Get(nextCursorTrailer)
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

// nextCursorLegal reports whether the token published alongside a page is the
// one that page's own rows imply.
//
// The handler peeks one row past the page and publishes a token only when the
// peek fires, so a token is a claim that another row exists — and the token is
// the LAST SENT row's key, not the peeked row's, because resume is exclusive
// and naming the peek would skip it.
//
// rows and pageSize carry the structural half, which holds whatever the model
// knows: the peek cannot fire before a full page has been sent, so a page the
// size limit did not fill carries no token. A follower that routed the read
// forwards the leader's token instead of deriving its own, but only after
// relaying the leader's whole page, which is full for the same reason — the
// leader caps at its own maximum and advertises the remainder through the
// trailer (adapter/grpc/cursor.go, upstreamPeekCursor).
func nextCursorLegal(next string, more cursorMore, lastKey string, rows, pageSize int) bool {
	if next == "" {
		return more != cursorRequired
	}

	if rows != pageSize {
		return false
	}

	return more != cursorForbidden && next == lastKey
}

// malformedCursors are tokens parseUint64Cursor must refuse. The endpoints keyed
// by a uint64 decode the token themselves, so one they cannot decode is
// InvalidArgument — never a silently empty page, which would look to a client
// like the end of the iteration.
var malformedCursors = []string{
	"not-a-cursor",
	"-1",
	"0x10",
	" 12",
	"12 ",
	"1.0",
	"18446744073709551616", // MaxUint64 + 1
}

// rollMalformedCursor returns a token the server must reject, one read in
// sixteen.
func rollMalformedCursor() (string, bool) {
	if !oneIn(16) {
		return "", false
	}

	return random.RandomChoice(malformedCursors), true
}

// handleMalformedCursorError validates the outcome of a token the server cannot
// decode: InvalidArgument, and no page. One Reachable literal per endpoint —
// Antithesis catalogues assertions by literal, and the three decode their token
// in their own handler. Returns true when it has fully handled err.
func handleMalformedCursorError(rolled bool, kind, cursor string, err error) bool {
	if !rolled {
		return false
	}

	if status.Code(err) == codes.InvalidArgument {
		switch kind {
		case "transaction":
			assert.Reachable("singleton_driver_model: malformed transaction cursor rejected", internal.Details{"cursor": cursor})
		case "log":
			assert.Reachable("singleton_driver_model: malformed log cursor rejected", internal.Details{"cursor": cursor})
		default:
			assert.Reachable("singleton_driver_model: malformed audit cursor rejected", internal.Details{"cursor": cursor})
		}

		return true
	}

	assert.Unreachable("singleton_driver_model: malformed cursor returned unexpected error", internal.Details{
		"kind":   kind,
		"cursor": cursor,
		"error":  err.Error(),
	})

	return true
}

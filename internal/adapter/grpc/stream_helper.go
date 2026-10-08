package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

// NextCursorTrailerKey and PreviousCursorTrailerKey are the gRPC trailer
// keys under which paged list handlers publish the page tokens
// (pkg/pagecursor) of the following and preceding pages. Clients pass one
// back as the next request's ListOptions.cursor.
const (
	NextCursorTrailerKey     = "x-next-cursor"
	PreviousCursorTrailerKey = "x-previous-cursor"
)

// sendPagedToStream serves the page that page requests from cur, closes the
// cursor, and publishes the adjacent pages' tokens as trailers. cur must be
// read in page.ReadReverse order and sized for pageSize+1 items: the extra
// row proves a further page exists without a spurious empty round-trip on a
// list of exactly pageSize items. keyOf renders an item's position as a
// cursor key; an empty key yields no link through that item.
//
// A forward page streams as it is read. A back page is read in the opposite
// order, so it is buffered (at most pageSize+1 items) and sent reversed.
//
// Pass pageSize == 0 to drain unbounded without trailers.
//
// # Phase attribution (EN-1859)
//
// The loop below is where a streaming read spends everything the QueryProfile
// execution counters cannot see: row serialisation, the transport write, and
// any time blocked on a slow consumer. It therefore splits its own wall time
// between two profile phases with opposite meanings:
//
//   - cur.Next() is row PRODUCTION and is charged to the execution phase. It is
//     near-free for an eagerly materialised local cursor, but on a follower that
//     routed the read to the leader each Next() is an upstream stream receive,
//     which is genuine server-side query cost.
//   - stream.Send() is DELIVERY. It contains flow-control back-pressure, so it
//     is deliberately kept out of ServerDuration — a total that grew because the
//     client stopped reading would mislead rather than inform.
//
// The split is performed whenever a profile is present, never gated on whether
// the caller asked to SEE the profile. An earlier revision gated it and charged
// the whole loop to delivery otherwise, which made ServerDuration blind in the
// only configuration that consumes it: nothing sends x-query-profile in normal
// operation, so the slow-query log was reading a total from which the entire
// send loop had been subtracted — a forwarded read taking seconds inside
// cur.Next() reported sub-millisecond. Two time.Now() per row (tens of
// microseconds across a 1000-row page, the server-side maximum) is not worth one
// measurement regime per caller behaviour.
func sendPagedToStream[Res any](
	ctx context.Context,
	cur cursor.Cursor[*Res],
	stream ggrpc.ServerStreamingServer[Res],
	itemName string,
	pageSize uint32,
	page pagecursor.Cursor,
	keyOf func(*Res) string,
) error {
	defer func() {
		_ = cur.Close()
	}()

	span := trace.SpanFromContext(ctx)

	// Most streaming list handlers are unprofiled and share this helper, so the
	// per-row clock reads are skipped entirely when there is no profile to feed.
	profile := query.ProfileFromContext(ctx)
	timed := profile != nil

	var count uint32

	next := func() (*Res, error) {
		produceStart := nowIf(timed)
		item, err := cur.Next()

		if timed {
			profile.AddProduction(time.Since(produceStart))
		}

		if err != nil && !errors.Is(err, io.EOF) {
			span.SetAttributes(attribute.Int64("stream.items_sent", int64(count)))

			return nil, fmt.Errorf("reading %s: %w", itemName, err)
		}

		return item, err
	}

	send := func(item *Res) error {
		deliverStart := nowIf(timed)
		sendErr := stream.Send(item)

		if timed {
			profile.AddDelivery(time.Since(deliverStart))
		}

		if sendErr != nil {
			span.SetAttributes(attribute.Int64("stream.items_sent", int64(count)))

			return fmt.Errorf("sending %s: %w", itemName, sendErr)
		}

		profile.MarkFirstRow()
		count++

		return nil
	}

	link := func(first, last *Res, more bool) {
		span.SetAttributes(attribute.Int64("stream.items_sent", int64(count)))

		if pageSize == 0 {
			return
		}

		var firstKey, lastKey string
		if first != nil {
			firstKey, lastKey = keyOf(first), keyOf(last)
		}

		nextToken, previousToken := page.Links(firstKey, lastKey, int(count), more)

		var md metadata.MD
		if nextToken != "" {
			md = metadata.Join(md, metadata.Pairs(NextCursorTrailerKey, nextToken))
		}

		if previousToken != "" {
			md = metadata.Join(md, metadata.Pairs(PreviousCursorTrailerKey, previousToken))
		}

		if md != nil {
			stream.SetTrailer(md)
		}
	}

	if page.Back {
		var read []*Res

		for pageSize == 0 || uint32(len(read)) <= pageSize {
			item, err := next()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return err
			}

			read = append(read, item)
		}

		more := cursor.SourceHasMore(cur)
		if pageSize > 0 && uint32(len(read)) > pageSize {
			read, more = read[:pageSize], true
		}

		var first, last *Res

		for _, r := range slices.Backward(read) {
			if err := send(r); err != nil {
				return err
			}
		}

		if len(read) > 0 {
			first, last = read[len(read)-1], read[0]
		}

		link(first, last, more)

		return nil
	}

	var first, last *Res

	for {
		item, err := next()
		if errors.Is(err, io.EOF) {
			// A routed source capped its response at the leader's page limit:
			// the leader's own peek is the only proof of a further page.
			link(first, last, cursor.SourceHasMore(cur))

			return nil
		}

		if err != nil {
			return err
		}

		// The (pageSize+1)th item proves another page exists; it is not sent.
		if pageSize > 0 && count >= pageSize {
			link(first, last, true)

			return nil
		}

		if err := send(item); err != nil {
			return err
		}

		if first == nil {
			first = item
		}

		last = item
	}
}

// nowIf returns the current instant only when the caller intends to use it,
// keeping the per-row clock reads out of a stream that has no profile to feed.
func nowIf(enabled bool) time.Time {
	if !enabled {
		return time.Time{}
	}

	return time.Now()
}

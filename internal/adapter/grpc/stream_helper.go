package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/query"
)

// NextCursorTrailerKey is the gRPC trailer key under which streaming list
// handlers publish the opaque resume token for the following page. Clients
// pass it back as the next request's ListOptions.cursor; the server is free
// to evolve the encoding (entity address, sequence number, opaque token, …)
// without coordinating with deployed clients.
//
// Cursors that are derived from user-controlled identifiers (ledger names,
// numscript names, …) must already be safe for HTTP/2 header values —
// printable ASCII (0x20–0x7E), no CR/LF/NUL. The corresponding domain
// validators (`ValidateLedgerName`, `ValidateNumscriptName`) enforce this
// at admission so we can drop tokens into the trailer raw without an
// extra encode/decode hop.
const NextCursorTrailerKey = "x-next-cursor"

// sendPagedToStream emits up to pageSize items from cur, closes the cursor,
// and only publishes an x-next-cursor trailer when it actually observes a
// (pageSize+1)th item. The peek-ahead avoids the false-positive where a list
// of exactly pageSize items would emit a trailer pointing past the last
// element — clients would then issue a spurious empty round-trip.
//
// When the peek fires, the cursor is computed from the LAST SENT item, not
// from the peeked item: resume semantics is exclusive ("after cursor"), so
// using the peeked item as the cursor would skip it on the next page.
//
// Callers must size the source cursor for pageSize+1 items so the peek can
// fire. Pass pageSize == 0 (and cursorOf == nil) to drain unbounded without
// emitting a trailer.
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
	cursorOf func(*Res) string,
) error {
	defer func() {
		_ = cur.Close()
	}()

	span := trace.SpanFromContext(ctx)

	// Most streaming list handlers are unprofiled and share this helper, so the
	// per-row clock reads are skipped entirely when there is no profile to feed.
	profile := query.ProfileFromContext(ctx)
	timed := profile != nil

	var (
		count    uint32
		lastSent *Res
	)

	emitTrailer := func() {
		if pageSize == 0 || cursorOf == nil || lastSent == nil {
			return
		}

		if next := cursorOf(lastSent); next != "" {
			stream.SetTrailer(metadata.Pairs(NextCursorTrailerKey, next))
		}
	}

	for {
		produceStart := nowIf(timed)
		item, err := cur.Next()

		if timed {
			profile.AddProduction(time.Since(produceStart))
		}

		if err != nil {
			span.SetAttributes(attribute.Int64("stream.items_sent", int64(count)))

			if errors.Is(err, io.EOF) {
				// Local cursor had no peek slot. If the source was a routed
				// gRPC stream whose own peek fired upstream, forward upstream's
				// cursor VERBATIM — we trust the upstream's "after this" token,
				// and re-deriving from our local lastSent would (a) drop the
				// trailer entirely when we sent zero items this batch
				// (lastSent == nil short-circuits emitTrailer) and (b) compute
				// a different value when upstream's encoding differs.
				if uc, ok := cur.(UpstreamTrailer); ok {
					if next := uc.NextCursor(); next != "" {
						stream.SetTrailer(metadata.Pairs(NextCursorTrailerKey, next))
					}
				}

				return nil
			}

			return fmt.Errorf("reading %s: %w", itemName, err)
		}

		// The (pageSize+1)th item proves another page exists. Resume tokens
		// are exclusive, so use the LAST SENT item — not this peek — as the
		// cursor; using the peek would have the client skip it next time.
		if pageSize > 0 && count >= pageSize {
			emitTrailer()

			span.SetAttributes(attribute.Int64("stream.items_sent", int64(count)))

			return nil
		}

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

		lastSent = item
		count++
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

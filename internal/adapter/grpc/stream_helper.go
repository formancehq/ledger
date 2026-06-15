package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
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
		item, err := cur.Next()
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

		if err := stream.Send(item); err != nil {
			span.SetAttributes(attribute.Int64("stream.items_sent", int64(count)))

			return fmt.Errorf("sending %s: %w", itemName, err)
		}

		lastSent = item
		count++
	}
}

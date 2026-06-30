package main

import (
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Hand-tunable knobs. The "t-N:{id}" naming and the generator's roll
// distributions stay inlined at their use sites.

// --- Fleet shape --------------------------------------------------------

// Overridable via MODEL_LEDGERS / MODEL_WORKERS. Tuned for a single-
// node local server.
const (
	defaultLedgers = 4
	defaultWorkers = 8
)

// --- Address space ------------------------------------------------------

// Type-name pool size. Smaller pool → more collisions → more chaos.
const numPrefixes = 25

// Indexes for random.RandomChoice (uint8 slice). Derived to stay in
// lockstep with numPrefixes.
var typePool = func() []uint8 {
	out := make([]uint8, numPrefixes)
	for i := range out {
		out[i] = uint8(i)
	}
	return out
}()

// Address id range per prefix; small enough for frequent re-touches.
const numIDsPerPrefix = 100

// --- Assets -------------------------------------------------------------

var assets = []string{"USD/2", "EUR/2", "COIN"}

// --- Metadata -----------------------------------------------------------

// Metadata key/value pools, both tiny so concurrent sets of the same
// (address, key) with different values are frequent — last-writer-wins
// ordering is the property the model exists to check.
const numMetaKeys = 4

var metaKeyPool = func() []uint8 {
	out := make([]uint8, numMetaKeys)
	for i := range out {
		out[i] = uint8(i)
	}
	return out
}()

// Value mix spanning words, ints, and bools, written as strings and stored
// verbatim — declared_type is an index hint, not applied on read, so reads
// return exactly these strings. The spread keeps values distinct across keys and
// covers the sized-int boundaries (200, -1/-200, 40000, 5000000000) and a
// leading-zero string (030).
var metaValuePool = []string{
	"v0", "7", "42", "true", "false",
	"200", "-1", "-200", "40000", "5000000000", "030",
}

// Typed-value pools, used alongside metaValuePool so metadata writes exercise
// every MetadataValue wire kind, not just strings. The server stores values
// verbatim (a declared field type is an index hint, not applied on read), so
// each kind must round-trip unchanged. Small pools keep same-key collisions
// frequent. Values straddle the sized-int boundaries and include zero, negative,
// and pre-1970 datetimes.
var (
	metaIntPool          = []int64{0, 1, -1, 42, -200, 5000000000}
	metaUintPool         = []uint64{0, 1, 42, 200, 40000, 5000000000}
	metaDatetimePool     = []int64{0, 1, -1000000, 1700000000000000} // microseconds since the Unix epoch
	metaNullOriginalPool = []string{"", "null", "030"}
)

// Field types declared by SetMetadataFieldType (string, bool, every
// signed/unsigned int width), exercising the schema declare/retype apply path.
var metaTypePool = []commonpb.MetadataType{
	commonpb.MetadataType_METADATA_TYPE_STRING,
	commonpb.MetadataType_METADATA_TYPE_INT64,
	commonpb.MetadataType_METADATA_TYPE_BOOL,
	commonpb.MetadataType_METADATA_TYPE_UINT64,
	commonpb.MetadataType_METADATA_TYPE_INT8,
	commonpb.MetadataType_METADATA_TYPE_INT16,
	commonpb.MetadataType_METADATA_TYPE_INT32,
	commonpb.MetadataType_METADATA_TYPE_UINT8,
	commonpb.MetadataType_METADATA_TYPE_UINT16,
	commonpb.MetadataType_METADATA_TYPE_UINT32,
}

// Targets whose metadata schema the model tracks.
var metaTargetPool = []commonpb.TargetType{
	commonpb.TargetType_TARGET_TYPE_ACCOUNT,
	commonpb.TargetType_TARGET_TYPE_LEDGER,
	commonpb.TargetType_TARGET_TYPE_TRANSACTION,
}

// --- Transaction back-pressure ------------------------------------------

// Each transaction the model tracks keeps a unique reference forever (they stay
// addressable, so they can't be pruned without breaking future reads), so the
// reference/metadata maps would grow without bound. New-transaction emission
// tapers with a ledger's committed-transaction count and stops past txEmitStop,
// shifting the workload toward exercising existing transactions. The cap keeps
// clone cost in the serialization search comparable to the volume table
// (numPrefixes*numIDsPerPrefix*len(assets) cells per ledger). Drain/transient
// transactions set no reference, so they are untracked and not gated here.
const (
	txEmitFull  = 500  // below this count: always create new transactions
	txEmitTaper = 2000 // below this: ~half the time
	txEmitStop  = 4000 // below this: ~1-in-8; at or above: stop creating new ones
)

// --- Worker pacing ------------------------------------------------------

// Sleep per worker iteration — CPU/server breathing room.
const workerLoopPause = 100 * time.Millisecond

// Worker → processor channel cap, well above steady-state inflight.
const incomingBuffer = 256

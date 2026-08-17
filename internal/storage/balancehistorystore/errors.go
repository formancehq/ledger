package balancehistorystore

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/domain"
)

const (
	errReasonHistoryBuilding           = "HISTORY_BUILDING"
	errReasonHistoryBehind             = "HISTORY_BEHIND"
	errReasonHistorySourceMissing      = "HISTORY_SOURCE_MISSING"
	errReasonHistoryCorrupt            = "HISTORY_CORRUPT"
	errReasonUnsupportedTemporalFilter = "UNSUPPORTED_TEMPORAL_FILTER"
)

// ErrBuilding means the local projection is still creating its first
// complete, verified history prefix. It is retryable.
type ErrBuilding struct {
	Current uint64
	Target  uint64
}

func (e *ErrBuilding) Error() string {
	return fmt.Sprintf("balance history is building: current log sequence %d, target %d", e.Current, e.Target)
}

func (*ErrBuilding) Reason() string { return errReasonHistoryBuilding }

func (e *ErrBuilding) Metadata() map[string]string {
	return logSequenceMetadata(e.Current, e.Target, "targetLogSequence")
}

// ErrBehind means the local projection has not published the requested source
// prefix yet. It is retryable.
type ErrBehind struct {
	Required uint64
	Current  uint64
}

func (e *ErrBehind) Error() string {
	return fmt.Sprintf("balance history is behind: required log sequence %d, current %d", e.Required, e.Current)
}

func (*ErrBehind) Reason() string { return errReasonHistoryBehind }

func (e *ErrBehind) Metadata() map[string]string {
	return logSequenceMetadata(e.Current, e.Required, "requiredLogSequence")
}

// ErrSourceMissing means no verified genesis-to-watermark source or additive
// base segment exists. A current live snapshot is not a valid substitute because
// it has forgotten purged ephemeral generations.
type ErrSourceMissing struct {
	Detail string
}

func (e *ErrSourceMissing) Error() string {
	if e.Detail == "" {
		return "balance history source is incomplete"
	}

	return "balance history source is incomplete: " + e.Detail
}

func (*ErrSourceMissing) Reason() string              { return errReasonHistorySourceMissing }
func (*ErrSourceMissing) Metadata() map[string]string { return nil }

// ErrCorrupt means a manifest or segment failed its structural integrity check.
type ErrCorrupt struct {
	Detail string
}

func (e *ErrCorrupt) Error() string {
	return "balance history is corrupt: " + e.Detail
}

func (*ErrCorrupt) Reason() string              { return errReasonHistoryCorrupt }
func (*ErrCorrupt) Metadata() map[string]string { return nil }

// ErrQuarantined means an integrity failure was persisted and the store will
// refuse reads and mutations until Reset completes. It unwraps to ErrCorrupt
// so public error mappings keep treating quarantine as HISTORY_CORRUPT.
type ErrQuarantined struct {
	Detail string
}

func (e *ErrQuarantined) Error() string {
	if e.Detail == "" {
		return "balance history is quarantined"
	}

	return "balance history is quarantined: " + e.Detail
}

func (e *ErrQuarantined) Unwrap() error {
	return &ErrCorrupt{Detail: e.Detail}
}

func (*ErrQuarantined) Reason() string              { return errReasonHistoryCorrupt }
func (*ErrQuarantined) Metadata() map[string]string { return nil }

// ErrUnsupportedTemporalFilter means the requested filter cannot be evaluated
// with the selected temporal view. Category must be a bounded classification,
// never the raw user-supplied filter expression.
type ErrUnsupportedTemporalFilter struct {
	Category string
}

func (e *ErrUnsupportedTemporalFilter) Error() string {
	if e.Category == "" {
		return "filter is not supported for historical balance queries"
	}

	return "filter is not supported for historical balance queries: " + e.Category
}

func (*ErrUnsupportedTemporalFilter) Reason() string { return errReasonUnsupportedTemporalFilter }

func (e *ErrUnsupportedTemporalFilter) Metadata() map[string]string {
	if e.Category == "" {
		return nil
	}

	return map[string]string{"filterCategory": e.Category}
}

func logSequenceMetadata(current, other uint64, otherKey string) map[string]string {
	return map[string]string{
		"currentLogSequence": strconv.FormatUint(current, 10),
		otherKey:             strconv.FormatUint(other, 10),
	}
}

var (
	_ domain.Describable = (*ErrBuilding)(nil)
	_ domain.Describable = (*ErrBehind)(nil)
	_ domain.Describable = (*ErrSourceMissing)(nil)
	_ domain.Describable = (*ErrCorrupt)(nil)
	_ domain.Describable = (*ErrQuarantined)(nil)
	_ domain.Describable = (*ErrUnsupportedTemporalFilter)(nil)
)

type ErrUnsupportedFormat struct {
	Found     uint32
	Supported uint32
}

func (e *ErrUnsupportedFormat) Error() string {
	return fmt.Sprintf("unsupported balance history format %d (supported %d)", e.Found, e.Supported)
}

func (*ErrUnsupportedFormat) Reason() string              { return errReasonHistoryCorrupt }
func (*ErrUnsupportedFormat) Metadata() map[string]string { return nil }

type ErrUnsupportedReducer struct {
	Found     uint32
	Supported uint32
}

func (e *ErrUnsupportedReducer) Error() string {
	return fmt.Sprintf("unsupported balance history reducer %d (supported %d)", e.Found, e.Supported)
}

func (*ErrUnsupportedReducer) Reason() string              { return errReasonHistoryCorrupt }
func (*ErrUnsupportedReducer) Metadata() map[string]string { return nil }

// ErrSourceGap prevents a builder from publishing a non-consecutive source
// range or an effect outside the declared range.
type ErrSourceGap struct {
	Detail string
}

func (e *ErrSourceGap) Error() string {
	return "balance history source gap: " + e.Detail
}

var (
	_ domain.Describable = (*ErrUnsupportedFormat)(nil)
	_ domain.Describable = (*ErrUnsupportedReducer)(nil)
)

func isIntegrityError(err error) bool {
	var corrupt *ErrCorrupt
	var unsupportedFormat *ErrUnsupportedFormat
	var unsupportedReducer *ErrUnsupportedReducer

	return errors.As(err, &corrupt) ||
		errors.As(err, &unsupportedFormat) ||
		errors.As(err, &unsupportedReducer)
}

// Package balancehistoryarchive archives immutable balance-history runs in
// cold storage and exposes them through a verified, byte-bounded local cache.
package balancehistoryarchive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	// FormatVersion identifies the deterministic run archive encoding.
	FormatVersion uint32 = 1
)

var (
	// ErrMissing identifies an archive that is not durably present in cold
	// storage. It is safe for callers to rebuild and publish that object.
	ErrMissing = errors.New("balance history archive is missing")
	// ErrCorrupt identifies an archive whose stored, calculated, or embedded
	// checksum does not match its content-addressed reference.
	ErrCorrupt = errors.New("balance history archive is corrupt")
	// ErrInvalidReference identifies an incomplete or internally inconsistent
	// object reference supplied by a caller.
	ErrInvalidReference = errors.New("invalid balance history archive reference")
	// ErrUnsupportedFormat identifies a run encoded with a version this binary
	// cannot decode.
	ErrUnsupportedFormat = errors.New("unsupported balance history archive format")
	// ErrUnsortedRecords identifies input that is not strictly key-sorted.
	ErrUnsortedRecords = errors.New("balance history archive records are not strictly sorted")
)

// Ref is the complete immutable identity of one archived run. SHA256 hashes
// the entire encoded blob, including its self-verifying trailer.
type Ref struct {
	Version     uint32   `json:"version"`
	SHA256      [32]byte `json:"sha256"`
	Size        uint64   `json:"size"`
	RecordCount uint64   `json:"recordCount"`
}

// Hex returns the lowercase content digest used in the cold object namespace.
func (r Ref) Hex() string {
	return hex.EncodeToString(r.SHA256[:])
}

func (r Ref) validate() error {
	if r.Version != FormatVersion {
		return fmt.Errorf("%w: got version %d, support %d", ErrUnsupportedFormat, r.Version, FormatVersion)
	}
	if r.Size < minimumBlobSize {
		return fmt.Errorf("%w: size %d is smaller than the minimum encoded size %d", ErrInvalidReference, r.Size, minimumBlobSize)
	}
	if r.SHA256 == ([32]byte{}) {
		return fmt.Errorf("%w: sha256 is required", ErrInvalidReference)
	}

	return nil
}

// Record is one raw key/value pair from an immutable logical history run.
// The codec does not interpret either side of the pair.
type Record struct {
	Key   []byte
	Value []byte
}

// RecordStream streams records to the encoder without materializing the
// encoded run in memory. Keys must be supplied in strictly increasing byte
// order. Record data is consumed before the next call to Next.
type RecordStream interface {
	Next() bool
	Record() Record
	Err() error
}

// Archive is the intentionally small cold-run boundary used by the history
// store. Fetch returns a lease: callers must close it after all readers opened
// from it have been closed so the local file can become evictable.
type Archive interface {
	Archive(ctx context.Context, records RecordStream) (Ref, error)
	Fetch(ctx context.Context, ref Ref) (*Lease, error)
	Available(ctx context.Context, ref Ref) (bool, error)
	Exists(ctx context.Context, ref Ref) (bool, error)
}

// IdentifiedArchive binds persisted content-addressed references to the
// physical destination and logical namespace that own them.
type IdentifiedArchive interface {
	Archive
	DestinationIdentity() string
}

// MissingError carries the immutable object identity that could not be found.
type MissingError struct {
	Ref   Ref
	Cause error
}

func (e *MissingError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", ErrMissing, e.Ref.Hex(), e.Cause)
	}

	return fmt.Sprintf("%s: %s", ErrMissing, e.Ref.Hex())
}

func (e *MissingError) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrMissing}
	}

	return []error{ErrMissing, e.Cause}
}

// CorruptError describes an integrity failure without weakening errors.Is
// checks against ErrCorrupt.
type CorruptError struct {
	Ref    Ref
	Detail string
	Cause  error
}

func (e *CorruptError) Error() string {
	message := fmt.Sprintf("%s: %s", ErrCorrupt, e.Ref.Hex())
	if e.Detail != "" {
		message += ": " + e.Detail
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}

	return message
}

func (e *CorruptError) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrCorrupt}
	}

	return []error{ErrCorrupt, e.Cause}
}

// SliceStream adapts an in-memory record slice to RecordStream. It is useful
// for small publications and tests; production callers can stream directly
// from their run iterator.
type SliceStream struct {
	records []Record
	next    int
	current Record
}

// NewSliceStream returns a single-use stream over records.
func NewSliceStream(records []Record) *SliceStream {
	return &SliceStream{records: records}
}

func (s *SliceStream) Next() bool {
	if s.next >= len(s.records) {
		s.current = Record{}

		return false
	}

	s.current = s.records[s.next]
	s.next++

	return true
}

func (s *SliceStream) Record() Record {
	return s.current
}

func (s *SliceStream) Err() error {
	return nil
}

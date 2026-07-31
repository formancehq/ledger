package balancehistoryarchive

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

const (
	archiveChapterID = uint64(0)
)

// Config configures the content-addressed cold namespace and local cache.
type Config struct {
	BaseBucketID  string `json:"baseBucketId"`
	OwnerID       string `json:"ownerId"`
	CacheDir      string `json:"cacheDir"`
	CacheMaxBytes int64  `json:"cacheMaxBytes"`
}

// Store implements Archive over the existing chapter-oriented ColdStorage
// interface without changing it. Each digest gets its own bucket namespace and
// uses chapter zero, making the resulting cold key content-addressed.
type Store struct {
	cold                coldstorage.ColdStorage
	catalog             coldstorage.ObjectCatalog
	namespace           objectNamespace
	destinationIdentity string
	cache               *cache
	metrics             *archiveMetrics
	closeOnce           sync.Once
	closeErr            error
}

// New creates a verified balance-history run archive.
func New(cold coldstorage.ColdStorage, config Config, meter metric.Meter) (*Store, error) {
	if cold == nil {
		return nil, errors.New("cold storage is required")
	}
	namespace, err := newObjectNamespace(config.BaseBucketID, config.OwnerID)
	if err != nil {
		return nil, err
	}
	identified, ok := cold.(coldstorage.DestinationIdentified)
	if !ok {
		return nil, errors.New("cold storage physical destination identity is required")
	}
	physicalIdentity, err := identified.DestinationIdentity()
	if err != nil {
		return nil, fmt.Errorf("identifying cold storage physical destination: %w", err)
	}
	if physicalIdentity == "" {
		return nil, errors.New("cold storage physical destination identity is empty")
	}

	cache, err := newCache(config.CacheDir, config.CacheMaxBytes)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("balancehistoryarchive")
	}
	metrics, err := newArchiveMetrics(meter, cache)
	if err != nil {
		return nil, err
	}

	return &Store{
		cold:                cold,
		catalog:             catalogFromColdStorage(cold),
		namespace:           namespace,
		destinationIdentity: namespace.destinationIdentity(physicalIdentity),
		cache:               cache,
		metrics:             metrics,
	}, nil
}

// DestinationIdentity returns the versioned, non-secret binding for the
// physical backend plus this Store's node-owned logical namespace.
func (s *Store) DestinationIdentity() string {
	return s.destinationIdentity
}

var _ IdentifiedArchive = (*Store)(nil)

// Archive encodes, verifies, and idempotently publishes one immutable run.
func (s *Store) Archive(ctx context.Context, records RecordStream) (Ref, error) {
	prepared, ref, err := s.encode(ctx, records)
	if err != nil {
		return Ref{}, err
	}
	keepPrepared := true
	defer func() {
		if keepPrepared {
			// The temporary file has no durable meaning before cache admission.
			_ = os.Remove(prepared.path)
		}
	}()

	exists, err := s.cold.Exists(ctx, s.objectBucket(ref), archiveChapterID)
	if err != nil {
		return Ref{}, fmt.Errorf("checking content-addressed balance history archive: %w", err)
	}
	if exists {
		if err := s.verifyRemote(ctx, ref); err != nil {
			return Ref{}, err
		}
	} else {
		file, err := os.Open(prepared.path)
		if err != nil {
			return Ref{}, fmt.Errorf("opening encoded balance history archive: %w", err)
		}
		archiveErr := s.cold.Archive(ctx, s.objectBucket(ref), archiveChapterID, file, ref.SHA256[:])
		closeErr := file.Close()
		if archiveErr != nil || closeErr != nil {
			return Ref{}, errors.Join(archiveErr, closeErr)
		}
		if err := s.verifyRemote(ctx, ref); err != nil {
			return Ref{}, err
		}
	}

	if err := s.cache.admit(prepared, ref); err != nil {
		return Ref{}, fmt.Errorf("admitting archived balance history run to local cache: %w", err)
	}
	keepPrepared = false

	return ref, nil
}

func (s *Store) encode(ctx context.Context, records RecordStream) (preparedFile, Ref, error) {
	file, err := os.CreateTemp(s.cache.dir, ".archive-*.tmp")
	if err != nil {
		return preparedFile{}, Ref{}, fmt.Errorf("creating balance history archive temp file: %w", err)
	}
	path := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			// A failed encoding is not a cache entry and can be discarded.
			_ = os.Remove(path)
		}
	}()

	buffered := bufio.NewWriterSize(file, 256*1024)
	ref, encodeErr := Encode(ctx, buffered, records)
	flushErr := buffered.Flush()
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(encodeErr, flushErr, syncErr, closeErr); err != nil {
		return preparedFile{}, Ref{}, err
	}
	if err := verifyFile(path, ref); err != nil {
		return preparedFile{}, Ref{}, err
	}

	cleanup = false

	return preparedFile{path: path, size: int64(ref.Size)}, ref, nil
}

// Fetch returns a local lease, downloading and verifying the object once
// across concurrent misses. Lease.Open performs the final verification
// immediately before exposing records and removes a corrupt cache entry.
func (s *Store) Fetch(ctx context.Context, ref Ref) (*Lease, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}

	lease, hit, err := s.cache.acquire(ctx, ref, func() (preparedFile, error) {
		started := time.Now()
		prepared, fetchErr := s.download(ctx, ref)
		s.metrics.recordFetch(ctx, started)

		return prepared, fetchErr
	})
	s.metrics.recordLookup(ctx, hit)
	if err != nil {
		return nil, err
	}

	return lease, nil
}

func (s *Store) download(ctx context.Context, ref Ref) (preparedFile, error) {
	bucket := s.objectBucket(ref)
	exists, err := s.cold.Exists(ctx, bucket, archiveChapterID)
	if err != nil {
		return preparedFile{}, fmt.Errorf("checking cold balance history run: %w", err)
	}
	if !exists {
		return preparedFile{}, &MissingError{Ref: ref}
	}

	expected, err := s.cold.ExpectedChecksum(ctx, bucket, archiveChapterID)
	if err != nil {
		if errors.Is(err, coldstorage.ErrChecksumNotFound) {
			return preparedFile{}, &MissingError{Ref: ref, Cause: err}
		}

		return preparedFile{}, &CorruptError{Ref: ref, Detail: "reading expected cold checksum", Cause: err}
	}
	if !bytes.Equal(expected, ref.SHA256[:]) {
		return preparedFile{}, &CorruptError{Ref: ref, Detail: "expected cold checksum does not match content address"}
	}

	reader, err := s.cold.Fetch(ctx, bucket, archiveChapterID)
	if err != nil {
		return preparedFile{}, fmt.Errorf("fetching cold balance history run: %w", err)
	}

	file, err := os.CreateTemp(s.cache.dir, ".fetch-*.tmp")
	if err != nil {
		closeErr := reader.Close()

		return preparedFile{}, errors.Join(fmt.Errorf("creating fetched run temp file: %w", err), closeErr)
	}
	path := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			// An incomplete download must never be considered a cache hit.
			_ = os.Remove(path)
		}
	}()

	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hasher), &contextReader{ctx: ctx, reader: reader})
	readerCloseErr := reader.Close()
	syncErr := file.Sync()
	fileCloseErr := file.Close()
	if err := errors.Join(copyErr, readerCloseErr, syncErr, fileCloseErr); err != nil {
		return preparedFile{}, err
	}
	if written < 0 || uint64(written) != ref.Size {
		return preparedFile{}, &CorruptError{
			Ref:    ref,
			Detail: fmt.Sprintf("downloaded size mismatch: expected %d, got %d", ref.Size, written),
		}
	}
	if calculated := hasher.Sum(nil); !bytes.Equal(calculated, expected) || !bytes.Equal(calculated, ref.SHA256[:]) {
		return preparedFile{}, &CorruptError{Ref: ref, Detail: "downloaded checksum mismatch"}
	}
	if err := verifyFile(path, ref); err != nil {
		return preparedFile{}, err
	}

	cleanup = false

	return preparedFile{path: path, size: written}, nil
}

// Available performs a head-only integrity check suitable for resolving a
// multipart manifest. It verifies that the committed object and expected
// checksum metadata exist and that the metadata equals the content address;
// it deliberately does not download or calculate the object checksum.
func (s *Store) Available(ctx context.Context, ref Ref) (bool, error) {
	if err := ref.validate(); err != nil {
		return false, err
	}

	bucket := s.objectBucket(ref)
	exists, err := s.cold.Exists(ctx, bucket, archiveChapterID)
	if err != nil {
		return false, fmt.Errorf("checking cold balance history archive availability: %w", err)
	}
	if !exists {
		return false, nil
	}

	expected, err := s.cold.ExpectedChecksum(ctx, bucket, archiveChapterID)
	if err != nil {
		if errors.Is(err, coldstorage.ErrChecksumNotFound) {
			return false, nil
		}
		if errors.Is(err, coldstorage.ErrChecksumMalformed) {
			return false, &CorruptError{Ref: ref, Detail: "reading expected cold checksum", Cause: err}
		}

		return false, fmt.Errorf("reading expected cold checksum: %w", err)
	}
	if !bytes.Equal(expected, ref.SHA256[:]) {
		return false, &CorruptError{Ref: ref, Detail: "expected cold checksum does not match content address"}
	}

	return true, nil
}

// Exists returns true only for a complete object whose expected and calculated
// cold checksums both equal the content-addressed reference.
func (s *Store) Exists(ctx context.Context, ref Ref) (bool, error) {
	if err := ref.validate(); err != nil {
		return false, err
	}

	exists, err := s.cold.Exists(ctx, s.objectBucket(ref), archiveChapterID)
	if err != nil {
		return false, fmt.Errorf("checking cold balance history archive: %w", err)
	}
	if !exists {
		return false, nil
	}
	if err := s.verifyRemote(ctx, ref); err != nil {
		if errors.Is(err, ErrMissing) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (s *Store) verifyRemote(ctx context.Context, ref Ref) error {
	bucket := s.objectBucket(ref)
	expected, err := s.cold.ExpectedChecksum(ctx, bucket, archiveChapterID)
	if err != nil {
		if errors.Is(err, coldstorage.ErrChecksumNotFound) {
			return &MissingError{Ref: ref, Cause: err}
		}

		return &CorruptError{Ref: ref, Detail: "reading expected cold checksum", Cause: err}
	}
	if !bytes.Equal(expected, ref.SHA256[:]) {
		return &CorruptError{Ref: ref, Detail: "expected cold checksum does not match content address"}
	}

	calculated, err := s.cold.Checksum(ctx, bucket, archiveChapterID)
	if err != nil {
		return &CorruptError{Ref: ref, Detail: "calculating cold checksum", Cause: err}
	}
	if !bytes.Equal(calculated, expected) || !bytes.Equal(calculated, ref.SHA256[:]) {
		return &CorruptError{Ref: ref, Detail: "calculated cold checksum does not match expected checksum"}
	}

	return nil
}

func (s *Store) objectBucket(ref Ref) string {
	return s.namespace.objectBucket(ref.SHA256)
}

// Namespace returns the stable, node-owned namespace this reclaimer is
// authorized to enumerate and delete. Collectors persist it beside their
// cursor so a configuration rotation can never replay queued digests against
// a different namespace.
func (s *Store) Namespace() string {
	return s.namespace.prefix
}

// List returns only canonical objects in this Store's stable owner namespace.
// Malformed and foreign keys are ignored, never promoted into deletion targets.
func (s *Store) List(ctx context.Context, cursor string, limit int) (RemoteObjectPage, error) {
	if s.catalog == nil {
		return RemoteObjectPage{}, ErrReclamationUnsupported
	}
	page, err := s.catalog.List(ctx, s.namespace.prefix, cursor, limit)
	if err != nil {
		return RemoteObjectPage{}, fmt.Errorf("listing owned balance history archives: %w", err)
	}

	owned := RemoteObjectPage{
		Objects:    make([]RemoteObject, 0, len(page.Objects)),
		NextCursor: page.NextCursor,
	}
	for _, object := range page.Objects {
		digest, ok := s.namespace.parse(object)
		if !ok {
			continue
		}
		owned.Objects = append(owned.Objects, RemoteObject{
			SHA256:       digest,
			Size:         object.Size,
			LastModified: object.LastModified,
		})
	}

	return owned, nil
}

// Delete idempotently removes one object from this Store's owner namespace.
func (s *Store) Delete(ctx context.Context, digest [32]byte) error {
	if s.catalog == nil {
		return ErrReclamationUnsupported
	}
	if digest == ([32]byte{}) {
		return errors.New("balance history archive digest is required for deletion")
	}
	if err := s.catalog.Delete(ctx, s.namespace.objectBucket(digest), archiveChapterID); err != nil {
		return fmt.Errorf("deleting owned balance history archive %x: %w", digest, err)
	}

	return nil
}

// CacheStats returns a point-in-time cache snapshot.
func (s *Store) CacheStats() CacheStats {
	return s.cache.stats()
}

// Close unregisters observable metrics. It does not invalidate outstanding
// leases because cached files have no process-owned open handles.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.metrics.close()
	})

	return s.closeErr
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	return r.reader.Read(buffer)
}

var _ Archive = (*Store)(nil)
var _ Reclaimer = (*Store)(nil)

func catalogFromColdStorage(cold coldstorage.ColdStorage) coldstorage.ObjectCatalog {
	catalog, _ := cold.(coldstorage.ObjectCatalog)

	return catalog
}

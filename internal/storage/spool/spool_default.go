package spool

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

var ErrCorrupt = errors.New("spool: corrupt record")

type DefaultSpoolConfig struct {
	Dir             string
	SegmentMaxBytes int64
	WriteBufBytes   int
	SyncEvery       int
	SyncMaxDelay    time.Duration
	Logger          logging.Logger
}

type Default struct {
	mu  sync.Mutex
	cfg DefaultSpoolConfig

	// current writer
	segID uint64
	f     *os.File
	w     *bufio.Writer
	size  int64

	segMinIndex uint64
	segMaxIndex uint64

	pendingN     int
	pendingSince time.Time

	// ---- read cache (RAM only) ----
	// Starting position for the next ReplayUntil
	rInit        bool
	rSegID       uint64
	rOffset      int64
	rLastApplied uint64 // last lastApplied seen during a replay
}

// --------------------
// Constantes format
// --------------------

const (
	recMagic   = 0x53504F4C // "SPOL"
	trailMagic = 0x54504F53 // "SPOT"
	recHdrLen  = 16
	trailerLen = 4 + 8 + 8 + 4
)

// --------------------
// Ouverture / fermeture
// --------------------

func NewDefault(cfg DefaultSpoolConfig) (*Default, error) {
	if cfg.Dir == "" {
		return nil, errors.New("dir required")
	}

	if cfg.SegmentMaxBytes <= 0 {
		cfg.SegmentMaxBytes = 256 << 20
	}

	if cfg.WriteBufBytes <= 0 {
		cfg.WriteBufBytes = 1 << 20
	}

	if cfg.SyncEvery <= 0 {
		cfg.SyncEvery = 1024
	}

	if cfg.SyncMaxDelay <= 0 {
		cfg.SyncMaxDelay = 200 * time.Millisecond
	}

	if cfg.Logger == nil {
		cfg.Logger = logging.NewDefaultLogger(os.Stderr, false, false, false)
	}

	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}

	ids, err := listSegments(cfg.Dir)
	if err != nil {
		return nil, err
	}

	s := &Default{cfg: cfg}
	if len(ids) == 0 {
		s.segID = 1
	} else {
		s.segID = ids[len(ids)-1]
	}

	if err := s.openWriter(s.segID); err != nil {
		return nil, err
	}

	// init read cache: if segments exist, start from the first segment.
	if len(ids) > 0 {
		s.rInit = true
		s.rSegID = ids[0]
		s.rOffset = 0
	} else {
		s.rInit = true
		s.rSegID = s.segID
		s.rOffset = 0
	}

	return s, nil
}

func (s *Default) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		_ = s.w.Flush()
	}

	if s.f != nil {
		_ = s.f.Sync()
		_ = s.writeTrailerLocked()

		return s.f.Close()
	}

	return nil
}

// --------------------
// Append
// --------------------

func (s *Default) AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		approx := s.size + recHdrLen + int64(e.Size()) + trailerLen
		if approx > s.cfg.SegmentMaxBytes {
			err := s.rotateLocked()
			if err != nil {
				return err
			}
		}

		n, err := writeRecord(s.w, e)
		if err != nil {
			return err
		}

		s.size += int64(n)

		if s.segMinIndex == 0 || e.Index < s.segMinIndex {
			s.segMinIndex = e.Index
		}

		if e.Index > s.segMaxIndex {
			s.segMaxIndex = e.Index
		}

		if s.pendingN == 0 {
			s.pendingSince = time.Now()
		}

		s.pendingN++

		if s.pendingN >= s.cfg.SyncEvery ||
			time.Since(s.pendingSince) >= s.cfg.SyncMaxDelay {
			err := s.flushAndSyncLocked()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// --------------------
// Watermark
// --------------------

func (s *Default) End() (*Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		err := s.w.Flush()
		if err != nil {
			return nil, err
		}
	}

	off, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	return &Position{SegID: s.segID, Offset: off}, nil
}

// --------------------
// Bounded replay + RAM read cache
// --------------------

// ReplayUntil replays records between the cached position (RAM) and the 'end' bound (watermark).
// It applies only entries whose Index > lastApplied.
// The cache advances as it goes, so a subsequent call won't re-parse what was already read.
//
// IMPORTANT: if lastApplied decreases (rewind), we reset the cache to the beginning for safety.
func (s *Default) ReplayUntil(
	ctx context.Context,
	end Position,
	lastApplied uint64,
	applyFn func(raftpb.Entry) error,
) error {
	ids, err := listSegments(s.cfg.Dir)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		return nil
	}

	// Load starting point from cache (RAM) + handle rewind
	startSeg, startOff := func() (uint64, int64) {
		s.mu.Lock()
		defer s.mu.Unlock()

		// If rewind (or first use), restart from the beginning.
		if !s.rInit || lastApplied < s.rLastApplied {
			s.rInit = true
			s.rSegID = ids[0]
			s.rOffset = 0
		}

		s.rLastApplied = lastApplied

		// If cache is beyond the end bound, nothing to do.
		if s.rSegID > end.SegID || (s.rSegID == end.SegID && s.rOffset >= end.Offset) {
			return s.rSegID, s.rOffset
		}

		return s.rSegID, s.rOffset
	}()

	// If start is already beyond end, stop.
	if startSeg > end.SegID || (startSeg == end.SegID && startOff >= end.Offset) {
		return nil
	}

	// Find the index of the starting segment in ids
	startIdx := 0
	for startIdx < len(ids) && ids[startIdx] < startSeg {
		startIdx++
	}
	// If the cache points to a deleted segment (pruned), restart from the next existing one.
	if startIdx >= len(ids) {
		return nil
	}

	if ids[startIdx] != startSeg {
		startSeg = ids[startIdx]
		startOff = 0

		s.mu.Lock()
		s.rSegID = startSeg
		s.rOffset = 0
		s.mu.Unlock()
	}

	// Loop through segments
	for i := startIdx; i < len(ids); i++ {
		segID := ids[i]
		if segID > end.SegID {
			return nil
		}

		path := segmentPath(s.cfg.Dir, segID)

		f, err := os.Open(path)
		if err != nil {
			return err
		}

		// Byte-level limit for the final segment
		var limit int64 = -1
		if segID == end.SegID {
			limit = end.Offset
		}

		// Optimization: if we're at the beginning of the segment (offset=0), we can skip the entire
		// segment via trailer if maxIndex <= lastApplied.
		// (If offset>0, we don't know if we're already "past" the useful entries, so we read.)
		curOff := int64(0)
		if segID == startSeg {
			curOff = startOff
		}

		if curOff == 0 {
			_, maxI, ok := readTrailer(f)
			if ok && maxI <= lastApplied {
				_ = f.Close()
				// advance the cache to the next segment
				s.advanceReadCache(segID, 0, true, ids, i)

				continue
			}
		}

		// Seek to the correct offset
		if _, err := f.Seek(curOff, io.SeekStart); err != nil {
			_ = f.Close()

			return err
		}

		r := bufio.NewReaderSize(f, 1<<20)

		for {
			select {
			case <-ctx.Done():
				_ = f.Close()

				return ctx.Err()
			default:
			}

			if limit >= 0 && curOff >= limit {
				break
			}

			e, n, err := readRecord(r)
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				// On the last segment, a corrupt record or unexpected EOF means
				// a partial/garbled write from a crash (SIGKILL, OOM). Truncate
				// at the last good offset and continue — the WAL has these
				// entries and they'll be replayed through normal Raft recovery.
				if (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrCorrupt)) && i == len(ids)-1 {
					s.cfg.Logger.Errorf("========================================")
					s.cfg.Logger.Errorf("SPOOL CORRUPTED: %v in segment %d at offset %d", err, segID, curOff)
					s.cfg.Logger.Errorf("Truncating corrupt tail and continuing...")
					s.cfg.Logger.Errorf("========================================")

					_ = f.Close()

					if truncErr := os.Truncate(path, curOff); truncErr != nil {
						return fmt.Errorf("truncating corrupted spool segment %d: %w", segID, truncErr)
					}

					break
				}

				_ = f.Close()

				return err
			}

			nextOff := curOff + int64(n)

			// Strict respect of the end bound on the final segment
			if limit >= 0 && nextOff > limit {
				// Stop before applying this partial/out-of-bounds record
				break
			}

			// Apply (if necessary)
			if e.Index > lastApplied {
				err := applyFn(e)
				if err != nil {
					_ = f.Close()

					return err
				}
				// NOTE: we don't update lastApplied here: the FSM persists it.
				// Here, we only use it as an input filter.
			}

			// Advance read cache after success (apply or skip)
			curOff = nextOff
			s.setReadCache(segID, curOff, lastApplied)
		}

		_ = f.Close()

		// If we finished this segment (or stopped before end on the final segment), advance.
		// If final segment and curOff >= limit, stop.
		if limit >= 0 && curOff >= limit {
			return nil
		}

		// Segment fini => passer au suivant (offset 0)
		s.advanceReadCache(segID, curOff, true, ids, i)
	}

	return nil
}

// ResetReadCache forces the next ReplayUntil to restart from the beginning (useful for debug/tests).
func (s *Default) ResetReadCache() error {
	ids, err := listSegments(s.cfg.Dir)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		s.mu.Lock()
		s.rInit = true
		s.rSegID = s.segID
		s.rOffset = 0
		s.mu.Unlock()

		return nil
	}

	s.mu.Lock()
	s.rInit = true
	s.rSegID = ids[0]
	s.rOffset = 0
	s.mu.Unlock()

	return nil
}

// Reset removes all spool segments and starts fresh. Called on startup because
// the WAL replay + leader sync make any pre-existing spool data obsolete.
func (s *Default) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close the current writer
	if s.w != nil {
		_ = s.w.Flush()
	}

	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
		s.w = nil
	}

	// Remove all segment files
	ids, err := listSegments(s.cfg.Dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		_ = os.Remove(segmentPath(s.cfg.Dir, id))
	}

	// Reinitialize
	s.segID = 1
	s.size = 0
	s.segMinIndex = 0
	s.segMaxIndex = 0
	s.pendingN = 0
	s.rInit = true
	s.rSegID = 1
	s.rOffset = 0
	s.rLastApplied = 0

	return s.openWriter(s.segID)
}

// Prune removes segments whose maxIndex <= lastApplied (optional).
func (s *Default) Prune(lastApplied uint64) error {
	ids, err := listSegments(s.cfg.Dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		f, err := os.Open(segmentPath(s.cfg.Dir, id))
		if err != nil {
			continue
		}

		_, maxI, ok := readTrailer(f)
		_ = f.Close()

		if ok && maxI <= lastApplied {
			_ = os.Remove(segmentPath(s.cfg.Dir, id))
		}
	}

	return nil
}

// --------------------
// Read cache helpers
// --------------------

func (s *Default) setReadCache(segID uint64, off int64, lastApplied uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rInit = true
	s.rSegID = segID
	s.rOffset = off
	s.rLastApplied = lastApplied
}

func (s *Default) advanceReadCache(curSeg uint64, curOff int64, moveNext bool, ids []uint64, idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rInit = true

	if !moveNext {
		s.rSegID = curSeg
		s.rOffset = curOff

		return
	}

	if idx+1 < len(ids) {
		s.rSegID = ids[idx+1]
		s.rOffset = 0
	} else {
		// Last segment: stay on it at the end (next replay will resume here)
		s.rSegID = curSeg
		s.rOffset = curOff
	}
}

// --------------------
// Internals writer
// --------------------

func (s *Default) rotateLocked() error {
	err := s.flushAndSyncLocked()
	if err != nil {
		return err
	}

	_ = s.writeTrailerLocked()
	_ = s.f.Sync()
	_ = s.f.Close()

	s.segID++

	return s.openWriter(s.segID)
}

func (s *Default) openWriter(id uint64) error {
	f, err := os.OpenFile(segmentPath(s.cfg.Dir, id),
		os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}

	pos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()

		return err
	}

	// If the segment ends with a trailer (written during a previous graceful
	// close), truncate it so new records are appended right after the last
	// data record. Without this, the trailer would be embedded in the middle
	// of the segment, causing corrupt-record errors on replay.
	if pos >= trailerLen {
		_, _, hasTrailer := readTrailer(f)
		if hasTrailer {
			pos -= trailerLen

			err := f.Truncate(pos)
			if err != nil {
				_ = f.Close()

				return err
			}
		}

		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			_ = f.Close()

			return err
		}
	}

	s.f = f
	s.w = bufio.NewWriterSize(f, s.cfg.WriteBufBytes)
	s.size = pos
	s.segMinIndex = 0
	s.segMaxIndex = 0
	s.pendingN = 0

	return nil
}

func (s *Default) flushAndSyncLocked() error {
	if s.w != nil {
		err := s.w.Flush()
		if err != nil {
			return err
		}
	}

	if s.f != nil {
		err := s.f.Sync()
		if err != nil {
			return err
		}
	}

	s.pendingN = 0

	return nil
}

func (s *Default) writeTrailerLocked() error {
	if s.segMaxIndex == 0 {
		return nil
	}

	buf := make([]byte, trailerLen)
	binary.LittleEndian.PutUint32(buf[0:4], trailMagic)
	binary.LittleEndian.PutUint64(buf[4:12], s.segMinIndex)
	binary.LittleEndian.PutUint64(buf[12:20], s.segMaxIndex)
	crc := crc32.ChecksumIEEE(buf[0:20])
	binary.LittleEndian.PutUint32(buf[20:24], crc)

	if s.w != nil {
		err := s.w.Flush()
		if err != nil {
			return err
		}
	}

	_, err := s.f.Write(buf)

	return err
}

// --------------------
// Encoding
// --------------------

func writeRecord(w io.Writer, e raftpb.Entry) (int, error) {
	payload, err := e.Marshal()
	if err != nil {
		return 0, err
	}

	crc := crc32.ChecksumIEEE(payload)

	h := make([]byte, recHdrLen)
	binary.LittleEndian.PutUint32(h[0:4], recMagic)
	binary.LittleEndian.PutUint32(h[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[8:12], crc)

	n1, err := w.Write(h)
	if err != nil {
		return n1, err
	}

	n2, err := w.Write(payload)

	return n1 + n2, err
}

func readRecord(r *bufio.Reader) (raftpb.Entry, int, error) {
	var e raftpb.Entry

	h := make([]byte, recHdrLen)
	if _, err := io.ReadFull(r, h); err != nil {
		return e, 0, err
	}

	magic := binary.LittleEndian.Uint32(h[0:4])
	if magic == trailMagic {
		// Trailer encountered mid-segment: treat as end of data records.
		return e, 0, io.EOF
	}

	if magic != recMagic {
		return e, 0, ErrCorrupt
	}

	n := int(binary.LittleEndian.Uint32(h[4:8]))
	want := binary.LittleEndian.Uint32(h[8:12])

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return e, 0, err
	}

	if crc32.ChecksumIEEE(payload) != want {
		return e, 0, ErrCorrupt
	}

	err := e.Unmarshal(payload)
	if err != nil {
		return e, 0, err
	}

	return e, recHdrLen + n, nil
}

// --------------------
// Utils
// --------------------

func readTrailer(f *os.File) (minI, maxI uint64, ok bool) {
	st, err := f.Stat()
	if err != nil || st.Size() < trailerLen {
		return 0, 0, false
	}

	if _, err := f.Seek(st.Size()-trailerLen, io.SeekStart); err != nil {
		return 0, 0, false
	}

	buf := make([]byte, trailerLen)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, 0, false
	}

	if binary.LittleEndian.Uint32(buf[0:4]) != trailMagic {
		return 0, 0, false
	}

	crc := binary.LittleEndian.Uint32(buf[20:24])
	if crc32.ChecksumIEEE(buf[0:20]) != crc {
		return 0, 0, false
	}

	return binary.LittleEndian.Uint64(buf[4:12]),
		binary.LittleEndian.Uint64(buf[12:20]), true
}

func segmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("spool-%020d.log", id))
}

func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ids []uint64

	for _, e := range ents {
		n := e.Name()
		if !strings.HasPrefix(n, "spool-") || !strings.HasSuffix(n, ".log") {
			continue
		}

		u, err := strconv.ParseUint(
			strings.TrimSuffix(strings.TrimPrefix(n, "spool-"), ".log"),
			10, 64,
		)
		if err == nil {
			ids = append(ids, u)
		}
	}

	slices.Sort(ids)

	return ids, nil
}

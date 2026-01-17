package raft

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

var ErrCorrupt = errors.New("spool: corrupt record")

type DefaultSpoolConfig struct {
	Dir             string
	SegmentMaxBytes int64
	WriteBufBytes   int
	SyncEvery       int
	SyncMaxDelay    time.Duration
}

type DefaultSpool struct {
	mu  sync.Mutex
	cfg DefaultSpoolConfig

	// writer courant
	segID uint64
	f     *os.File
	w     *bufio.Writer
	size  int64

	segMinIndex uint64
	segMaxIndex uint64

	pendingN     int
	pendingSince time.Time

	// ---- cache de lecture (RAM only) ----
	// Position de départ pour le prochain ReplayUntil
	rInit        bool
	rSegID       uint64
	rOffset      int64
	rLastApplied uint64 // dernier lastApplied vu lors d'un replay
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

func NewDefaultSpool(cfg DefaultSpoolConfig) (*DefaultSpool, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("Dir required")
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

	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}

	ids, err := listSegments(cfg.Dir)
	if err != nil {
		return nil, err
	}

	s := &DefaultSpool{cfg: cfg}
	if len(ids) == 0 {
		s.segID = 1
	} else {
		s.segID = ids[len(ids)-1]
	}
	if err := s.openWriter(s.segID); err != nil {
		return nil, err
	}

	// init cache lecture: si segments existent, partir du premier segment.
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

func (s *DefaultSpool) Close() error {
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

func (s *DefaultSpool) AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error {
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
			if err := s.rotateLocked(); err != nil {
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
			if err := s.flushAndSyncLocked(); err != nil {
				return err
			}
		}
	}
	return nil
}

// --------------------
// Watermark
// --------------------

func (s *DefaultSpool) End() (*Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		if err := s.w.Flush(); err != nil {
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
// Replay borné + cache lecture RAM
// --------------------

// ReplayUntil rejoue les records entre la position cachee (RAM) et la borne 'end' (watermark).
// Il applique seulement les entries dont Index > lastApplied.
// Le cache avance au fur et a mesure, donc un appel suivant ne reparsera pas ce qui a deja ete lu.
//
// IMPORTANT: si lastApplied diminue (rewind), on reset le cache au debut pour etre safe.
func (s *DefaultSpool) ReplayUntil(
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

	// Charger point de départ depuis cache (RAM) + gestion rewind
	startSeg, startOff := func() (uint64, int64) {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Si rewind (ou premier usage), repartir au debut.
		if !s.rInit || lastApplied < s.rLastApplied {
			s.rInit = true
			s.rSegID = ids[0]
			s.rOffset = 0
		}
		s.rLastApplied = lastApplied

		// Si cache au-dela de la borne end, rien a faire.
		if s.rSegID > end.SegID || (s.rSegID == end.SegID && s.rOffset >= end.Offset) {
			return s.rSegID, s.rOffset
		}
		return s.rSegID, s.rOffset
	}()

	// Si start est deja au-dela de end, stop.
	if startSeg > end.SegID || (startSeg == end.SegID && startOff >= end.Offset) {
		return nil
	}

	// Trouver l'index du segment de depart dans ids
	startIdx := 0
	for startIdx < len(ids) && ids[startIdx] < startSeg {
		startIdx++
	}
	// Si le cache pointe sur un seg supprime (prune), on repart au prochain existant.
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

	// Boucler segments
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

		// Limite byte-level pour le segment final
		var limit int64 = -1
		if segID == end.SegID {
			limit = end.Offset
		}

		// Optimisation: si on est au début du segment (offset=0), on peut skipper le segment
		// entier via trailer si maxIndex <= lastApplied.
		// (Si offset>0, on ne sait pas si on est deja "apres" les entries utiles, donc on lit.)
		curOff := int64(0)
		if segID == startSeg {
			curOff = startOff
		}

		if curOff == 0 {
			_, maxI, ok := readTrailer(f)
			if ok && maxI <= lastApplied {
				_ = f.Close()
				// avancer le cache au segment suivant
				s.advanceReadCache(segID, 0, true, ids, i)
				continue
			}
		}

		// Se positionner au bon offset
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
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = f.Close()
				return err
			}

			nextOff := curOff + int64(n)

			// Respect strict de la borne end sur le segment final
			if limit >= 0 && nextOff > limit {
				// On s'arrete avant d'appliquer ce record partiel/hors-borne
				break
			}

			// Apply (si necessaire)
			if e.Index > lastApplied {
				if err := applyFn(e); err != nil {
					_ = f.Close()
					return err
				}
				// NOTE: on ne met pas a jour lastApplied ici: c'est ton FSM qui
				// le persiste. Ici, on l'utilise seulement comme filtre d'entree.
			}

			// Avancer cache de lecture apres succes (apply ou skip)
			curOff = nextOff
			s.setReadCache(segID, curOff, lastApplied)
		}

		_ = f.Close()

		// Si on a termine ce segment (ou qu'on s'est arrete avant end sur seg final), avancer.
		// Si seg final et curOff >= limit, on stop.
		if limit >= 0 && curOff >= limit {
			return nil
		}

		// Segment fini => passer au suivant (offset 0)
		s.advanceReadCache(segID, curOff, true, ids, i)
	}

	return nil
}

// ResetReadCache force le prochain ReplayUntil a repartir du debut (utile en debug / tests).
func (s *DefaultSpool) ResetReadCache() error {
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

// Prune supprime les segments dont maxIndex <= lastApplied (optionnel).
func (s *DefaultSpool) Prune(lastApplied uint64) error {
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
// Helpers cache lecture
// --------------------

func (s *DefaultSpool) setReadCache(segID uint64, off int64, lastApplied uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rInit = true
	s.rSegID = segID
	s.rOffset = off
	s.rLastApplied = lastApplied
}

func (s *DefaultSpool) advanceReadCache(curSeg uint64, curOff int64, moveNext bool, ids []uint64, idx int) {
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
		// Dernier segment: rester dessus a la fin (prochain replay reprendra ici)
		s.rSegID = curSeg
		s.rOffset = curOff
	}
}

// --------------------
// Internals writer
// --------------------

func (s *DefaultSpool) rotateLocked() error {
	if err := s.flushAndSyncLocked(); err != nil {
		return err
	}
	_ = s.writeTrailerLocked()
	_ = s.f.Sync()
	_ = s.f.Close()

	s.segID++
	return s.openWriter(s.segID)
}

func (s *DefaultSpool) openWriter(id uint64) error {
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

	s.f = f
	s.w = bufio.NewWriterSize(f, s.cfg.WriteBufBytes)
	s.size = pos
	s.segMinIndex = 0
	s.segMaxIndex = 0
	s.pendingN = 0
	return nil
}

func (s *DefaultSpool) flushAndSyncLocked() error {
	if s.w != nil {
		if err := s.w.Flush(); err != nil {
			return err
		}
	}
	if s.f != nil {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	s.pendingN = 0
	return nil
}

func (s *DefaultSpool) writeTrailerLocked() error {
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
		if err := s.w.Flush(); err != nil {
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
	if binary.LittleEndian.Uint32(h[0:4]) != recMagic {
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
	if err := e.Unmarshal(payload); err != nil {
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
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

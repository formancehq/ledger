package raft

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source spool.go -destination spool_generated_test.go -typed -package raft . Spool
type Spool interface {
	AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error
	Next() (*raftpb.Entry, error)
	Reset() error
	Close() error
}

type DefaultSpool struct {
	f    *os.File
	w    *bufio.Writer
	path string

	readOffset int64
}

func NewDefaultSpool(path string) (*DefaultSpool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &DefaultSpool{
		f:    f,
		w:    bufio.NewWriterSize(f, 1<<20),
		path: path,
	}, nil
}

func (s *DefaultSpool) Close() error {
	if s.w != nil {
		_ = s.w.Flush()
	}
	return s.f.Close()
}

func (s *DefaultSpool) AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error {

	// se placer en fin de fichier
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	for _, cmd := range entries {
		if err := writeRecord(s.w, cmd); err != nil {
			return err
		}
	}

	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *DefaultSpool) Next() (*raftpb.Entry, error) {
	if _, err := s.f.Seek(s.readOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(s.f, 1<<20)

	off := s.readOffset
	entry, n, err := readRecord(r)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		// en cas de crash au milieu d'un record, tu peux choisir :
		// - de tronquer à off et repartir propre
		return nil, err
	}

	// todo: write somewhere to avoid replaying all commands if the node is restarted
	s.readOffset = off + int64(n)
	return entry, nil
}

// Reset efface complètement le spool (ex: une fois que tu as fini le rattrapage + replay).
func (s *DefaultSpool) Reset() error {

	if err := s.f.Truncate(0); err != nil {
		return err
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s.readOffset = 0
	s.w.Reset(s.f)
	return s.f.Sync()
}

var (
	ErrCorrupt = fmt.Errorf("record corrupted")
)

func writeRecord(w io.Writer, cmd raftpb.Entry) error {
	payload, err := cmd.Marshal()
	if err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(payload)

	hdr := make([]byte, 16)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc)

	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readRecord(r *bufio.Reader) (*raftpb.Entry, int, error) {
	var entry raftpb.Entry

	hdr := make([]byte, 16)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, 0, err
	}
	n := int(binary.LittleEndian.Uint32(hdr[:4]))
	crc := binary.LittleEndian.Uint32(hdr[4:8])

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(payload) != crc {
		return nil, 0, ErrCorrupt
	}

	if err := entry.Unmarshal(payload); err != nil {
		return nil, 0, err
	}

	return &entry, 16 + n, nil
}

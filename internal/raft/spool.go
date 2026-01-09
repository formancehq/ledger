package raft

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source spool.go -destination spool_generated_test.go -package raft . Spool
type Spool interface {
	AppendCommittedEntries(ctx context.Context, commands ...*Command) error
	Next() (*Command, error)
	Reset() error
	Close() error
}

type fileSpool struct {
	f    *os.File
	w    *bufio.Writer
	path string

	readOffset int64
}

func newFileSpool(path string) (*fileSpool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &fileSpool{
		f:    f,
		w:    bufio.NewWriterSize(f, 1<<20),
		path: path,
	}, nil
}

func (s *fileSpool) Close() error {
	if s.w != nil {
		_ = s.w.Flush()
	}
	return s.f.Close()
}

func (s *fileSpool) AppendCommittedEntries(ctx context.Context, commands ...*Command) error {

	// se placer en fin de fichier
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	for _, cmd := range commands {
		if err := writeRecord(s.w, cmd); err != nil {
			return err
		}
	}

	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *fileSpool) Next() (*Command, error) {
	if _, err := s.f.Seek(s.readOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(s.f, 1<<20)

	off := s.readOffset
	cmd, n, err := readRecord(r)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		// en cas de crash au milieu d'un record, tu peux choisir :
		// - de tronquer à off et repartir propre
		return nil, err
	}

	// todo: write somewhere to avoid replaying all commands if the rawNode is restarted
	s.readOffset = off + int64(n)
	return cmd, nil
}

// Reset efface complètement le spool (ex: une fois que tu as fini le rattrapage + replay).
func (s *fileSpool) Reset() error {

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

/*
Record format (little endian):

u32  magic = 0x53504F4C ('SPOL')
u32  len(payload)
u32  crc32(payload)
payload = Command marshaled
*/

const (
	recordDelimiter = 0x53504F4C
)

var (
	ErrCorrupt = fmt.Errorf("record corrupted")
)

func writeRecord(w io.Writer, cmd *Command) error {
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(payload)

	hdr := make([]byte, 16)
	binary.LittleEndian.PutUint32(hdr[0:4], recordDelimiter) // "SPOL"
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[8:12], crc)
	// hdr[12:16] reserved

	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readRecord(r *bufio.Reader) (*Command, int, error) {
	var cmd Command

	hdr := make([]byte, 16)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, 0, err
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != 0x53504F4C {
		return nil, 0, ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint32(hdr[4:8]))
	wantCRC := binary.LittleEndian.Uint32(hdr[8:12])

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return nil, 0, ErrCorrupt
	}

	if err := proto.Unmarshal(payload, &cmd); err != nil {
		return nil, 0, err
	}

	return &cmd, 16 + n, nil
}

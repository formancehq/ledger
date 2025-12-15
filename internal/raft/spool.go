package raft

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type spool struct {
	f    *os.File
	w    *bufio.Writer
	path string

	// offset du prochain record à lire (pour replay incrémental)
	readOffset int64
}

func newSpool(path string) (*spool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &spool{
		f:    f,
		w:    bufio.NewWriterSize(f, 1<<20),
		path: path,
	}, nil
}

func (s *spool) Close() error {
	if s.w != nil {
		_ = s.w.Flush()
	}
	return s.f.Close()
}

// AppendCommittedEntries écrit des committed entries (déjà ordonnées) dans le spool.
// À appeler quand storageReady == false, au lieu d’appliquer au FSM.
func (s *spool) AppendCommittedEntries(ctx context.Context, commands ...Command) error {
	// se placer en fin de fichier
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	for _, cmd := range commands {
		if err := writeRecord(s.w, cmd); err != nil {
			return err
		}
	}

	// Flush + fsync pour durabilité (tu peux le faire toutes les N entrées pour perf)
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// Replay lit tous les records à partir de readOffset, appelle fn(entry) pour chacun,
// et avance readOffset si fn réussit.
func (s *spool) Replay(fn func(command Command) error) error {

	if _, err := s.f.Seek(s.readOffset, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(s.f, 1<<20)

	for {
		off := s.readOffset
		cmd, n, err := readRecord(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			// en cas de crash au milieu d’un record, tu peux choisir :
			// - de tronquer à off et repartir propre
			return err
		}

		if err := fn(cmd); err != nil {
			return err
		}

		// record consommé
		s.readOffset = off + int64(n)
	}
}

// Reset efface complètement le spool (ex: une fois que tu as fini le rattrapage + replay).
func (s *spool) Reset() error {

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

func writeRecord(w io.Writer, cmd Command) error {
	payload, err := cmd.MarshalBinary()
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

func readRecord(r *bufio.Reader) (Command, int, error) {
	var cmd Command

	hdr := make([]byte, 16)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return cmd, 0, err
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != 0x53504F4C {
		return cmd, 0, ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint32(hdr[4:8]))
	wantCRC := binary.LittleEndian.Uint32(hdr[8:12])

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return cmd, 0, err
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return cmd, 0, ErrCorrupt
	}

	if err := cmd.UnmarshalBinary(payload); err != nil {
		return cmd, 0, err
	}

	return cmd, 16 + n, nil
}


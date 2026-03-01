package grpc

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

const (
	// defaultChunkSize is the default size of each chunk sent in streaming RPCs (64KB).
	defaultChunkSize = 64 * 1024
)

// TarStreamChunk represents a single chunk of a tar archive being streamed.
type TarStreamChunk struct {
	Data               []byte
	ChunkOffset        uint64
	IsFirst            bool
	IsEOF              bool
	ContentSHA256      string
	ContentSize        uint64
	EstimatedTotalSize uint64 // Set on first chunk; estimated tar archive size
}

// estimateTarSize walks dirPath and returns the estimated tar archive size.
// Each entry has a 512-byte header; file data is padded to 512-byte boundaries;
// the archive ends with two 512-byte zero blocks.
func estimateTarSize(dirPath string) (uint64, error) {
	var total uint64
	err := filepath.Walk(dirPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		total += 512 // tar header
		if !info.IsDir() {
			size := uint64(info.Size())
			total += size
			if rem := size % 512; rem != 0 {
				total += 512 - rem // padding to 512-byte boundary
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	total += 1024 // end-of-archive marker (two zero blocks)
	return total, nil
}

// StreamDirAsTar creates a tar archive of dirPath and streams it in chunks via sendChunk.
// If offset > 0, bytes before that offset are skipped (for resumption).
func StreamDirAsTar(dirPath string, offset uint64, sendChunk func(TarStreamChunk) error) error {
	// Pre-compute estimated tar size for progress reporting
	estimatedSize, err := estimateTarSize(dirPath)
	if err != nil {
		return err
	}

	// Create a pipe to stream tar data
	pr, pw := io.Pipe()

	// Hash writer to calculate SHA256
	hash := sha256.New()

	// Channel to collect errors from the tar writer goroutine
	errCh := make(chan error, 1)

	// Start tar writer in a goroutine
	go func() {
		defer func() {
			_ = pw.Close()
		}()

		tw := tar.NewWriter(io.MultiWriter(pw, hash))
		defer func() {
			_ = tw.Close()
		}()

		err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Get relative path
			relPath, err := filepath.Rel(dirPath, path)
			if err != nil {
				return err
			}

			// Create tar header
			header, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			header.Name = relPath

			if err := tw.WriteHeader(header); err != nil {
				return err
			}

			// Write file content if not a directory
			if !info.IsDir() {
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer func() {
					_ = f.Close()
				}()

				if _, err := io.Copy(tw, f); err != nil {
					return err
				}
			}

			return nil
		})

		errCh <- err
	}()

	// Stream data in chunks
	var (
		currentOffset uint64
		totalSize     uint64
		headerSent    bool
	)

	buf := make([]byte, defaultChunkSize)
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			// Skip bytes if we're resuming from an offset
			if currentOffset < offset {
				skip := offset - currentOffset
				if skip >= uint64(n) {
					currentOffset += uint64(n)
					continue
				}
				buf = buf[skip:]
				n -= int(skip)
				currentOffset = offset
			}

			chunk := TarStreamChunk{
				Data:        buf[:n],
				ChunkOffset: currentOffset,
				IsFirst:     !headerSent,
			}
			if !headerSent {
				chunk.EstimatedTotalSize = estimatedSize
			}
			headerSent = true

			if err := sendChunk(chunk); err != nil {
				return err
			}

			currentOffset += uint64(n)
			totalSize += uint64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	// Wait for tar writer to finish and check for errors
	if err := <-errCh; err != nil {
		return err
	}

	// Send final chunk with EOF
	return sendChunk(TarStreamChunk{
		IsFirst:       !headerSent,
		IsEOF:         true,
		ChunkOffset:   currentOffset,
		ContentSHA256: hex.EncodeToString(hash.Sum(nil)),
		ContentSize:   totalSize,
	})
}

package upgrade

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// parseChecksums parses a checksums.txt file (goreleaser format: "<sha256>  <filename>")
// and returns a map from filename to hex-encoded SHA256.
func parseChecksums(r io.Reader) (map[string]string, error) {
	m := make(map[string]string)
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// goreleaser format: "<sha256>  <filename>" (two spaces)
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 {
			continue
		}

		m[strings.TrimSpace(parts[1])] = strings.TrimSpace(parts[0])
	}

	return m, scanner.Err()
}

// downloadAndVerify downloads the archive and checksums, verifies the archive's
// SHA256 hash, then extracts the ledgerctl binary to a temp file.
// Returns the path to the temp file containing the extracted binary.
func downloadAndVerify(archiveAsset, checksumsAsset *assetInfo, spinner *cmdutil.Spinner) (string, error) {
	// 1. Download checksums.txt (small, in memory).
	spinner.UpdateText("Downloading checksums...")

	checksumsResp, err := githubDownload(checksumsAsset.BrowserDownloadURL)
	if err != nil {
		return "", fmt.Errorf("downloading checksums: %w", err)
	}

	defer func() { _ = checksumsResp.Body.Close() }()

	checksums, err := parseChecksums(checksumsResp.Body)
	if err != nil {
		return "", fmt.Errorf("parsing checksums: %w", err)
	}

	expectedHash, ok := checksums[archiveAsset.Name]
	if !ok {
		return "", fmt.Errorf("no checksum found for %s in checksums.txt", archiveAsset.Name)
	}

	// 2. Download archive to temp file, computing SHA256 as we go.
	spinner.UpdateText(fmt.Sprintf("Downloading %s...", archiveAsset.Name))

	archiveResp, err := githubDownload(archiveAsset.BrowserDownloadURL)
	if err != nil {
		return "", fmt.Errorf("downloading archive: %w", err)
	}

	defer func() { _ = archiveResp.Body.Close() }()

	tmpArchive, err := os.CreateTemp("", "ledgerctl-upgrade-*")
	if err != nil {
		return "", fmt.Errorf("creating temp file: %w", err)
	}

	defer func() {
		_ = tmpArchive.Close()
		_ = os.Remove(tmpArchive.Name())
	}()

	hash := sha256.New()
	tee := io.TeeReader(archiveResp.Body, hash)

	var written int64

	buf := make([]byte, 32*1024)
	for {
		n, readErr := tee.Read(buf)
		if n > 0 {
			if _, wErr := tmpArchive.Write(buf[:n]); wErr != nil {
				return "", fmt.Errorf("writing archive: %w", wErr)
			}

			written += int64(n)
			spinner.UpdateText(fmt.Sprintf("Downloading %s... %s",
				archiveAsset.Name, cmdutil.FormatBytes(uint64(written))))
		}

		if readErr == io.EOF {
			break
		}

		if readErr != nil {
			return "", fmt.Errorf("reading archive: %w", readErr)
		}
	}

	// 3. Verify checksum.
	spinner.UpdateText("Verifying SHA256 checksum...")

	actualHash := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actualHash, expectedHash) {
		return "", fmt.Errorf("checksum verification failed: expected %s, got %s", expectedHash, actualHash)
	}

	// 4. Extract the ledgerctl binary from the platform archive.
	spinner.UpdateText("Extracting ledgerctl...")

	if _, err := tmpArchive.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seeking archive: %w", err)
	}

	archiveInfo, err := tmpArchive.Stat()
	if err != nil {
		return "", fmt.Errorf("reading archive metadata: %w", err)
	}

	return extractBinary(
		tmpArchive,
		archiveInfo.Size(),
		archiveAsset.Name,
		executableName(runtime.GOOS),
	)
}

// extractBinary extracts a named file from a supported release archive and writes it to a temp file.
func extractBinary(archive io.ReaderAt, archiveSize int64, archiveName, binaryName string) (string, error) {
	switch {
	case strings.HasSuffix(archiveName, ".tar.gz"):
		return extractTarGzBinary(io.NewSectionReader(archive, 0, archiveSize), binaryName)
	case strings.HasSuffix(archiveName, ".zip"):
		return extractZipBinary(archive, archiveSize, binaryName)
	default:
		return "", fmt.Errorf("unsupported archive format %q", archiveName)
	}
}

func extractTarGzBinary(archive io.Reader, binaryName string) (string, error) {
	gz, err := gzip.NewReader(archive)
	if err != nil {
		return "", fmt.Errorf("opening gzip reader: %w", err)
	}

	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", fmt.Errorf("reading tar: %w", err)
		}

		if isBinaryEntry(hdr.Name, binaryName) {
			return writeExtractedBinary(tr)
		}
	}

	return "", fmt.Errorf("binary %q not found in archive", binaryName)
}

func extractZipBinary(archive io.ReaderAt, archiveSize int64, binaryName string) (string, error) {
	zr, err := zip.NewReader(archive, archiveSize)
	if err != nil {
		return "", fmt.Errorf("opening zip reader: %w", err)
	}

	for _, file := range zr.File {
		if !isBinaryEntry(file.Name, binaryName) {
			continue
		}

		binary, err := file.Open()
		if err != nil {
			return "", fmt.Errorf("opening binary in zip: %w", err)
		}

		path, extractErr := writeExtractedBinary(binary)
		closeErr := binary.Close()

		if extractErr != nil {
			return "", extractErr
		}

		if closeErr != nil {
			_ = os.Remove(path)

			return "", fmt.Errorf("closing binary in zip: %w", closeErr)
		}

		return path, nil
	}

	return "", fmt.Errorf("binary %q not found in archive", binaryName)
}

func isBinaryEntry(entryName, binaryName string) bool {
	return entryName == binaryName || strings.HasSuffix(entryName, "/"+binaryName)
}

func writeExtractedBinary(binary io.Reader) (string, error) {
	tmpBinary, err := os.CreateTemp("", "ledgerctl-new-*")
	if err != nil {
		return "", fmt.Errorf("creating temp binary: %w", err)
	}

	if _, err := io.Copy(tmpBinary, binary); err != nil {
		_ = tmpBinary.Close()
		_ = os.Remove(tmpBinary.Name())

		return "", fmt.Errorf("extracting binary: %w", err)
	}

	if err := tmpBinary.Close(); err != nil {
		_ = os.Remove(tmpBinary.Name())

		return "", fmt.Errorf("closing temp binary: %w", err)
	}

	return tmpBinary.Name(), nil
}

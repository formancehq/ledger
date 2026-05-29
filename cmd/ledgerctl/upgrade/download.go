package upgrade

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pterm/pterm"

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
func downloadAndVerify(archiveAsset, checksumsAsset *assetInfo, spinner *pterm.SpinnerPrinter) (string, error) {
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

	tmpArchive, err := os.CreateTemp("", "ledgerctl-upgrade-*.tar.gz")
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

	// 4. Extract ledgerctl binary from tar.gz.
	spinner.UpdateText("Extracting ledgerctl...")

	if _, err := tmpArchive.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seeking archive: %w", err)
	}

	return extractBinary(tmpArchive, "ledgerctl")
}

// extractBinary extracts a named file from a tar.gz archive and writes it to a temp file.
// Returns the path to the temp file.
func extractBinary(archive io.Reader, name string) (string, error) {
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

		if hdr.Name == name || strings.HasSuffix(hdr.Name, "/"+name) {
			tmpBinary, err := os.CreateTemp("", "ledgerctl-new-*")
			if err != nil {
				return "", fmt.Errorf("creating temp binary: %w", err)
			}

			if _, err := io.Copy(tmpBinary, tr); err != nil {
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
	}

	return "", fmt.Errorf("binary %q not found in archive", name)
}

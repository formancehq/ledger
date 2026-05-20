package query

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/semver"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadNumscriptLatestVersion reads the latest version string for a numscript by ledger and name
// from the attributes zone (0xF1).
// Returns "" if the numscript does not exist.
func ReadNumscriptLatestVersion(attr *attributes.Attribute[*commonpb.NumscriptVersionValue], reader dal.PebbleReader, ledgerID uint32, name string) (string, error) {
	val, err := attr.Get(reader, domain.NumscriptVersionKey{LedgerID: ledgerID, Name: name}.Bytes())
	if err != nil {
		return "", fmt.Errorf("reading numscript latest version for %d/%q: %w", ledgerID, name, err)
	}

	if val == nil {
		return "", nil
	}

	return val.GetVersion(), nil
}

// ReadNumscript reads a numscript by ledger, name and version spec from the attributes zone (0xF1).
//   - ""          → read latest pointer, then fetch that version
//   - "latest"    → read latest pointer, then fetch that version
//   - "1.0.0"     → direct Get on exact version
//   - "1.0"       → scan all versions for (ledger, name), find highest matching
//   - "1"         → scan all versions for (ledger, name), find highest matching
//
// Returns nil if the numscript or version does not exist.
func ReadNumscript(
	versionAttr *attributes.Attribute[*commonpb.NumscriptVersionValue],
	contentAttr *attributes.Attribute[*commonpb.NumscriptInfo],
	reader dal.PebbleReader,
	ledgerID uint32, name string,
	version string,
) (*commonpb.NumscriptInfo, error) {
	if version == "latest" {
		// Direct lookup for the "latest" slot content.
		return readNumscriptExact(contentAttr, reader, ledgerID, name, "latest")
	}

	if version == "" {
		latestVersion, err := ReadNumscriptLatestVersion(versionAttr, reader, ledgerID, name)
		if err != nil {
			return nil, err
		}

		if latestVersion == "" {
			return nil, nil
		}

		return readNumscriptExact(contentAttr, reader, ledgerID, name, latestVersion)
	}

	major, minor, patch, depth, err := semver.ParsePartial(version)
	if err != nil {
		return nil, nil
	}

	if depth == 3 {
		return readNumscriptExact(contentAttr, reader, ledgerID, name, fmt.Sprintf("%d.%d.%d", major, minor, patch))
	}

	return resolvePartialVersion(contentAttr, reader, ledgerID, name, major, minor, depth)
}

// ReadAllNumscripts lists all numscripts for a ledger by scanning the latest version pointers
// from the attributes zone, then fetching each script's content.
func ReadAllNumscripts(
	versionAttr *attributes.Attribute[*commonpb.NumscriptVersionValue],
	contentAttr *attributes.Attribute[*commonpb.NumscriptInfo],
	reader dal.PebbleReader,
	ledgerID uint32,
) ([]*commonpb.NumscriptInfo, error) {
	// Scan all version pointers for this ledger.
	// The canonical key prefix is [ledgerID BE 4B].
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, ledgerID)
	entries, err := versionAttr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning numscript versions for ledger %d: %w", ledgerID, err)
	}

	var scripts []*commonpb.NumscriptInfo

	for _, entry := range entries {
		version := entry.Value.GetVersion()
		if version == "" {
			continue // deleted
		}

		// Reconstruct the name from the canonical key: [ledger\x00name]
		// Skip ledger + \x00 prefix
		nameBytes := entry.CanonicalKey[len(prefix):]
		name := string(nameBytes)

		info, err := readNumscriptExact(contentAttr, reader, ledgerID, name, version)
		if err != nil {
			return nil, err
		}

		if info != nil {
			scripts = append(scripts, info)
		}
	}

	return scripts, nil
}

// readNumscriptExact does a direct Get on the exact version key in the attributes zone.
func readNumscriptExact(attr *attributes.Attribute[*commonpb.NumscriptInfo], reader dal.PebbleReader, ledgerID uint32, name, version string) (*commonpb.NumscriptInfo, error) {
	return attr.Get(reader, domain.NumscriptEntryKey{LedgerID: ledgerID, Name: name, Version: version}.Bytes())
}

// resolvePartialVersion scans all versions for (ledger, name) from the attributes zone
// and finds the highest matching semver.
func resolvePartialVersion(attr *attributes.Attribute[*commonpb.NumscriptInfo], reader dal.PebbleReader, ledgerID uint32, name string, targetMajor, targetMinor uint32, depth int) (*commonpb.NumscriptInfo, error) {
	// Scan all versions for this (ledger, name) by using the common prefix.
	prefix := domain.NumscriptEntryKey{LedgerID: ledgerID, Name: name, Version: ""}.Bytes()

	entries, err := attr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning numscript versions for %d/%q: %w", ledgerID, name, err)
	}

	var (
		bestInfo             *commonpb.NumscriptInfo
		bestMajor, bestMinor uint32
		bestPatch            uint32
		found                bool
	)

	for _, entry := range entries {
		info := entry.Value
		major, minor, patch, d, parseErr := semver.ParsePartial(info.GetVersion())

		if parseErr != nil || d != 3 {
			continue
		}

		matches := false

		switch depth {
		case 1:
			matches = major == targetMajor
		case 2:
			matches = major == targetMajor && minor == targetMinor
		}

		if !matches {
			continue
		}

		isHigher := major > bestMajor ||
			(major == bestMajor && minor > bestMinor) ||
			(major == bestMajor && minor == bestMinor && patch > bestPatch)
		if !found || isHigher {
			bestInfo = info
			bestMajor = major
			bestMinor = minor
			bestPatch = patch
			found = true
		}
	}

	return bestInfo, nil
}

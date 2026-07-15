package query

import (
	"fmt"
	"sort"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadNumscriptLatestVersion reads the per-name latest pointer (the greatest
// stored semver) from the attributes zone. Returns "" if the numscript does
// not exist.
func ReadNumscriptLatestVersion(attr *attributes.Attribute[*commonpb.NumscriptVersionValue], reader dal.PebbleGetter, ledgerName string, name string) (string, error) {
	val, err := attr.Get(reader, domain.NumscriptVersionKey{LedgerName: ledgerName, Name: name}.Bytes())
	if err != nil {
		return "", fmt.Errorf("reading numscript latest version for %q/%q: %w", ledgerName, name, err)
	}

	if val == nil {
		return "", nil
	}

	return val.GetVersion(), nil
}

// ReadNumscript reads a numscript by ledger, name and version spec:
//   - "" / "latest" → the greatest stored semver (via the latest pointer)
//   - "1.0.0"       → direct Get on the exact version
//   - "1" / "1.2"   → highest matching semver (read-only partial selector)
//
// Returns nil if the numscript or version does not exist.
func ReadNumscript(
	versionAttr *attributes.Attribute[*commonpb.NumscriptVersionValue],
	contentAttr *attributes.Attribute[*commonpb.NumscriptInfo],
	reader dal.PebbleReader,
	ledgerName string, name string,
	version string,
) (*commonpb.NumscriptInfo, error) {
	if version == "" || version == "latest" {
		latestVersion, err := ReadNumscriptLatestVersion(versionAttr, reader, ledgerName, name)
		if err != nil {
			return nil, err
		}

		if latestVersion == "" {
			return nil, nil
		}

		return readNumscriptExact(contentAttr, reader, ledgerName, name, latestVersion)
	}

	major, minor, patch, depth, err := semver.ParsePartial(version)
	if err != nil {
		return nil, nil
	}

	if depth == 3 {
		return readNumscriptExact(contentAttr, reader, ledgerName, name, fmt.Sprintf("%d.%d.%d", major, minor, patch))
	}

	return resolvePartialVersion(contentAttr, reader, ledgerName, name, major, minor, depth)
}

// ReadAllNumscripts lists all numscripts for a ledger by scanning the latest
// pointers, then fetching each script's greatest version content.
func ReadAllNumscripts(
	versionAttr *attributes.Attribute[*commonpb.NumscriptVersionValue],
	contentAttr *attributes.Attribute[*commonpb.NumscriptInfo],
	reader dal.PebbleReader,
	ledgerName string,
) ([]*commonpb.NumscriptInfo, error) {
	// Scan all latest pointers for this ledger.
	// The canonical key prefix is [ledgerName padded 64B].
	prefix := make([]byte, dal.LedgerNameFixedSize)
	copy(prefix, ledgerName)
	entries, err := versionAttr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning numscript versions for ledger %q: %w", ledgerName, err)
	}

	var scripts []*commonpb.NumscriptInfo

	for _, entry := range entries {
		version := entry.Value.GetVersion()
		if version == "" {
			continue
		}

		// Reconstruct the name from the canonical key: [ledgerName padded 64B][name].
		name := string(entry.CanonicalKey[len(prefix):])

		info, err := readNumscriptExact(contentAttr, reader, ledgerName, name, version)
		if err != nil {
			return nil, err
		}

		if info != nil {
			scripts = append(scripts, info)
		}
	}

	return scripts, nil
}

// ReadAllNumscriptVersions returns the numscript's history: its current latest
// (greatest stored semver) and every stored version ordered highest-first.
func ReadAllNumscriptVersions(
	versionAttr *attributes.Attribute[*commonpb.NumscriptVersionValue],
	contentAttr *attributes.Attribute[*commonpb.NumscriptInfo],
	reader dal.PebbleReader,
	ledgerName string, name string,
) (string, []*commonpb.NumscriptVersionEntry, error) {
	latest, err := ReadNumscriptLatestVersion(versionAttr, reader, ledgerName, name)
	if err != nil {
		return "", nil, err
	}

	prefix := domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: ""}.Bytes()
	entries, err := contentAttr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return "", nil, fmt.Errorf("scanning numscript versions for %q/%q: %w", ledgerName, name, err)
	}

	versions := make([]*commonpb.NumscriptVersionEntry, 0, len(entries))
	for _, entry := range entries {
		info := entry.Value
		versions = append(versions, &commonpb.NumscriptVersionEntry{
			Version:   info.GetVersion(),
			CreatedAt: info.GetCreatedAt(),
		})
	}

	sortNumscriptVersions(versions)

	return latest, versions, nil
}

// sortNumscriptVersions orders stored versions highest-first by semver, with any
// unparseable version ordered last (lexically) for a stable, total order.
func sortNumscriptVersions(versions []*commonpb.NumscriptVersionEntry) {
	sort.SliceStable(versions, func(i, j int) bool {
		vi, ei := semver.Parse(versions[i].GetVersion())
		vj, ej := semver.Parse(versions[j].GetVersion())
		if ei != nil || ej != nil {
			return versions[i].GetVersion() > versions[j].GetVersion()
		}

		return vi.Compare(vj) > 0
	})
}

// readNumscriptExact does a direct Get on the exact version key in the attributes zone.
func readNumscriptExact(attr *attributes.Attribute[*commonpb.NumscriptInfo], reader dal.PebbleGetter, ledgerName string, name, version string) (*commonpb.NumscriptInfo, error) {
	return attr.Get(reader, domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: version}.Bytes())
}

// resolvePartialVersion scans all versions for (ledger, name) from the attributes zone
// and finds the highest matching semver.
func resolvePartialVersion(attr *attributes.Attribute[*commonpb.NumscriptInfo], reader dal.PebbleReader, ledgerName string, name string, targetMajor, targetMinor uint32, depth int) (*commonpb.NumscriptInfo, error) {
	// Scan all versions for this (ledger, name) by using the common prefix.
	prefix := domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: ""}.Bytes()

	entries, err := attr.ComputeAllForPrefix(reader, prefix)
	if err != nil {
		return nil, fmt.Errorf("scanning numscript versions for %q/%q: %w", ledgerName, name, err)
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

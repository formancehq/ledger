package query

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/semver"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadNumscriptLatestVersion reads the latest version string for a numscript by name.
// Returns "" if the numscript does not exist.
func ReadNumscriptLatestVersion(reader dal.PebbleReader, name string) (string, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixNumscriptLatest).PutString(name)

	value, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("reading numscript latest version for %q: %w", name, err)
	}
	defer func() { _ = closer.Close() }()

	return string(value), nil
}

// ReadNumscript reads a numscript by name and version spec.
//   - ""          → read latest pointer, then fetch that version
//   - "latest"    → direct Get on the latest slot
//   - "1.0.0"     → direct Get on exact semver
//   - "1.0"       → range scan [1.0.0, 1.1.0), iter.Last()
//   - "1"         → range scan [1.0.0, 2.0.0), iter.Last()
//
// Returns nil if the numscript or version does not exist.
func ReadNumscript(reader dal.PebbleReader, name string, version string) (*commonpb.NumscriptInfo, error) {
	if version == "" {
		latestVersion, err := ReadNumscriptLatestVersion(reader, name)
		if err != nil {
			return nil, err
		}
		if latestVersion == "" {
			return nil, nil
		}
		return ReadNumscript(reader, name, latestVersion)
	}

	if version == "latest" {
		return readNumscriptLatestSlot(reader, name)
	}

	major, minor, patch, depth, err := semver.ParsePartial(version)
	if err != nil {
		// Invalid version string can never match a stored entry
		return nil, nil
	}

	if depth == 3 {
		return readNumscriptExactSemver(reader, name, major, minor, patch)
	}

	return resolvePartialVersion(reader, name, major, minor, depth)
}

// readNumscriptLatestSlot does a direct Get on the "latest" slot key.
func readNumscriptLatestSlot(reader dal.PebbleReader, name string) (*commonpb.NumscriptInfo, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixNumscript).
		PutString(name).
		PutByte(0x00).
		PutByte(domain.NumscriptVersionTagLatest)

	return readNumscriptFromKey(reader, kb.Build(), name, "latest")
}

// readNumscriptExactSemver does a direct Get on the exact semver key.
func readNumscriptExactSemver(reader dal.PebbleReader, name string, major, minor, patch uint32) (*commonpb.NumscriptInfo, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixNumscript).
		PutString(name).
		PutByte(0x00).
		PutByte(domain.NumscriptVersionTagSemver).
		PutUInt32(major).
		PutUInt32(minor).
		PutUInt32(patch)

	return readNumscriptFromKey(reader, kb.Build(), name, fmt.Sprintf("%d.%d.%d", major, minor, patch))
}

// readNumscriptFromKey reads and unmarshals a NumscriptInfo from the given Pebble key.
func readNumscriptFromKey(reader dal.PebbleReader, key []byte, name, version string) (*commonpb.NumscriptInfo, error) {
	value, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading numscript %q v%s: %w", name, version, err)
	}
	defer func() { _ = closer.Close() }()

	info := &commonpb.NumscriptInfo{}
	if err := proto.Unmarshal(value, info); err != nil {
		return nil, fmt.Errorf("unmarshaling numscript %q v%s: %w", name, version, err)
	}
	return info, nil
}

// resolvePartialVersion performs a range scan to find the highest matching semver.
// depth=1: scan [major.0.0, major+1.0.0)
// depth=2: scan [major.minor.0, major.minor+1.0)
func resolvePartialVersion(reader dal.PebbleReader, name string, major, minor uint32, depth int) (*commonpb.NumscriptInfo, error) {
	kb := dal.NewKeyBuilder()

	// Build lower bound: [prefix][name]\x00\x00[major][minor][patch=0]
	kb.PutByte(dal.KeyPrefixNumscript).
		PutString(name).
		PutByte(0x00).
		PutByte(domain.NumscriptVersionTagSemver).
		PutUInt32(major).
		PutUInt32(minor).
		PutUInt32(0)
	lowerBound := kb.Build()

	// Build upper bound depending on depth
	kb.PutByte(dal.KeyPrefixNumscript).
		PutString(name).
		PutByte(0x00).
		PutByte(domain.NumscriptVersionTagSemver)

	switch depth {
	case 1:
		// Scan [major.0.0, (major+1).0.0)
		kb.PutUInt32(major + 1).PutUInt32(0).PutUInt32(0)
	case 2:
		// Scan [major.minor.0, major.(minor+1).0)
		kb.PutUInt32(major).PutUInt32(minor + 1).PutUInt32(0)
	}
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for partial version resolution: %w", err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return nil, nil
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading partial version value: %w", err)
	}

	info := &commonpb.NumscriptInfo{}
	if err := proto.Unmarshal(value, info); err != nil {
		return nil, fmt.Errorf("unmarshaling partial version result: %w", err)
	}
	return info, nil
}

// ReadAllNumscripts reads all numscripts (latest version of each) from the given reader.
func ReadAllNumscripts(reader dal.PebbleReader) ([]*commonpb.NumscriptInfo, error) {
	lowerBound := []byte{dal.KeyPrefixNumscriptLatest}
	upperBound := []byte{dal.KeyPrefixNumscriptLatest + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for numscript latest versions: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var scripts []*commonpb.NumscriptInfo
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		name := string(key[1:]) // skip prefix byte

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading numscript latest version value: %w", err)
		}
		version := string(value)
		if version == "" {
			continue
		}

		info, err := ReadNumscript(reader, name, version)
		if err != nil {
			return nil, fmt.Errorf("reading numscript %q v%s: %w", name, version, err)
		}
		if info != nil {
			scripts = append(scripts, info)
		}
	}

	return scripts, nil
}

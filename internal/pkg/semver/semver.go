package semver

import (
	"fmt"
	"strconv"
	"strings"
)

// Version represents a parsed semantic version with major, minor, and patch components.
type Version struct {
	Major uint32
	Minor uint32
	Patch uint32
}

// String returns the semver as "major.minor.patch".
func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Parse parses a strict "major.minor.patch" version string.
func Parse(s string) (Version, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid semver %q: expected major.minor.patch", s)
	}

	var v Version

	for i, part := range parts {
		if part == "" {
			return Version{}, fmt.Errorf("invalid semver %q: empty component", s)
		}

		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return Version{}, fmt.Errorf("invalid semver %q: %w", s, err)
		}

		switch i {
		case 0:
			v.Major = uint32(n)
		case 1:
			v.Minor = uint32(n)
		case 2:
			v.Patch = uint32(n)
		}
	}

	return v, nil
}

// ParsePartial parses a version string with 1, 2, or 3 parts.
// Returns the parsed components and depth (1, 2, or 3).
func ParsePartial(s string) (major, minor, patch uint32, depth int, err error) {
	parts := strings.Split(s, ".")
	if len(parts) < 1 || len(parts) > 3 {
		return 0, 0, 0, 0, fmt.Errorf("invalid partial semver %q: expected 1-3 parts", s)
	}

	depth = len(parts)
	for i, part := range parts {
		if part == "" {
			return 0, 0, 0, 0, fmt.Errorf("invalid partial semver %q: empty component", s)
		}

		n, parseErr := strconv.ParseUint(part, 10, 32)
		if parseErr != nil {
			return 0, 0, 0, 0, fmt.Errorf("invalid partial semver %q: %w", s, parseErr)
		}

		switch i {
		case 0:
			major = uint32(n)
		case 1:
			minor = uint32(n)
		case 2:
			patch = uint32(n)
		}
	}

	return major, minor, patch, depth, nil
}

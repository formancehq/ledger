package commonpb

import (
	"fmt"
	"strings"
)

// Persistence string names. These are the canonical camelCase-context values
// used across the HTTP API, the CLI, and the OpenAPI spec. They are uppercase
// because they name an enum value, not a JSON property.
const (
	PersistenceNormal    = "NORMAL"
	PersistenceEphemeral = "EPHEMERAL"
	PersistenceTransient = "TRANSIENT"
)

// SegmentType constraint discriminator names, used as the "type" field of the
// JSON SegmentType representation.
const (
	SegmentTypeRegex  = "regex"
	SegmentTypeUUID   = "uuid"
	SegmentTypeUint64 = "uint64"
	SegmentTypeBytes  = "bytes"
)

// ParsePersistence converts a persistence string to an AccountTypePersistence
// enum. It accepts the canonical uppercase names as well as their lowercase
// variants; an empty string defaults to NORMAL.
func ParsePersistence(s string) (AccountTypePersistence, error) {
	switch strings.ToLower(s) {
	case "", "normal":
		return AccountTypePersistence_ACCOUNT_TYPE_NORMAL, nil
	case "ephemeral":
		return AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, nil
	case "transient":
		return AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT, nil
	default:
		return 0, fmt.Errorf("unknown persistence mode %q (valid: NORMAL, EPHEMERAL, TRANSIENT)", s)
	}
}

// PersistenceToString returns the canonical uppercase name for a persistence
// enum. Unknown values fall back to NORMAL, matching the proto zero value.
func PersistenceToString(p AccountTypePersistence) string {
	switch p {
	case AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL:
		return PersistenceEphemeral
	case AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT:
		return PersistenceTransient
	default:
		return PersistenceNormal
	}
}

// SegmentTypeJSON is the camelCase JSON representation of a SegmentType. The
// `type` field discriminates the constraint; only the `regex` type carries a
// `regex` value, the others (uuid, uint64, bytes) are pure markers.
type SegmentTypeJSON struct {
	Type  string `json:"type"`
	Regex string `json:"regex,omitempty"`
}

// SegmentTypeToJSON converts a proto SegmentType to its JSON representation.
// Returns nil when the segment type carries no constraint.
func SegmentTypeToJSON(st *SegmentType) *SegmentTypeJSON {
	if st == nil {
		return nil
	}

	switch c := st.GetConstraint().(type) {
	case *SegmentType_Regex:
		return &SegmentTypeJSON{Type: SegmentTypeRegex, Regex: c.Regex}
	case *SegmentType_Uuid:
		return &SegmentTypeJSON{Type: SegmentTypeUUID}
	case *SegmentType_Uint64:
		return &SegmentTypeJSON{Type: SegmentTypeUint64}
	case *SegmentType_Bytes:
		return &SegmentTypeJSON{Type: SegmentTypeBytes}
	default:
		return nil
	}
}

// SegmentTypeFromJSON converts a JSON SegmentType to its proto representation.
func SegmentTypeFromJSON(j *SegmentTypeJSON) (*SegmentType, error) {
	if j == nil {
		return nil, nil
	}

	switch strings.ToLower(j.Type) {
	case SegmentTypeRegex:
		if j.Regex == "" {
			return nil, fmt.Errorf("segment type %q requires a non-empty regex", SegmentTypeRegex)
		}

		return &SegmentType{Constraint: &SegmentType_Regex{Regex: j.Regex}}, nil
	case SegmentTypeUUID:
		return &SegmentType{Constraint: &SegmentType_Uuid{Uuid: &UUIDConstraint{}}}, nil
	case SegmentTypeUint64:
		return &SegmentType{Constraint: &SegmentType_Uint64{Uint64: &Uint64Constraint{}}}, nil
	case SegmentTypeBytes:
		return &SegmentType{Constraint: &SegmentType_Bytes{Bytes: &BytesConstraint{}}}, nil
	default:
		return nil, fmt.Errorf("unknown segment type %q (valid: %s, %s, %s, %s)",
			j.Type, SegmentTypeRegex, SegmentTypeUUID, SegmentTypeUint64, SegmentTypeBytes)
	}
}

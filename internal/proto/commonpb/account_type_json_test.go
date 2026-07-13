package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePersistence(t *testing.T) {
	t.Parallel()

	cases := map[string]AccountTypePersistence{
		"":          AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		"normal":    AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		"NORMAL":    AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		"ephemeral": AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
		"EPHEMERAL": AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
		"transient": AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
		"TRANSIENT": AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
	}

	for in, want := range cases {
		got, err := ParsePersistence(in)
		require.NoError(t, err, "input %q", in)
		require.Equal(t, want, got, "input %q", in)
	}

	_, err := ParsePersistence("bogus")
	require.Error(t, err)
}

func TestPersistenceToStringRoundTrip(t *testing.T) {
	t.Parallel()

	for _, p := range []AccountTypePersistence{
		AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
		AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
	} {
		got, err := ParsePersistence(PersistenceToString(p))
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
}

func TestSegmentTypeJSONRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []*SegmentType{
		{Constraint: &SegmentType_Regex{Regex: "[a-z]+"}},
		{Constraint: &SegmentType_Uuid{Uuid: &UUIDConstraint{}}},
		{Constraint: &SegmentType_Uint64{Uint64: &Uint64Constraint{}}},
		{Constraint: &SegmentType_Bytes{Bytes: &BytesConstraint{}}},
	}

	for _, st := range cases {
		j := SegmentTypeToJSON(st)
		require.NotNil(t, j)

		back, err := SegmentTypeFromJSON(j)
		require.NoError(t, err)
		require.IsType(t, st.GetConstraint(), back.GetConstraint())
	}
}

func TestSegmentTypeFromJSON_Errors(t *testing.T) {
	t.Parallel()

	_, err := SegmentTypeFromJSON(&SegmentTypeJSON{Type: "regex"})
	require.Error(t, err, "regex without value must fail")

	_, err = SegmentTypeFromJSON(&SegmentTypeJSON{Type: "unknown"})
	require.Error(t, err)

	got, err := SegmentTypeFromJSON(nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

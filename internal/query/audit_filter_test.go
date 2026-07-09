package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// fakeAuditIndex is a hand-configured AuditIndexReader for compiler tests.
type fakeAuditIndex struct {
	byString  map[string][]uint64 // key: string(field)+value
	byOutcome map[bool][]uint64
	byRange   func(field byte, lo, hi uint64) []uint64
}

func (f *fakeAuditIndex) AuditSeqsByString(field byte, value string) ([]uint64, error) {
	return f.byString[string(field)+value], nil
}

func (f *fakeAuditIndex) AuditSeqsByOutcome(success bool) ([]uint64, error) {
	return f.byOutcome[success], nil
}

func (f *fakeAuditIndex) AuditSeqsByUint64Range(field byte, lo, hi uint64) ([]uint64, error) {
	if f.byRange == nil {
		return nil, nil
	}

	return f.byRange(field, lo, hi), nil
}

func auditString(field commonpb.AuditField, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Audit{
			Audit: &commonpb.AuditCondition{
				Field: field,
				Condition: &commonpb.AuditCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
					},
				},
			},
		},
	}
}

func auditUint(field commonpb.AuditField, lo, hi *uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Audit{
			Audit: &commonpb.AuditCondition{
				Field: field,
				Condition: &commonpb.AuditCondition_UintCond{
					UintCond: &commonpb.UintCondition{Min: lo, Max: hi},
				},
			},
		},
	}
}

func TestCompileAuditFilter_Nil(t *testing.T) {
	t.Parallel()

	seqs, lo, hi, narrowed, err := CompileAuditFilter(&fakeAuditIndex{}, nil)
	require.NoError(t, err)
	require.False(t, narrowed)
	require.Nil(t, seqs)
	require.Equal(t, uint64(0), lo)
	require.Equal(t, uint64(math.MaxUint64), hi)
}

func TestCompileAuditFilter_Outcome(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{byOutcome: map[bool][]uint64{false: {3, 7}, true: {1, 2}}}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, auditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"))
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{3, 7}, seqs)
}

func TestCompileAuditFilter_OutcomeInvalidValue(t *testing.T) {
	t.Parallel()

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, auditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "maybe"))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_StringField(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{byString: map[string][]uint64{
		string(readstore.AuditFieldLedger) + "main": {5, 9},
	}}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, auditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "main"))
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{5, 9}, seqs)
}

func TestCompileAuditFilter_And_Intersects(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{
		byOutcome: map[bool][]uint64{false: {1, 2, 3, 4}},
		byString: map[string][]uint64{
			string(readstore.AuditFieldLedger) + "main": {2, 4, 6},
		},
	}

	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
				auditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "main"),
			}},
		},
	}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{2, 4}, seqs)
}

func TestCompileAuditFilter_Or_Unions(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{byString: map[string][]uint64{
		string(readstore.AuditFieldOrderType) + "create_transaction": {1, 3},
		string(readstore.AuditFieldOrderType) + "revert_transaction": {3, 5},
	}}

	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
				auditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"),
				auditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "revert_transaction"),
			}},
		},
	}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{1, 3, 5}, seqs)
}

func TestCompileAuditFilter_SeqRange_BoundsOnly(t *testing.T) {
	t.Parallel()

	// audit[seq] between 10 and 20 -> zone bounds, not index-narrowed.
	seqs, lo, hi, narrowed, err := CompileAuditFilter(&fakeAuditIndex{},
		auditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(10)), new(uint64(20))))
	require.NoError(t, err)
	require.False(t, narrowed)
	require.Nil(t, seqs)
	require.Equal(t, uint64(10), lo)
	require.Equal(t, uint64(20), hi)
}

func TestCompileAuditFilter_SeqRange_AndWithIndex(t *testing.T) {
	t.Parallel()

	// audit[outcome]==failure and audit[seq] >= 3 -> index set narrowed to {3,7},
	// bounded to seq>=3.
	idx := &fakeAuditIndex{byOutcome: map[bool][]uint64{false: {1, 3, 7}}}

	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
				auditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(3)), nil),
			}},
		},
	}

	seqs, lo, hi, narrowed, err := CompileAuditFilter(idx, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{1, 3, 7}, seqs)
	require.Equal(t, uint64(3), lo)
	require.Equal(t, uint64(math.MaxUint64), hi)
}

func TestCompileAuditFilter_UintRange(t *testing.T) {
	t.Parallel()

	var gotField byte
	var gotLo, gotHi uint64
	idx := &fakeAuditIndex{byRange: func(field byte, lo, hi uint64) []uint64 {
		gotField, gotLo, gotHi = field, lo, hi

		return []uint64{11, 12}
	}}

	// proposal_id between 100 and 200 (inclusive).
	seqs, _, _, narrowed, err := CompileAuditFilter(idx,
		auditUint(commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, new(uint64(100)), new(uint64(200))))
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{11, 12}, seqs)
	require.Equal(t, readstore.AuditFieldProposalID, gotField)
	require.Equal(t, uint64(100), gotLo)
	require.Equal(t, uint64(200), gotHi)
}

func TestCompileAuditFilter_RejectsNot(t *testing.T) {
	t.Parallel()

	notFilter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Not{
			Not: &commonpb.NotFilter{Filter: auditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, notFilter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_RejectsNonAuditCondition(t *testing.T) {
	t.Parallel()

	metaFilter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: "k"},
			},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, metaFilter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_RejectsSeqInsideOr(t *testing.T) {
	t.Parallel()

	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
				auditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(3)), nil),
			}},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, filter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_StringParamRejected(t *testing.T) {
	t.Parallel()

	paramFilter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Audit{
			Audit: &commonpb.AuditCondition{
				Field: commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT,
				Condition: &commonpb.AuditCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Param{Param: "p"},
					},
				},
			},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, paramFilter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestIntersectSorted(t *testing.T) {
	t.Parallel()

	require.Equal(t, []uint64{2, 4}, intersectSorted([]uint64{1, 2, 3, 4}, []uint64{2, 4, 6}))
	require.Empty(t, intersectSorted([]uint64{1, 3}, []uint64{2, 4}))
	require.Empty(t, intersectSorted(nil, []uint64{1}))
}

func TestUnionSorted(t *testing.T) {
	t.Parallel()

	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, unionSorted([]uint64{1, 3, 5}, []uint64{2, 4, 6}))
	require.Equal(t, []uint64{1, 2, 3}, unionSorted([]uint64{1, 2, 3}, []uint64{2}))
	require.Equal(t, []uint64{1}, unionSorted(nil, []uint64{1}))
}

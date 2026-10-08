package query

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// fakeAuditIndex is a hand-configured AuditIndexReader for compiler tests.
type fakeAuditIndex struct {
	byString       map[string][]uint64 // key: string(field)+value
	byStringPrefix map[string][]uint64 // key: string(field)+value
	stringErr      error
	prefixErr      error
	byOutcome      map[bool][]uint64
	byRange        func(field byte, lo, hi uint64) []uint64
}

func (f *fakeAuditIndex) AuditSeqsByStringPrefix(field byte, value string) ([]uint64, error) {
	if f.prefixErr != nil {
		return nil, f.prefixErr
	}

	return f.byStringPrefix[string(field)+value], nil
}

func (f *fakeAuditIndex) AuditSeqsByString(field byte, value string) ([]uint64, error) {
	if f.stringErr != nil {
		return nil, f.stringErr
	}

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

func auditString(field ledgerpb.AuditField, value string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Audit{
			Audit: &ledgerpb.AuditCondition{
				Field: field,
				Condition: &ledgerpb.AuditCondition_StringCond{
					StringCond: &ledgerpb.StringCondition{
						Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: value},
					},
				},
			},
		},
	}
}

func auditUint(field ledgerpb.AuditField, lo, hi *uint64) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Audit{
			Audit: &ledgerpb.AuditCondition{
				Field: field,
				Condition: &ledgerpb.AuditCondition_UintCond{
					UintCond: &ledgerpb.UintCondition{Min: lo, Max: hi},
				},
			},
		},
	}
}

func auditStringPrefix(field ledgerpb.AuditField, value string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Audit{Audit: &ledgerpb.AuditCondition{
		Field: field, Condition: &ledgerpb.AuditCondition_StringPrefix{StringPrefix: value},
	}}}
}

func TestCompileAuditFilter_IdempotencyKeyEqualityAndPrefix(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{
		byString: map[string][]uint64{
			string(readstore.AuditFieldIdempotencyKey) + "retry-1": {3, 9},
		},
		byStringPrefix: map[string][]uint64{
			string(readstore.AuditFieldIdempotencyKey) + "retry-": {3, 7, 9},
		},
	}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx,
		auditString(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, "retry-1"))
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{3, 9}, seqs, "an expired then reused key keeps every historical sequence")

	seqs, _, _, narrowed, err = CompileAuditFilter(idx,
		auditStringPrefix(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, "retry-"))
	require.NoError(t, err)
	require.True(t, narrowed, "prefix lookup must return index candidates, never request a global audit scan")
	require.Equal(t, []uint64{3, 7, 9}, seqs)
	require.NoError(t, ValidateAuditFilter(
		auditStringPrefix(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, "retry-")))
}

func TestCompileAuditFilter_IdempotencyKeyValidationAndLookupErrors(t *testing.T) {
	t.Parallel()

	param := &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Audit{Audit: &ledgerpb.AuditCondition{
		Field: ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
		Condition: &ledgerpb.AuditCondition_StringCond{StringCond: &ledgerpb.StringCondition{
			Value: &ledgerpb.StringCondition_Param{Param: "key"},
		}},
	}}}
	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, param)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	wrongType := auditUint(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, new(uint64(1)), nil)
	_, _, _, _, err = CompileAuditFilter(&fakeAuditIndex{}, wrongType)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	lookupErr := errors.New("lookup failed")
	_, _, _, _, err = CompileAuditFilter(&fakeAuditIndex{stringErr: lookupErr},
		auditString(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, "retry-1"))
	require.ErrorIs(t, err, lookupErr)

	_, _, _, _, err = CompileAuditFilter(&fakeAuditIndex{prefixErr: lookupErr},
		auditStringPrefix(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, "retry-"))
	require.ErrorIs(t, err, lookupErr)
}

func TestCompileAuditFilter_IdempotencyKeyNULPrefixIsInvalidArgument(t *testing.T) {
	t.Parallel()

	// A NUL-bearing prefix must be rejected at the compiler level as
	// codes.InvalidArgument so ValidateAuditFilter can catch it and the gRPC
	// error code is consistent with every other client-facing rejection.
	nulPrefix := auditStringPrefix(ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, "key\x00")
	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, nulPrefix)
	require.Equal(t, codes.InvalidArgument, status.Code(err),
		"NUL prefix must be rejected as gRPC InvalidArgument")
	require.ErrorContains(t, err, "NUL")

	// ValidateAuditFilter must also reject it (it uses auditValidationIndex{}).
	err = ValidateAuditFilter(nulPrefix)
	require.Equal(t, codes.InvalidArgument, status.Code(err),
		"ValidateAuditFilter must surface NUL rejection as InvalidArgument")
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

func TestAuditFilterNeedsIndex(t *testing.T) {
	t.Parallel()

	seqLower := auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(5)), nil)
	seqUpper := auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, nil, new(uint64(20)))
	outcome := auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")

	seqAnd := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
			seqLower,
			seqUpper,
		}}},
	}
	mixedAnd := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
			seqLower,
			outcome,
		}}},
	}
	mixedOr := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Or{Or: &ledgerpb.OrFilter{Filters: []*ledgerpb.QueryFilter{
			seqLower,
			outcome,
		}}},
	}
	indexedNot := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Not{Not: &ledgerpb.NotFilter{Filter: outcome}},
	}

	tooDeep := seqLower
	for range MaxFilterDepth {
		tooDeep = &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_And{And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{tooDeep}}},
		}
	}

	tests := []struct {
		name  string
		input *ledgerpb.QueryFilter
		want  bool
	}{
		{name: "nil", input: nil, want: false},
		{name: "sequence bound", input: seqLower, want: false},
		{name: "and of sequence bounds", input: seqAnd, want: false},
		{name: "empty or", input: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Or{Or: &ledgerpb.OrFilter{}}}, want: false},
		{name: "indexed field", input: outcome, want: true},
		{name: "sequence and indexed field", input: mixedAnd, want: true},
		{name: "sequence or indexed field", input: mixedOr, want: true},
		{name: "not indexed field", input: indexedNot, want: true},
		{name: "malformed sequence condition", input: auditString(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, "bad"), want: true},
		{name: "missing filter arm", input: &ledgerpb.QueryFilter{}, want: true},
		{name: "over depth", input: tooDeep, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, AuditFilterNeedsIndex(tt.input))
		})
	}
}

func TestValidateAuditFilterExercisesIndexBackedGrammarWithoutIndexReads(t *testing.T) {
	t.Parallel()

	minimum := uint64(3)
	for name, filter := range map[string]*ledgerpb.QueryFilter{
		"string":  auditString(ledgerpb.AuditField_AUDIT_FIELD_LEDGER, "main"),
		"outcome": auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "success"),
		"numeric": auditUint(ledgerpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, &minimum, nil),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, ValidateAuditFilter(filter))
		})
	}

	require.Error(t, ValidateAuditFilter(&ledgerpb.QueryFilter{}))
}

func TestCompileAuditFilter_Outcome(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{byOutcome: map[bool][]uint64{false: {3, 7}, true: {1, 2}}}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"))
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{3, 7}, seqs)
}

func TestCompileAuditFilter_OutcomeInvalidValue(t *testing.T) {
	t.Parallel()

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "maybe"))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_StringField(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{byString: map[string][]uint64{
		string(readstore.AuditFieldLedger) + "main": {5, 9},
	}}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, auditString(ledgerpb.AuditField_AUDIT_FIELD_LEDGER, "main"))
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

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{
			And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
				auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditString(ledgerpb.AuditField_AUDIT_FIELD_LEDGER, "main"),
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

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Or{
			Or: &ledgerpb.OrFilter{Filters: []*ledgerpb.QueryFilter{
				auditString(ledgerpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"),
				auditString(ledgerpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "revert_transaction"),
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

	// seq between 10 and 20 -> zone bounds, not index-narrowed.
	seqs, lo, hi, narrowed, err := CompileAuditFilter(&fakeAuditIndex{},
		auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(10)), new(uint64(20))))
	require.NoError(t, err)
	require.False(t, narrowed)
	require.Nil(t, seqs)
	require.Equal(t, uint64(10), lo)
	require.Equal(t, uint64(20), hi)
}

func TestCompileAuditFilter_SeqRange_AndWithIndex(t *testing.T) {
	t.Parallel()

	// outcome==failure and seq >= 3 -> failures {1,3,7} filtered
	// to seq>=3 = {3,7}, with the bound baked into the seq set (window reset to
	// full) so an enclosing OR cannot lose it.
	idx := &fakeAuditIndex{byOutcome: map[bool][]uint64{false: {1, 3, 7}}}

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{
			And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
				auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(3)), nil),
			}},
		},
	}

	seqs, lo, hi, narrowed, err := CompileAuditFilter(idx, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{3, 7}, seqs)
	require.Equal(t, uint64(0), lo)
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
		auditUint(ledgerpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, new(uint64(100)), new(uint64(200))))
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{11, 12}, seqs)
	require.Equal(t, readstore.AuditFieldProposalID, gotField)
	require.Equal(t, uint64(100), gotLo)
	require.Equal(t, uint64(200), gotHi)
}

func TestCompileAuditFilter_RejectsNot(t *testing.T) {
	t.Parallel()

	notFilter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Not{
			Not: &ledgerpb.NotFilter{Filter: auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, notFilter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_RejectsNonAuditCondition(t *testing.T) {
	t.Parallel()

	metaFilter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: "k"},
			},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, metaFilter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_RejectsSeqInsideOr(t *testing.T) {
	t.Parallel()

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Or{
			Or: &ledgerpb.OrFilter{Filters: []*ledgerpb.QueryFilter{
				auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(3)), nil),
			}},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, filter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_StringParamRejected(t *testing.T) {
	t.Parallel()

	paramFilter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Audit{
			Audit: &ledgerpb.AuditCondition{
				Field: ledgerpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT,
				Condition: &ledgerpb.AuditCondition_StringCond{
					StringCond: &ledgerpb.StringCondition{
						Value: &ledgerpb.StringCondition_Param{Param: "p"},
					},
				},
			},
		},
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{}, paramFilter)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_UnspecifiedFieldRejected(t *testing.T) {
	t.Parallel()

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{},
		auditString(ledgerpb.AuditField_AUDIT_FIELD_UNSPECIFIED, "x"))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_StringFieldWrongConditionType(t *testing.T) {
	t.Parallel()

	// A string field (ledger) given a uint condition must be rejected.
	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{},
		auditUint(ledgerpb.AuditField_AUDIT_FIELD_LEDGER, new(uint64(1)), new(uint64(2))))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_UintFieldWrongConditionType(t *testing.T) {
	t.Parallel()

	// A uint field (proposal_id) given a string condition must be rejected.
	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{},
		auditString(ledgerpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, "x"))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_OutcomeWrongConditionType(t *testing.T) {
	t.Parallel()

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{},
		auditUint(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, new(uint64(1)), new(uint64(1))))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_SeqWrongConditionType(t *testing.T) {
	t.Parallel()

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{},
		auditString(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, "x"))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_TimestampAndLogSeqDispatch(t *testing.T) {
	t.Parallel()

	var gotFields []byte
	idx := &fakeAuditIndex{byRange: func(field byte, _, _ uint64) []uint64 {
		gotFields = append(gotFields, field)

		return []uint64{1}
	}}

	_, _, _, _, err := CompileAuditFilter(idx, auditUint(ledgerpb.AuditField_AUDIT_FIELD_TIMESTAMP, new(uint64(10)), nil))
	require.NoError(t, err)

	_, _, _, _, err = CompileAuditFilter(idx, auditUint(ledgerpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, nil, new(uint64(20))))
	require.NoError(t, err)

	require.Equal(t, []byte{readstore.AuditFieldTimestamp, readstore.AuditFieldLogSeq}, gotFields)
}

func TestCompileAuditFilter_CallerSubjectAndOrderTypeDispatch(t *testing.T) {
	t.Parallel()

	idx := &fakeAuditIndex{byString: map[string][]uint64{
		string(readstore.AuditFieldCallerSubject) + "alice":          {2},
		string(readstore.AuditFieldOrderType) + "create_transaction": {3},
	}}

	seqs, _, _, _, err := CompileAuditFilter(idx, auditString(ledgerpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, "alice"))
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, seqs)

	seqs, _, _, _, err = CompileAuditFilter(idx, auditString(ledgerpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"))
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, seqs)
}

func TestCompileAuditFilter_EmptyUintRangeMatchesNothing(t *testing.T) {
	t.Parallel()

	// proposal_id > MaxUint64 is unsatisfiable -> narrowed with empty set,
	// the index is never consulted.
	consulted := false
	idx := &fakeAuditIndex{byRange: func(_ byte, _, _ uint64) []uint64 {
		consulted = true

		return []uint64{1}
	}}

	cond := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Audit{
			Audit: &ledgerpb.AuditCondition{
				Field: ledgerpb.AuditField_AUDIT_FIELD_PROPOSAL_ID,
				Condition: &ledgerpb.AuditCondition_UintCond{
					UintCond: &ledgerpb.UintCondition{Min: new(uint64(math.MaxUint64)), MinExclusive: true},
				},
			},
		},
	}

	seqs, _, _, narrowed, err := CompileAuditFilter(idx, cond)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Empty(t, seqs)
	require.False(t, consulted, "unsatisfiable range must not hit the index")
}

func TestCompileAuditFilter_EmptyAndIsUnconstrained(t *testing.T) {
	t.Parallel()

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{And: &ledgerpb.AndFilter{}},
	}

	seqs, lo, hi, narrowed, err := CompileAuditFilter(&fakeAuditIndex{}, filter)
	require.NoError(t, err)
	require.False(t, narrowed)
	require.Nil(t, seqs)
	require.Equal(t, uint64(0), lo)
	require.Equal(t, uint64(math.MaxUint64), hi)
}

func TestCompileAuditFilter_AndOfTwoSeqBounds(t *testing.T) {
	t.Parallel()

	// seq >= 5 and seq <= 20 -> both non-narrowed, bounds intersect
	// to [5,20]; the index is never consulted.
	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{
			And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
				auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, new(uint64(5)), nil),
				auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, nil, new(uint64(20))),
			}},
		},
	}

	seqs, lo, hi, narrowed, err := CompileAuditFilter(&fakeAuditIndex{}, filter)
	require.NoError(t, err)
	require.False(t, narrowed)
	require.Nil(t, seqs)
	require.Equal(t, uint64(5), lo)
	require.Equal(t, uint64(20), hi)
}

func TestCompileAuditFilter_OrDoesNotLeakBranchSeqBound(t *testing.T) {
	t.Parallel()

	// (outcome == failure and seq < 10) or ledger == main
	// Failures are seqs {3, 12, 20}; the seq<10 bound must keep only {3} from
	// that branch, then union with ledger==main {50}. Without baking the branch
	// bound, 12 and 20 would leak in.
	idx := &fakeAuditIndex{
		byOutcome: map[bool][]uint64{false: {3, 12, 20}},
		byString: map[string][]uint64{
			string(readstore.AuditFieldLedger) + "main": {50},
		},
	}

	andBranch := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{
			And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
				auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, nil, new(uint64(9))), // seq <= 9
			}},
		},
	}

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Or{
			Or: &ledgerpb.OrFilter{Filters: []*ledgerpb.QueryFilter{
				andBranch,
				auditString(ledgerpb.AuditField_AUDIT_FIELD_LEDGER, "main"),
			}},
		},
	}

	seqs, lo, hi, narrowed, err := CompileAuditFilter(idx, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{3, 50}, seqs, "12 and 20 must be excluded by the branch-local seq bound")
	require.Equal(t, uint64(0), lo)
	require.Equal(t, uint64(math.MaxUint64), hi)
}

func TestCompileAuditFilter_AndBakesSeqBoundIntoSeqs(t *testing.T) {
	t.Parallel()

	// outcome == failure and seq <= 9 -> {3} (bound baked into seqs,
	// window reset to full).
	idx := &fakeAuditIndex{byOutcome: map[bool][]uint64{false: {3, 12, 20}}}

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{
			And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{
				auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"),
				auditUint(ledgerpb.AuditField_AUDIT_FIELD_SEQUENCE, nil, new(uint64(9))),
			}},
		},
	}

	seqs, lo, hi, narrowed, err := CompileAuditFilter(idx, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{3}, seqs)
	require.Equal(t, uint64(0), lo)
	require.Equal(t, uint64(math.MaxUint64), hi)
}

func TestCompileAuditFilter_EmptyOrMatchesNothing(t *testing.T) {
	t.Parallel()

	filter := &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Or{Or: &ledgerpb.OrFilter{}},
	}

	seqs, _, _, narrowed, err := CompileAuditFilter(&fakeAuditIndex{}, filter)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Empty(t, seqs)
}

func TestCompileAuditFilter_RejectsTooDeep(t *testing.T) {
	t.Parallel()

	// Build an and/or tree nested deeper than MaxFilterDepth; the compiler must
	// return InvalidArgument rather than overflow the stack.
	leaf := auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")
	f := leaf
	for range MaxFilterDepth + 5 {
		f = &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_And{And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{f}}},
		}
	}

	_, _, _, _, err := CompileAuditFilter(&fakeAuditIndex{byOutcome: map[bool][]uint64{false: {1}}}, f)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCompileAuditFilter_AcceptsAtDepthLimit(t *testing.T) {
	t.Parallel()

	// A tree just under the limit still compiles.
	leaf := auditString(ledgerpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")
	f := leaf
	for range MaxFilterDepth - 2 {
		f = &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_And{And: &ledgerpb.AndFilter{Filters: []*ledgerpb.QueryFilter{f}}},
		}
	}

	seqs, _, _, narrowed, err := CompileAuditFilter(&fakeAuditIndex{byOutcome: map[bool][]uint64{false: {1}}}, f)
	require.NoError(t, err)
	require.True(t, narrowed)
	require.Equal(t, []uint64{1}, seqs)
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

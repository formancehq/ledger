package balancehistorystore

import (
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	domainhistory "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

func TestCursorOnlyPublicationsKeepManifestRetentionBounded(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, err := store.Publish(Publication{
		Coverage: Coverage{AuditSequence: 1, AuditHash: []byte("1"), SourceComplete: true},
	})
	require.NoError(t, err)
	pinned, err := store.OpenView(0)
	require.NoError(t, err)

	for sequence := uint64(2); sequence <= 100; sequence++ {
		_, err := store.Publish(Publication{
			Coverage: Coverage{
				AuditSequence:  sequence,
				AuditHash:      []byte{byte(sequence)},
				SourceComplete: true,
			},
		})
		require.NoError(t, err)
	}
	require.Equal(t, 2, countPhysicalManifests(t, store), "current plus pinned manifest")
	require.NoError(t, pinned.Close())
	_, err = store.CollectGarbage()
	require.NoError(t, err)
	require.Equal(t, 1, countPhysicalManifests(t, store))
}

func countPhysicalManifests(t *testing.T, store *Store) int {
	t.Helper()

	iter, err := store.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixManifest},
		UpperBound: []byte{prefixManifest + 1},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	count := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())

	return count
}

func TestBuildRunRecordsFromGroupsRejectsOversizedIdentityComponentsDeterministically(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("x", math.MaxUint16+1)
	testCases := []struct {
		name     string
		identity recordIdentity
		wantErr  string
	}{
		{
			name: "account",
			identity: recordIdentity{
				Temporality: TemporalityEffective,
				LedgerName:  "default",
				Account:     oversized,
				AssetBase:   "USD",
			},
			wantErr: "encoding balance history catalog key: history account key component exceeds 65535 bytes",
		},
		{
			name: "asset base",
			identity: recordIdentity{
				Temporality: TemporalityEffective,
				LedgerName:  "default",
				Account:     "assets:cash",
				AssetBase:   oversized,
			},
			wantErr: "encoding balance history catalog key: history key component exceeds 65535 bytes",
		},
		{
			name: "color",
			identity: recordIdentity{
				Temporality: TemporalityEffective,
				LedgerName:  "default",
				Account:     "assets:cash",
				AssetBase:   "USD",
				Color:       oversized,
			},
			wantErr: "encoding balance history catalog key: history key component exceeds 65535 bytes",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			for range 10 {
				assertRunRecordBuildError(t, map[recordIdentity][]timedDelta{
					testCase.identity: nil,
				}, testCase.wantErr)
			}
		})
	}

	t.Run("mixed invalid identities", func(t *testing.T) {
		t.Parallel()

		groups := map[recordIdentity][]timedDelta{
			{
				Temporality: TemporalityEffective,
				LedgerName:  "a",
				Account:     oversized,
				AssetBase:   "USD",
			}: nil,
			{
				Temporality: TemporalityEffective,
				LedgerName:  "b",
				Account:     "assets:cash",
				AssetBase:   oversized,
			}: nil,
			{
				Temporality: TemporalityEffective,
				LedgerName:  "c",
				Account:     "assets:cash",
				AssetBase:   "USD",
				Color:       oversized,
			}: nil,
		}

		for range 100 {
			assertRunRecordBuildError(t, groups, "encoding balance history catalog key: history account key component exceeds 65535 bytes")
		}
	})
}

func assertRunRecordBuildError(t *testing.T, groups map[recordIdentity][]timedDelta, wantErr string) {
	t.Helper()

	records, dataCount, identityCount, err := buildRunRecordsFromGroups(1, groups)
	require.EqualError(t, err, wantErr)
	require.Nil(t, records)
	require.Zero(t, dataCount)
	require.Zero(t, identityCount)
}

func validPublicationEffect() domainhistory.Effect {
	return domainhistory.Effect{
		LedgerName:    "default",
		AuditSequence: 6,
		LogSequence:   6,
		EffectiveAt:   10,
		InsertedAt:    20,
		Account:       "assets:cash",
		AssetBase:     "USD",
		Input:         domainhistory.AmountFromUint64(1),
	}
}

func TestValidateEffectRejectsIncompleteEffects(t *testing.T) {
	t.Parallel()

	valid := validPublicationEffect()
	tests := []struct {
		name   string
		mutate func(*domainhistory.Effect)
		want   string
	}{
		{name: "ledger", mutate: func(effect *domainhistory.Effect) { effect.LedgerName = "" }, want: "ledger name is required"},
		{name: "audit", mutate: func(effect *domainhistory.Effect) { effect.AuditSequence = 0 }, want: "audit sequence is required"},
		{name: "log", mutate: func(effect *domainhistory.Effect) { effect.LogSequence = 0 }, want: "log sequence is required"},
		{name: "account", mutate: func(effect *domainhistory.Effect) { effect.Account = "" }, want: "account is required"},
		{name: "asset", mutate: func(effect *domainhistory.Effect) { effect.AssetBase = "" }, want: "asset base is required"},
		{
			name: "zero mutation",
			mutate: func(effect *domainhistory.Effect) {
				effect.Input = domainhistory.Amount{}
				effect.Output = domainhistory.Amount{}
			},
			want: "effect must change input or output",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			effect := valid
			test.mutate(&effect)
			require.ErrorContains(t, validateEffect(effect), test.want)
		})
	}
	require.NoError(t, validateEffect(valid))
}

func TestValidatePublicationRejectsSourceGaps(t *testing.T) {
	t.Parallel()

	current := Manifest{
		AuditWatermark: 5,
		LogWatermark:   5,
		AuditHash:      []byte("current"),
		SourceComplete: true,
	}
	valid := Publication{
		Coverage: Coverage{
			AuditSequence:  6,
			LogSequence:    6,
			AuditHash:      []byte("next"),
			SourceComplete: true,
		},
		Effects: []domainhistory.Effect{validPublicationEffect()},
	}
	tests := []struct {
		name   string
		mutate func(*Publication)
		want   string
	}{
		{name: "audit moves backward", mutate: func(p *Publication) { p.Coverage.AuditSequence = 4 }, want: "audit watermark moved backward"},
		{name: "log moves backward", mutate: func(p *Publication) { p.Coverage.LogSequence = 4 }, want: "log watermark moved backward"},
		{name: "completeness revoked", mutate: func(p *Publication) { p.Coverage.SourceComplete = false }, want: "source completeness cannot be revoked"},
		{
			name: "effects without audit advance",
			mutate: func(p *Publication) {
				p.Coverage.AuditSequence = 5
				p.Coverage.LogSequence = 5
				p.Coverage.AuditHash = []byte("current")
			},
			want: "effects cannot be appended",
		},
		{
			name: "log advances alone",
			mutate: func(p *Publication) {
				p.Effects = nil
				p.Coverage.AuditSequence = 5
			},
			want: "log coverage cannot advance",
		},
		{
			name: "hash changes alone",
			mutate: func(p *Publication) {
				p.Effects = nil
				p.Coverage.AuditSequence = 5
				p.Coverage.LogSequence = 5
			},
			want: "audit hash changed",
		},
		{
			name: "invalid effect",
			mutate: func(p *Publication) {
				p.Effects[0].Account = ""
			},
			want: "invalid monetary effect 0",
		},
		{
			name: "effect before current",
			mutate: func(p *Publication) {
				p.Effects[0].AuditSequence = 5
			},
			want: "effect audit sequence 5 is outside",
		},
		{
			name: "effect beyond coverage",
			mutate: func(p *Publication) {
				p.Effects[0].AuditSequence = 7
			},
			want: "effect audit sequence 7 is outside",
		},
		{
			name: "effect log beyond coverage",
			mutate: func(p *Publication) {
				p.Effects[0].LogSequence = 7
			},
			want: "effect log sequence 7 exceeds watermark 6",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			publication := valid
			publication.Effects = append([]domainhistory.Effect(nil), valid.Effects...)
			test.mutate(&publication)
			require.ErrorContains(t, validatePublication(current, publication), test.want)
		})
	}
	require.NoError(t, validatePublication(current, valid))
}

func TestPublicationHelpers(t *testing.T) {
	t.Parallel()

	effect := validPublicationEffect()
	effect.AssetPrecision = 2
	effect.Color = "BLUE"
	effectiveVolume := effectIdentity(effect, TemporalityEffective)
	require.Equal(t, "assets:cash", effectiveVolume.Account)
	require.Equal(t, uint64(10), effectTimestamp(effect, TemporalityEffective))
	require.Equal(t, uint64(20), effectTimestamp(effect, TemporalityInsertion))
	insertionVolume := effectIdentity(effect, TemporalityInsertion)
	require.Equal(t, "assets:cash", insertionVolume.Account)

	require.Equal(t, -1, compareUint64(1, 2))
	require.Equal(t, 1, compareUint64(2, 1))
	require.Zero(t, compareUint64(1, 1))
	require.Equal(t, -1, compareUint32(1, 2))
	require.Equal(t, 1, compareUint32(2, 1))
	require.Zero(t, compareUint32(1, 1))
	require.Equal(t, 3, firstNonZero(0, 3, 4))
	require.Zero(t, firstNonZero(0, 0))
}

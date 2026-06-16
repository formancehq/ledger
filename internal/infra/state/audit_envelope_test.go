package state

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestHashChain_Envelope_Golden pins the WHOLE chain spec — both the
// envelope encoding rules (header + per-item) AND the keyed BLAKE3
// formula — by reproducing the hash by hand for a realistic
// AuditEntry+AuditItem fixture. Any drift in:
//   - the field-ordering inside HashedHeaderPayload / PerItemPayload
//   - the length-prefix convention (u32 BE)
//   - the sort order on maps / repeated string slices
//   - the keyed-BLAKE3 derivation from ClusterID
//   - the concatenation order H(header || items... || lastHash)
//
// will trip this test even if every individual piece still looks
// self-consistent. It is the single anchor an external (cross-language)
// verifier compares its implementation against.
//
// If you intentionally bump the envelope spec, change the version of
// the algorithm enum and add a parallel "v2" generator and golden — do
// not edit this fixture, otherwise historical entries become
// unverifiable.
func TestHashChain_Envelope_Golden(t *testing.T) {
	t.Parallel()

	const clusterID = "golden-cluster-id"

	// Realistic entry: success outcome, two ledgers in the proposal,
	// transient + purged accounts (testing the map iteration order), a
	// caller snapshot with two scopes + key_id source.
	entry := &auditpb.AuditEntry{
		Sequence:    42,
		Timestamp:   &commonpb.Timestamp{Data: 1700000000},
		ProposalId:  77,
		OrderCount:  2,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Ledgers:     []string{"ledger-b", "ledger-a"}, // intentionally un-sorted: builder must sort
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{
				MinLogSequence: 100,
				MaxLogSequence: 101,
				TransientAccounts: map[string]*auditpb.AccountList{
					"ledger-b": {Accounts: []string{"users:1", "users:0"}}, // builder must sort
					"ledger-a": {Accounts: []string{"users:2"}},
				},
				PurgedAccounts: map[string]*auditpb.AccountList{
					"ledger-a": {Accounts: []string{"world"}},
				},
			},
		},
		CallerSnapshot: &commonpb.CallerSnapshot{
			Identity: &commonpb.CallerIdentity{
				Subject: "alice",
				Source:  &commonpb.CallerIdentity_KeyId{KeyId: "kid-1"},
			},
			Scopes: []string{"write", "read"}, // builder must sort
			God:    false,
		},
	}

	items := []*auditpb.AuditItem{
		{OrderIndex: 0, LogSequence: 100, SerializedOrder: []byte("order-A")},
		{OrderIndex: 1, LogSequence: 0, SerializedOrder: []byte("order-B")}, // log_sequence=0 simulates idempotent reference
	}

	lastHash := []byte("previous-chain-link")

	// 1. Build via the production helpers.
	headerPayload, err := BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range items {
		hashSlices = append(hashSlices, BuildPerItemPayload(item))
	}

	g := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)
	_, gotHash := g.Compute(nil, lastHash, hashSlices)

	// 2. Recompute the same hash entirely by hand (no production helpers
	//    in the pre-image construction, no HashGenerator). The aim is to
	//    pin the spec, not to test that the helpers are self-consistent.
	expectedHeader := goldenBuildHeader(entry)
	require.Equal(t, expectedHeader, headerPayload,
		"BuildHashedHeaderPayload drifted from the golden hand-rolled spec")

	expectedItem0 := goldenBuildPerItem(items[0])
	require.Equal(t, expectedItem0, BuildPerItemPayload(items[0]),
		"BuildPerItemPayload drifted from the golden hand-rolled spec (item 0)")

	expectedItem1 := goldenBuildPerItem(items[1])
	require.Equal(t, expectedItem1, BuildPerItemPayload(items[1]),
		"BuildPerItemPayload drifted from the golden hand-rolled spec (item 1)")

	keyMaterial := blake3.Sum256([]byte("audit-hash:blake3:v1:" + clusterID))

	hasher, err := blake3.NewKeyed(keyMaterial[:])
	require.NoError(t, err)
	_, _ = hasher.Write(expectedHeader)
	_, _ = hasher.Write(expectedItem0)
	_, _ = hasher.Write(expectedItem1)
	_, _ = hasher.Write(lastHash)
	expectedHash := hasher.Sum(nil)

	require.Equal(t, expectedHash, gotHash,
		"audit chain hash drifted from H(blake3-key(clusterID), header || items... || lastHash). "+
			"If this drift is intentional, bump commonpb.HashAlgorithm and add a new envelope version.")
}

// goldenBuildHeader implements the envelope spec from scratch, without
// using anything from audit_envelope.go. Any change to the production
// builder that diverges from this spec must also update this function —
// the diff between the two is the contract.
func goldenBuildHeader(e *auditpb.AuditEntry) []byte {
	var buf []byte

	buf = goldenU64(buf, e.GetSequence())
	buf = goldenU64(buf, e.GetTimestamp().GetData())
	buf = goldenU64(buf, e.GetProposalId())
	buf = goldenU32(buf, e.GetOrderCount())
	buf = goldenU32(buf, e.GetHashVersion())

	ledgers := append([]string(nil), e.GetLedgers()...)
	goldenSortStrings(ledgers)
	buf = goldenU32(buf, uint32(len(ledgers)))

	for _, l := range ledgers {
		buf = goldenLenString(buf, l)
	}

	switch out := e.GetOutcome().(type) {
	case *auditpb.AuditEntry_Success:
		buf = append(buf, 0x00) // outcome_tag = success
		buf = goldenLenBytes(buf, goldenBuildSuccess(out.Success))
	case *auditpb.AuditEntry_Failure:
		buf = append(buf, 0x01) // outcome_tag = failure
		buf = goldenLenBytes(buf, goldenBuildFailure(out.Failure))
	default:
		buf = append(buf, 0x00)
		buf = goldenLenBytes(buf, nil)
	}

	if snap := e.GetCallerSnapshot(); snap != nil {
		buf = goldenLenBytes(buf, goldenBuildSnapshot(snap))
	} else {
		buf = goldenLenBytes(buf, nil)
	}

	return buf
}

func goldenBuildSuccess(s *auditpb.AuditSuccess) []byte {
	var buf []byte
	buf = goldenU64(buf, s.GetMinLogSequence())
	buf = goldenU64(buf, s.GetMaxLogSequence())
	buf = goldenAccountMap(buf, s.GetTransientAccounts())
	buf = goldenAccountMap(buf, s.GetPurgedAccounts())

	return buf
}

func goldenBuildFailure(f *auditpb.AuditFailure) []byte {
	var buf []byte
	buf = goldenLenString(buf, f.GetErrorType())
	buf = goldenLenString(buf, f.GetMessage())

	keys := make([]string, 0, len(f.GetContext()))
	for k := range f.GetContext() {
		keys = append(keys, k)
	}

	goldenSortStrings(keys)
	buf = goldenU32(buf, uint32(len(keys)))

	for _, k := range keys {
		buf = goldenLenString(buf, k)
		buf = goldenLenString(buf, f.GetContext()[k])
	}

	return buf
}

func goldenAccountMap(buf []byte, m map[string]*auditpb.AccountList) []byte {
	ledgers := make([]string, 0, len(m))
	for k := range m {
		ledgers = append(ledgers, k)
	}

	goldenSortStrings(ledgers)
	buf = goldenU32(buf, uint32(len(ledgers)))

	for _, l := range ledgers {
		buf = goldenLenString(buf, l)

		accs := append([]string(nil), m[l].GetAccounts()...)
		goldenSortStrings(accs)
		buf = goldenU32(buf, uint32(len(accs)))

		for _, a := range accs {
			buf = goldenLenString(buf, a)
		}
	}

	return buf
}

func goldenBuildSnapshot(s *commonpb.CallerSnapshot) []byte {
	var buf []byte
	id := s.GetIdentity()
	buf = goldenLenString(buf, id.GetSubject())

	switch src := id.GetSource().(type) {
	case *commonpb.CallerIdentity_Issuer:
		buf = append(buf, 0x01) // callerSourceIssuer
		buf = goldenLenString(buf, src.Issuer)
	case *commonpb.CallerIdentity_KeyId:
		buf = append(buf, 0x02) // callerSourceKeyID
		buf = goldenLenString(buf, src.KeyId)
	default:
		buf = append(buf, 0x00) // callerSourceNone
		buf = goldenLenBytes(buf, nil)
	}

	if s.GetGod() {
		buf = append(buf, 0x01)
	} else {
		buf = append(buf, 0x00)
	}

	scopes := append([]string(nil), s.GetScopes()...)
	goldenSortStrings(scopes)
	buf = goldenU32(buf, uint32(len(scopes)))

	for _, sc := range scopes {
		buf = goldenLenString(buf, sc)
	}

	return buf
}

func goldenBuildPerItem(item *auditpb.AuditItem) []byte {
	var buf []byte
	buf = goldenU32(buf, item.GetOrderIndex())
	buf = goldenU64(buf, item.GetLogSequence())
	buf = goldenLenBytes(buf, item.GetSerializedOrder())

	return buf
}

func goldenU32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)

	return append(buf, b[:]...)
}

func goldenU64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)

	return append(buf, b[:]...)
}

func goldenLenBytes(buf []byte, value []byte) []byte {
	buf = goldenU32(buf, uint32(len(value)))
	buf = append(buf, value...)

	return buf
}

func goldenLenString(buf []byte, s string) []byte {
	return goldenLenBytes(buf, []byte(s))
}

func goldenSortStrings(s []string) {
	// Inline insertion sort to avoid pulling in slices.Sort — keeps the
	// golden helpers self-contained and unmistakably independent of the
	// production code paths.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

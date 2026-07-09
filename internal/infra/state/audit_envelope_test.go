package state

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
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

	// Realistic SUCCESS entry: two ledgers in the proposal (unsorted to
	// exercise the builder's sort), a caller snapshot with two scopes
	// (also unsorted) + key_id source. The failure variant — which is
	// the only path that still carries a Go map in the hashed pre-image
	// (AuditFailure.context) — is covered by TestHashChain_Envelope_Failure
	// below; we keep both around so any drift between the production
	// builder and the golden spec is caught regardless of outcome.
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
		Idempotency: &commonpb.Idempotency{Key: "batch-key-42"},
		Signature: &signaturepb.SignedApplyBatch{
			KeyId:     "sign-kid",
			Signature: []byte("sig-bytes"),
			Payload:   []byte("batch-payload"),
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

// TestHashChain_Envelope_SystemCaller pins the system_component source tag in
// the hash pre-image and proves a system-attributed entry is NOT byte-equal to
// a caller-less one — the whole point of tagging system actions is that they
// are distinguishable in the audit trail, hash included.
func TestHashChain_Envelope_SystemCaller(t *testing.T) {
	t.Parallel()

	base := func() *auditpb.AuditEntry {
		return &auditpb.AuditEntry{
			Sequence:    7,
			Timestamp:   &commonpb.Timestamp{Data: 1700000000},
			ProposalId:  9,
			OrderCount:  1,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			Ledgers:     []string{"ledger-a"},
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 1},
			},
		}
	}

	systemEntry := base()
	systemEntry.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentChapterArchiver)

	systemHeader, err := BuildHashedHeaderPayload(systemEntry)
	require.NoError(t, err)

	// Production builder agrees with the hand-rolled spec for the new tag.
	require.Equal(t, goldenBuildHeader(systemEntry), systemHeader,
		"buildCallerSnapshotPayload drifted from the golden spec for system_component")

	// System-attributed vs caller-less must produce different pre-images.
	nilCallerHeader, err := BuildHashedHeaderPayload(base())
	require.NoError(t, err)
	require.NotEqual(t, nilCallerHeader, systemHeader,
		"a system-tagged audit entry must not hash identically to a caller-less one")
}

// TestHashChain_Envelope_Failure exercises the failure outcome path. The
// AuditFailure.context map<string,string> is the only Go map left in the
// hashed pre-image after PR #542, so a desync between buildAuditFailurePayload
// and goldenBuildFailure on key ordering would silently produce drifting
// hashes — the success-only golden case above misses that drift entirely.
// We deliberately seed the map with keys that hash-iterate in a different
// order than they sort, forcing both builders to actually run the sort.
func TestHashChain_Envelope_Failure(t *testing.T) {
	t.Parallel()

	const clusterID = "golden-cluster-id"

	entry := &auditpb.AuditEntry{
		Sequence:    43,
		Timestamp:   &commonpb.Timestamp{Data: 1700000001},
		ProposalId:  78,
		OrderCount:  3,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Ledgers:     []string{"ledger-a"},
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
				// Intentionally unsorted: zebra, apple, mango force the
				// builder to actually sort.
				Context: map[string]string{
					"zebra":  "z-value",
					"apple":  "a-value",
					"mango":  "m-value",
					"banana": "b-value",
				},
			},
		},
	}

	items := []*auditpb.AuditItem{
		{OrderIndex: 0, LogSequence: 0, SerializedOrder: []byte("order-A")},
		{OrderIndex: 1, LogSequence: 0, SerializedOrder: []byte("order-B")},
		{OrderIndex: 2, LogSequence: 0, SerializedOrder: []byte("order-C")},
	}

	lastHash := []byte("previous-chain-link")

	headerPayload, err := BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	expectedHeader := goldenBuildHeader(entry)
	require.Equal(t, expectedHeader, headerPayload,
		"BuildHashedHeaderPayload drifted from golden spec on the failure path "+
			"(likely a regression on AuditFailure.context map sort order)")

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)
	for _, item := range items {
		hashSlices = append(hashSlices, BuildPerItemPayload(item))
	}

	g := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)
	_, gotHash := g.Compute(nil, lastHash, hashSlices)

	keyMaterial := blake3.Sum256([]byte("audit-hash:blake3:v1:" + clusterID))
	hasher, err := blake3.NewKeyed(keyMaterial[:])
	require.NoError(t, err)
	_, _ = hasher.Write(expectedHeader)
	for _, item := range items {
		_, _ = hasher.Write(goldenBuildPerItem(item))
	}
	_, _ = hasher.Write(lastHash)
	expectedHash := hasher.Sum(nil)

	require.Equal(t, expectedHash, gotHash,
		"audit chain hash for a failure entry drifted from golden spec")

	// Stability under repeated builds — protects against a future
	// "optimization" that re-introduces map iteration order into the
	// canonical payload.
	for i := range 5 {
		again, err := BuildHashedHeaderPayload(entry)
		require.NoError(t, err)
		require.Equal(t, headerPayload, again,
			"BuildHashedHeaderPayload is not stable across calls (iteration %d)", i)
	}
}

// TestAuditEntry_MarshalDeterministicVT_StableAcrossRuns guards the OTHER
// canonicalisation path: appendAuditEntries persists AuditEntry via
// MarshalDeterministicVT, so cross-node byte compares on the audit stream
// depend on this method being a pure function of the entry's contents.
// vtproto's generated code sorts map keys, but the test pins that contract
// explicitly — a future regeneration that flips back to map-iteration
// order would break the persisted-byte invariant without warning today.
func TestAuditEntry_MarshalDeterministicVT_StableAcrossRuns(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:    99,
		Timestamp:   &commonpb.Timestamp{Data: 1700000002},
		ProposalId:  100,
		OrderCount:  1,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Ledgers:     []string{"ledger-a"},
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_VALIDATION,
				Message: "y",
				Context: map[string]string{
					"k3": "v3",
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
	}

	baseline := entry.MarshalDeterministicVT(nil)
	require.NotEmpty(t, baseline)

	for i := range 10 {
		got := entry.MarshalDeterministicVT(nil)
		require.Equal(t, baseline, got,
			"MarshalDeterministicVT is not stable across calls (iteration %d) — "+
				"a regression here breaks cross-node byte compares on the audit stream",
			i)
	}
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

	buf = goldenLenString(buf, e.GetIdempotency().GetKey())
	buf = goldenLenBytes(buf, goldenBuildSignature(e.GetSignature()))

	return buf
}

func goldenBuildSignature(sb *signaturepb.SignedApplyBatch) []byte {
	if sb == nil {
		return nil
	}

	var buf []byte
	buf = goldenLenString(buf, sb.GetKeyId())
	buf = goldenLenBytes(buf, sb.GetSignature())
	buf = goldenLenBytes(buf, sb.GetPayload())

	return buf
}

func goldenBuildSuccess(s *auditpb.AuditSuccess) []byte {
	var buf []byte
	buf = goldenU64(buf, s.GetMinLogSequence())
	buf = goldenU64(buf, s.GetMaxLogSequence())

	return buf
}

func goldenBuildFailure(f *auditpb.AuditFailure) []byte {
	var buf []byte
	buf = goldenU32(buf, uint32(f.GetReason()))
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
	case *commonpb.CallerIdentity_SystemComponent:
		buf = append(buf, 0x03) // callerSourceSystem
		buf = goldenLenString(buf, src.SystemComponent)
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

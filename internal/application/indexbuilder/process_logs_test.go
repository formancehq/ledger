package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestCloneBytes(t *testing.T) {
	t.Parallel()

	t.Run("normal slice", func(t *testing.T) {
		t.Parallel()

		original := []byte{1, 2, 3, 4, 5}
		cloned := cloneBytes(original)

		assert.Equal(t, original, cloned)
		// Verify it is a distinct allocation.
		original[0] = 99
		assert.NotEqual(t, original[0], cloned[0])
	})

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		original := []byte{}
		cloned := cloneBytes(original)

		assert.Equal(t, original, cloned)
		assert.Equal(t, 0, len(cloned))
	})

	t.Run("nil slice", func(t *testing.T) {
		t.Parallel()

		// nil slice treated as zero-length.
		cloned := cloneBytes(nil)
		assert.Equal(t, 0, len(cloned))
	})
}

func TestExtractMetadataKeyFromReverseMap_Account(t *testing.T) {
	t.Parallel()

	// Build a reverse map key for an account.
	// Key format after stripping PrefixReverseMap byte:
	//   [ledger\x00][a:][account\x00][metadataKey]
	// The nsPrefix (also without PrefixReverseMap) is: [ledger\x00][a:]
	ledger := "myled"
	account := "users:alice"
	metaKey := "role"
	ns := readstore.NamespaceAccount

	// Build the suffix (everything after PrefixReverseMap byte).
	var suffix []byte
	suffix = append(suffix, ledger...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, account...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, metaKey...)

	// Build the nsPrefix (everything up to and including namespace).
	var nsPrefix []byte
	nsPrefix = append(nsPrefix, ledger...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractMetadataKeyFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, metaKey, result)
}

func TestExtractMetadataKeyFromReverseMap_Transaction(t *testing.T) {
	t.Parallel()

	ledger := "myled"
	metaKey := "category"
	ns := readstore.NamespaceTransaction

	// Build the suffix: [ledger\x00][t:][txID(8B)][metadataKey]
	var suffix []byte
	suffix = append(suffix, ledger...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	// 8-byte txID (e.g., txID=42 big-endian).
	suffix = append(suffix, 0, 0, 0, 0, 0, 0, 0, 42)
	suffix = append(suffix, metaKey...)

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, ledger...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractMetadataKeyFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, metaKey, result)
}

func TestExtractMetadataKeyFromReverseMap_AccountNoNullTerminator(t *testing.T) {
	t.Parallel()

	ns := readstore.NamespaceAccount

	// If there's no null terminator in the account suffix, return empty.
	var suffix []byte
	suffix = append(suffix, "ledger"...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, "nonnullaccount"...) // no 0x00 terminator

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, "ledger"...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractMetadataKeyFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, "", result)
}

func TestExtractMetadataKeyFromReverseMap_TransactionTooShort(t *testing.T) {
	t.Parallel()

	ns := readstore.NamespaceTransaction

	// If the suffix after nsPrefix is < 8 bytes (no room for txID), return empty.
	var suffix []byte
	suffix = append(suffix, "led"...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, 1, 2, 3) // only 3 bytes, need 8

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, "led"...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractMetadataKeyFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, "", result)
}

func TestExtractMetadataKeyFromReverseMap_TransactionExactly8Bytes(t *testing.T) {
	t.Parallel()

	ns := readstore.NamespaceTransaction

	// Exactly 8 bytes = txID only, no metadata key.
	var suffix []byte
	suffix = append(suffix, "led"...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, 0, 0, 0, 0, 0, 0, 0, 1) // 8 bytes

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, "led"...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractMetadataKeyFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, "", result)
}

func TestExtractEntityIDFromReverseMap_Account(t *testing.T) {
	t.Parallel()

	ledger := "myled"
	account := "users:alice"
	ns := readstore.NamespaceAccount

	// suffix: [ledger\x00][a:][account\x00][metadataKey]
	var suffix []byte
	suffix = append(suffix, ledger...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, account...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, "somekey"...)

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, ledger...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractEntityIDFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, []byte(account), result)
}

func TestExtractEntityIDFromReverseMap_AccountNoNull(t *testing.T) {
	t.Parallel()

	ns := readstore.NamespaceAccount

	// If there is no null terminator, the whole suffix is returned.
	var suffix []byte
	suffix = append(suffix, "led"...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, "noterm"...)

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, "led"...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractEntityIDFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, []byte("noterm"), result)
}

func TestExtractEntityIDFromReverseMap_Transaction(t *testing.T) {
	t.Parallel()

	ns := readstore.NamespaceTransaction

	// suffix: [ledger\x00][t:][txID(8B)][metadataKey]
	var suffix []byte
	suffix = append(suffix, "led"...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	txIDBytes := []byte{0, 0, 0, 0, 0, 0, 0, 42}
	suffix = append(suffix, txIDBytes...)
	suffix = append(suffix, "metakey"...)

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, "led"...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractEntityIDFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, txIDBytes, result)
}

func TestExtractEntityIDFromReverseMap_TransactionShort(t *testing.T) {
	t.Parallel()

	ns := readstore.NamespaceTransaction

	// If suffix < 8 bytes, return whatever is there.
	var suffix []byte
	suffix = append(suffix, "l"...)
	suffix = append(suffix, 0x00)
	suffix = append(suffix, ns...)
	suffix = append(suffix, 1, 2, 3)

	var nsPrefix []byte
	nsPrefix = append(nsPrefix, "l"...)
	nsPrefix = append(nsPrefix, 0x00)
	nsPrefix = append(nsPrefix, ns...)

	result := extractEntityIDFromReverseMap(suffix, nsPrefix, ns)
	assert.Equal(t, []byte{1, 2, 3}, result)
}

func TestLookupPreviousAccountValue_NilMap(t *testing.T) {
	t.Parallel()

	result := lookupPreviousAccountValue(nil, "acct", "key")
	assert.Nil(t, result)
}

func TestLookupPreviousAccountValue_AccountNotFound(t *testing.T) {
	t.Parallel()

	prevMeta := map[string]*commonpb.MetadataMap{
		"other": {Values: map[string]*commonpb.MetadataValue{
			"key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "val"}},
		}},
	}

	result := lookupPreviousAccountValue(prevMeta, "acct", "key")
	assert.Nil(t, result)
}

func TestLookupPreviousAccountValue_NilSet(t *testing.T) {
	t.Parallel()

	prevMeta := map[string]*commonpb.MetadataMap{
		"acct": nil,
	}

	result := lookupPreviousAccountValue(prevMeta, "acct", "key")
	assert.Nil(t, result)
}

func TestLookupPreviousAccountValue_KeyNotFound(t *testing.T) {
	t.Parallel()

	prevMeta := map[string]*commonpb.MetadataMap{
		"acct": {Values: map[string]*commonpb.MetadataValue{
			"other": {Type: &commonpb.MetadataValue_StringValue{StringValue: "val"}},
		}},
	}

	result := lookupPreviousAccountValue(prevMeta, "acct", "key")
	assert.Nil(t, result)
}

func TestLookupPreviousAccountValue_Found(t *testing.T) {
	t.Parallel()

	mv := &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "hello"}}
	prevMeta := map[string]*commonpb.MetadataMap{
		"users:alice": {Values: map[string]*commonpb.MetadataValue{
			"role": mv,
		}},
	}

	result := lookupPreviousAccountValue(prevMeta, "users:alice", "role")
	expected := readstore.EncodeMetadataValue(nil, mv)
	assert.Equal(t, expected, result)
}

func TestLookupPreviousAccountValue_FoundAmongMultiple(t *testing.T) {
	t.Parallel()

	mv1 := &commonpb.MetadataValue{Type: &commonpb.MetadataValue_IntValue{IntValue: 42}}
	mv2 := &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{BoolValue: true}}
	prevMeta := map[string]*commonpb.MetadataMap{
		"acct": {Values: map[string]*commonpb.MetadataValue{
			"count":  mv1,
			"active": mv2,
		}},
	}

	result := lookupPreviousAccountValue(prevMeta, "acct", "active")
	expected := readstore.EncodeMetadataValue(nil, mv2)
	assert.Equal(t, expected, result)
}

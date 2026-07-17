package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// writeAccountMetadata stores an account metadata value directly in Pebble so
// the admissionValueSource can read it back through the shared snapshot.
func writeAccountMetadata(t *testing.T, admission *Admission, ledger, account, key string, value *commonpb.MetadataValue) {
	t.Helper()

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
		Key:        key,
	}

	batch := admission.store.OpenWriteSession()
	_, err := admission.attrs.Metadata.Set(batch, metaKey.Bytes(), value)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// TestAdmissionValueSource_Metadata_PresentEmptyString pins the fix for the
// empty-string presence bug on the admission side: a stored StringValue("") is
// a valid, PRESENT metadata value. Presence must be driven by nil-ness alone,
// never by str=="", or a valid meta() read of an empty string resolves as
// absent — diverging from the FSM scopeValueSource and poisoning the resolution
// hash with the absent sentinel.
func TestAdmissionValueSource_Metadata_PresentEmptyString(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	writeAccountMetadata(t, admission, testLedgerName, "users:001", "note", commonpb.NewStringValue(""))

	source := &admissionValueSource{admission: admission, ledgerName: testLedgerName}

	value, present, err := source.Metadata("users:001", "note")
	require.NoError(t, err)
	require.True(t, present, "a present empty-string metadata value must be reported as present")
	require.Equal(t, "", value)
}

// TestAdmissionValueSource_Metadata_Absent confirms a genuinely absent key is
// reported as not present.
func TestAdmissionValueSource_Metadata_Absent(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	source := &admissionValueSource{admission: admission, ledgerName: testLedgerName}

	value, present, err := source.Metadata("users:001", "missing")
	require.NoError(t, err)
	require.False(t, present)
	require.Equal(t, "", value)
}

// TestAdmissionValueSource_Metadata_NonEmpty confirms a normal value round-trips.
func TestAdmissionValueSource_Metadata_NonEmpty(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	writeAccountMetadata(t, admission, testLedgerName, "users:001", "status", commonpb.NewStringValue("active"))

	source := &admissionValueSource{admission: admission, ledgerName: testLedgerName}

	value, present, err := source.Metadata("users:001", "status")
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, "active", value)
}

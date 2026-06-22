package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestExtractLedgerScopedNeeds_ProtoExhaustive walks the LedgerScopedOrder
// payload oneof reflectively and asserts that every variant the proto
// descriptor lists is also covered by the manually-enumerated dispatch
// tests in this package. Without this, a new payload field could be added
// to the proto without a matching needs case in extractLedgerScopedNeeds —
// the existing manual tests would still pass because they don't know about
// the new variant.
//
// Pair with the loud default in extractLedgerScopedNeeds: this test is the
// compile-time gate; the default is the runtime gate.
func TestExtractLedgerScopedNeeds_ProtoExhaustive(t *testing.T) {
	t.Parallel()

	covered := ledgerScopedCoveredByDispatchTest(t)

	// Reflect over the LedgerScopedOrder oneof and assert every field name
	// the proto declares is enumerated by the dispatch test.
	desc := (&raftcmdpb.LedgerScopedOrder{}).ProtoReflect().Descriptor()
	oneofs := desc.Oneofs()
	require.Equal(t, 1, oneofs.Len(), "LedgerScopedOrder must have exactly one oneof named 'payload'")

	payload := oneofs.Get(0)
	require.Equal(t, "payload", string(payload.Name()))

	fields := payload.Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		name := string(f.Name())
		require.Contains(t, covered, name,
			"LedgerScopedOrder.payload.%s is declared in the proto but not exercised by TestExtractLedgerScopedNeeds_CoversEveryPayloadVariant — add a case there and a matching arm in extractLedgerScopedNeeds", name)
	}
}

// TestExtractSystemScopedNeeds_ProtoExhaustive mirrors the ledger-scoped
// test for the system-scoped oneof. Even though most system-scoped variants
// are intentional no-ops in extractSystemScopedNeeds, every one must be
// listed there explicitly (so the loud default catches new variants) — and
// must appear in the dispatch test that drives them through ProcessOrder.
func TestExtractSystemScopedNeeds_ProtoExhaustive(t *testing.T) {
	t.Parallel()

	covered := systemScopedCoveredByDispatchTest(t)

	desc := (&raftcmdpb.SystemScopedOrder{}).ProtoReflect().Descriptor()
	oneofs := desc.Oneofs()
	require.Equal(t, 1, oneofs.Len(), "SystemScopedOrder must have exactly one oneof named 'payload'")

	payload := oneofs.Get(0)
	require.Equal(t, "payload", string(payload.Name()))

	fields := payload.Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		name := string(f.Name())
		require.Contains(t, covered, name,
			"SystemScopedOrder.payload.%s is declared in the proto but not exercised by TestExtractSystemScopedNeeds_OnlySinkConfigsContribute — add a case there and (if it preloads) a matching arm in extractSystemScopedNeeds", name)
	}
}

// ledgerScopedCoveredByDispatchTest returns the snake_case payload names the
// `wrapper_dispatch_test.go` table-driven test enumerates. Kept here as a
// derived list rather than importing the test fixtures: the test author must
// add the field name *here* when they add a case in
// TestExtractLedgerScopedNeeds_CoversEveryPayloadVariant — that double-write
// is the protection against silent drift between the proto and the test.
func ledgerScopedCoveredByDispatchTest(t *testing.T) map[string]struct{} {
	t.Helper()

	return map[string]struct{}{
		"apply":                  {},
		"create_ledger":          {},
		"delete_ledger":          {},
		"mirror_ingest":          {},
		"promote_ledger":         {},
		"save_ledger_metadata":   {},
		"delete_ledger_metadata": {},
		"save_numscript":         {},
		"delete_numscript":       {},
		"create_prepared_query":  {},
		"update_prepared_query":  {},
		"delete_prepared_query":  {},
	}
}

func systemScopedCoveredByDispatchTest(t *testing.T) map[string]struct{} {
	t.Helper()

	return map[string]struct{}{
		"register_signing_key":             {},
		"revoke_signing_key":               {},
		"set_signing_config":               {},
		"add_events_sink":                  {},
		"remove_events_sink":               {},
		"close_chapter":                    {},
		"seal_chapter":                     {},
		"archive_chapter":                  {},
		"confirm_archive_chapter":          {},
		"set_maintenance_mode":             {},
		"set_chapter_schedule":             {},
		"delete_chapter_schedule":          {},
		"create_query_checkpoint":          {},
		"delete_query_checkpoint":          {},
		"set_query_checkpoint_schedule":    {},
		"delete_query_checkpoint_schedule": {},
	}
}

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// modelLedgerForInfo builds a ledger exercising every field ledgerInfoMatches
// compares, and the LedgerInfo the server must answer for it.
func modelLedgerForInfo(t *testing.T) (oracle.GlobalState, *commonpb.LedgerInfo) {
	t.Helper()

	applied := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
			Name:                   "L",
			Mode:                   commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			MirrorSource:           &commonpb.MirrorSourceConfig{LedgerName: "src"},
			DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			AccountTypes:           map[string]*commonpb.AccountType{"known": {Name: "known", Pattern: "known:{id}"}},
			InitialSchema: []*commonpb.SetMetadataFieldTypeCommand{{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "rank",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			}},
		}}},
		actions.SaveLedgerMetadataAction("L", map[string]string{"owner": "treasury"}),
	}})
	require.True(t, applied.OK)

	return applied.State, &commonpb.LedgerInfo{
		Name:                   "L",
		Id:                     7,
		CreatedAt:              &commonpb.Timestamp{Data: 1000},
		Mode:                   commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource:           &commonpb.MirrorSourceConfig{LedgerName: "src"},
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes:           map[string]*commonpb.AccountType{"known": {Name: "known", Pattern: "known:{id}"}},
		MetadataSchema: &commonpb.MetadataSchema{AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"rank": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		}},
		Metadata: map[string]*commonpb.MetadataValue{
			"owner": {Type: &commonpb.MetadataValue_StringValue{StringValue: "treasury"}},
		},
	}
}

// Every field the model derives must break the match on its own: a comparison
// that quietly stops covering one field would otherwise keep passing.
func TestLedgerInfoMatchesComparesEveryDerivedField(t *testing.T) {
	t.Parallel()

	state, info := modelLedgerForInfo(t)
	require.True(t, ledgerInfoMatches(state, info))

	for name, corrupt := range map[string]func(*commonpb.LedgerInfo){
		"name": func(i *commonpb.LedgerInfo) { i.Name = "other" },
		"mode": func(i *commonpb.LedgerInfo) { i.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL },
		"mirror source": func(i *commonpb.LedgerInfo) {
			i.MirrorSource = &commonpb.MirrorSourceConfig{LedgerName: "elsewhere"}
		},
		"dropped mirror source": func(i *commonpb.LedgerInfo) { i.MirrorSource = nil },
		"enforcement mode": func(i *commonpb.LedgerInfo) {
			i.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		},
		"account type pattern": func(i *commonpb.LedgerInfo) {
			i.AccountTypes = map[string]*commonpb.AccountType{"known": {Name: "known", Pattern: "other:{id}"}}
		},
		"extra account type": func(i *commonpb.LedgerInfo) {
			i.AccountTypes["extra"] = &commonpb.AccountType{Name: "extra", Pattern: "extra:{id}"}
		},
		"schema type": func(i *commonpb.LedgerInfo) {
			i.MetadataSchema.AccountFields["rank"] = &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}
		},
		"schema target": func(i *commonpb.LedgerInfo) {
			i.MetadataSchema = &commonpb.MetadataSchema{LedgerFields: i.GetMetadataSchema().GetAccountFields()}
		},
		"dropped schema": func(i *commonpb.LedgerInfo) { i.MetadataSchema = nil },
		"metadata value": func(i *commonpb.LedgerInfo) {
			i.Metadata["owner"] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "other"}}
		},
		"dropped metadata": func(i *commonpb.LedgerInfo) { i.Metadata = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, corrupted := modelLedgerForInfo(t)
			corrupt(corrupted)
			require.False(t, ledgerInfoMatches(state, corrupted))
		})
	}
}

func TestLedgerInfoMatchesRejectsAnAbsentOrDeletedLedger(t *testing.T) {
	t.Parallel()

	state, info := modelLedgerForInfo(t)

	require.False(t, ledgerInfoMatches(oracle.NewGlobalState(), info))

	deleted := state.Apply(oracle.Bulk{Requests: []*servicepb.Request{actions.DeleteLedgerAction("L")}})
	require.True(t, deleted.OK)
	require.False(t, ledgerInfoMatches(deleted.State, info))
}

func TestLedgerInfoStructureViolation(t *testing.T) {
	t.Parallel()

	_, info := modelLedgerForInfo(t)
	require.Empty(t, ledgerInfoStructureViolation(info))

	for name, corrupt := range map[string]func(*commonpb.LedgerInfo){
		"listed ledger has no name":               func(i *commonpb.LedgerInfo) { i.Name = "" },
		"listed ledger has no id":                 func(i *commonpb.LedgerInfo) { i.Id = 0 },
		"listed ledger has no creation timestamp": func(i *commonpb.LedgerInfo) { i.CreatedAt = nil },
		"listing served a soft-deleted ledger":    func(i *commonpb.LedgerInfo) { i.DeletedAt = &commonpb.Timestamp{Data: 2} },
		"listing carried mirror sync progress":    func(i *commonpb.LedgerInfo) { i.MirrorSyncProgress = &commonpb.MirrorSyncProgress{} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, corrupted := modelLedgerForInfo(t)
			corrupt(corrupted)
			require.Equal(t, name, ledgerInfoStructureViolation(corrupted))
		})
	}
}

func TestLedgerIdentityMatchesBothFields(t *testing.T) {
	t.Parallel()

	_, info := modelLedgerForInfo(t)
	identity := ledgerIdentity{id: 7, createdAt: &commonpb.Timestamp{Data: 1000}}
	require.True(t, identity.matches(info))

	require.False(t, ledgerIdentity{id: 8, createdAt: identity.createdAt}.matches(info))
	require.False(t, ledgerIdentity{id: 7, createdAt: &commonpb.Timestamp{Data: 1001}}.matches(info))
	require.False(t, ledgerIdentity{id: 7}.matches(info))
}

// A recreate in flight makes either identity legal, so the pin must stand down
// for exactly as long as one could still land.
func TestLedgerIdentityIsNotPinnedWhileACreateIsUnsettled(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-0"}, nil)
	stale := &commonpb.LedgerInfo{Name: "model-0", Id: 9, CreatedAt: &commonpb.Timestamp{Data: 1}}

	require.Empty(t, c.ledgerIdentityViolation([]*commonpb.LedgerInfo{stale}),
		"a ledger whose creation was never observed is not pinned")

	c.ledgerIdentities["model-0"] = ledgerIdentity{id: 3, createdAt: &commonpb.Timestamp{Data: 1}}
	require.Equal(t, "model-0", c.ledgerIdentityViolation([]*commonpb.LedgerInfo{stale}))

	c.inflight[1] = oracle.Bulk{Requests: []*servicepb.Request{actions.CreateLedgerAction("model-0", nil)}}
	require.Empty(t, c.ledgerIdentityViolation([]*commonpb.LedgerInfo{stale}))

	delete(c.inflight, 1)
	c.pending = []*pendingObservation{{obs: observation{
		bulk: oracle.Bulk{Requests: []*servicepb.Request{actions.CreateLedgerAction("model-0", nil)}},
	}}}
	require.Empty(t, c.ledgerIdentityViolation([]*commonpb.LedgerInfo{stale}),
		"a create committed but not yet drained leaves either identity legal")

	c.pending = nil
	require.Equal(t, "model-0", c.ledgerIdentityViolation([]*commonpb.LedgerInfo{stale}),
		"once no create can land, the identity is the one creation reported")
}

// The unit comparisons pin that each field is read; only a real server proves
// the model's value is the one it actually serves for that field.
func TestListLedgersRowMatchesTheWholeModel(t *testing.T) {
	t.Parallel()
	ctx, client := skippableTestServer(t)

	create := &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
		Name:                   "L",
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes:           map[string]*commonpb.AccountType{"known": {Pattern: "known:{id}"}},
		InitialSchema: []*commonpb.SetMetadataFieldTypeCommand{{
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        "rank",
			Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
		}},
	}}}
	requests := []*servicepb.Request{
		create,
		actions.SaveLedgerMetadataAction("L", map[string]string{"owner": "treasury"}),
		// A type declared after creation lands in the same schema the listing
		// serves, so the two sources of declarations are both covered.
		{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
			Ledger:     "L",
			TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
			Key:        "tier",
			Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
		}}},
	}

	applied := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: requests})
	require.True(t, applied.OK)

	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", requests...))
	require.NoError(t, err)
	require.Len(t, resp.GetLogs(), len(requests))

	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{PageSize: 10}})
	require.NoError(t, err)
	rows, err := drainStream(stream)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0]
	require.Empty(t, ledgerInfoStructureViolation(row))
	require.True(t, ledgerInfoMatches(applied.State, row), "served %+v", row)

	// The identity the listing serves is the one the creation log announced.
	created := resp.GetLogs()[0].GetPayload().GetCreateLedger()
	require.NotNil(t, created)
	require.True(t, ledgerIdentity{id: created.GetId(), createdAt: created.GetCreatedAt()}.matches(row))
}

// A filter is not a supported option on this endpoint, so it must be refused
// rather than ignored.
func TestListLedgersRejectsAFilter(t *testing.T) {
	t.Parallel()
	ctx, client := skippableTestServer(t)

	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)

	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{
		PageSize: 10,
		Filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
			Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "known:"},
		}}},
	}})
	if err == nil {
		_, err = drainStream(stream)
	}
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// One base must explain the whole listing page. A bulk that updates A's
// metadata and deletes B splits the page across two states: the old fleet
// explains the continuation toward B, the new state explains A's row.
func TestLedgerListingMatchesNeedsOneBase(t *testing.T) {
	t.Parallel()

	created := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "A"}}},
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "B"}}},
	}})
	require.True(t, created.OK, created.Reason)
	before := created.State

	updated := before.Apply(oracle.Bulk{Requests: []*servicepb.Request{
		actions.SaveLedgerMetadataAction("A", map[string]string{"owner": "treasury"}),
		actions.DeleteLedgerAction("B"),
	}})
	require.True(t, updated.OK, updated.Reason)
	after := updated.State

	row := &commonpb.LedgerInfo{Name: "A", Metadata: map[string]*commonpb.MetadataValue{
		"owner": {Type: &commonpb.MetadataValue_StringValue{StringValue: "treasury"}},
	}}
	require.True(t, ledgerInfoMatches(after, row), "the row is the new state's A")
	served := map[string]*commonpb.LedgerInfo{"A": row}
	names := []string{"A"}

	require.True(t, ledgerWindowMatches(before, names, "", 1, false, "A"), "the old fleet still holds B past A")
	require.False(t, ledgerListingMatches(before, names, served, "", 1, false, "A"), "the old state's A has no metadata")
	require.False(t, ledgerListingMatches(after, names, served, "", 1, false, "A"), "the new fleet has nothing past A")
}

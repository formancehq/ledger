package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLedgerInfoJSONNormalDefaults(t *testing.T) {
	data, err := json.Marshal(&LedgerInfo{Name: "normal", AccountTypes: map[string]*AccountType{
		"users": {Name: "users", Pattern: "users:{id}", SegmentTypes: map[string]*SegmentType{
			"id": {Constraint: &SegmentType_Uuid{Uuid: &UUIDConstraint{}}},
		}},
	}})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"normal","mode":"NORMAL","defaultEnforcementMode":"STRICT",
		"accountTypes":{"users":{"name":"users","pattern":"users:{id}","persistence":"NORMAL",
		"segmentTypes":{"id":{"type":"uuid"}}}}}`, string(data))
}

func TestLedgerInfoJSONMetadataSchemaAndSegmentConstraints(t *testing.T) {
	ledger := &LedgerInfo{Name: "normal", DefaultEnforcementMode: ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		MetadataSchema: &MetadataSchema{LedgerFields: map[string]*MetadataFieldSchema{
			"owner": {}, "active": {Type: MetadataType_METADATA_TYPE_BOOL},
		}},
		AccountTypes: map[string]*AccountType{"accounts": {Name: "accounts", Pattern: "accounts:{id}",
			Persistence: AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT, SegmentTypes: map[string]*SegmentType{
				"regex":  {Constraint: &SegmentType_Regex{Regex: "[a-z]+"}},
				"uuid":   {Constraint: &SegmentType_Uuid{Uuid: &UUIDConstraint{}}},
				"uint64": {Constraint: &SegmentType_Uint64{Uint64: &Uint64Constraint{}}},
				"bytes":  {Constraint: &SegmentType_Bytes{Bytes: &BytesConstraint{}}},
				"any":    {},
			}}},
	}
	data, err := json.Marshal(ledger)
	require.NoError(t, err)
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &got))
	require.JSONEq(t, `{"ledgerFields":{"owner":{"type":"string"},"active":{"type":"bool"}}}`, string(got["metadataSchema"]))
	require.JSONEq(t, `{"accounts":{"name":"accounts","pattern":"accounts:{id}","persistence":"TRANSIENT",
		"segmentTypes":{"regex":{"type":"regex","regex":"[a-z]+"},"uuid":{"type":"uuid"},"uint64":{"type":"uint64"},"bytes":{"type":"bytes"},"any":null}}}`, string(got["accountTypes"]))
	require.Equal(t, `"AUDIT"`, string(got["defaultEnforcementMode"]))
}

func TestLedgerInfoJSONPropagatesInvalidValues(t *testing.T) {
	for _, ledger := range []*LedgerInfo{
		{Name: "bad", Mode: LedgerMode(999)},
		{Name: "bad", DefaultEnforcementMode: ChartEnforcementMode(999)},
		{Name: "bad", MirrorSyncProgress: &MirrorSyncProgress{State: MirrorSyncState(999)}},
		{Name: "bad", AccountTypes: map[string]*AccountType{"bad": {Persistence: AccountTypePersistence(999)}}},
		{Name: "bad", MirrorSource: &MirrorSourceConfig{LedgerName: "\xff"}},
		{Name: "bad", MetadataSchema: &MetadataSchema{LedgerFields: map[string]*MetadataFieldSchema{"bad": {Type: MetadataType(999)}}}},
	} {
		_, err := json.Marshal(ledger)
		require.Error(t, err)
	}
}

func TestLedgerInfoJSONMirrorProgress(t *testing.T) {
	stamp := &Timestamp{Data: 1791453600000000}
	for _, tc := range []struct {
		name     string
		progress *MirrorSyncProgress
		want     string
	}{
		{"zero", &MirrorSyncProgress{}, `{"state":"SYNCING","cursor":"0","sourceLogCount":"0","remainingLogs":"0"}`},
		{"following", &MirrorSyncProgress{State: MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING,
			Cursor: 9007199254740993, SourceLogCount: ^uint64(0), RemainingLogs: 1,
			Error: &MirrorSyncError{Message: "source unavailable", OccurredAt: stamp}},
			`{"state":"FOLLOWING","cursor":"9007199254740993","sourceLogCount":"18446744073709551615","remainingLogs":"1","error":{"message":"source unavailable","occurredAt":"2026-10-08T10:00:00Z"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(&LedgerInfo{Name: "mirror", Mode: LedgerMode_LEDGER_MODE_MIRROR,
				CreatedAt: stamp, DeletedAt: stamp, MirrorSyncProgress: tc.progress,
				Metadata: map[string]*MetadataValue{"active": NewBoolValue(true), "owner": NewStringValue("customer")}})
			require.NoError(t, err)
			var got map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &got))
			require.JSONEq(t, tc.want, string(got["mirrorSyncProgress"]))
			require.Equal(t, `"MIRROR"`, string(got["mode"]))
			require.Equal(t, `"2026-10-08T10:00:00Z"`, string(got["createdAt"]))
			require.Equal(t, string(got["createdAt"]), string(got["deletedAt"]))
			require.JSONEq(t, `{"active":true,"owner":"customer"}`, string(got["metadata"]))
		})
	}
}

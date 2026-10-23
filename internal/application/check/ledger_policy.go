package check

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ledgerPolicyVerifier compares both ledger projections to the isolated FSM
// replay of verified audit orders. Comparing the copies only to each other
// would miss coordinated corruption. Tombstones retain their final policy.
type ledgerPolicyVerifier struct {
	expected      map[string]*commonpb.LedgerInfo
	liveTruncated bool
}

func (v *ledgerPolicyVerifier) compare(reader dal.PebbleReader, attrs *attributes.Attributes, callback func(*servicepb.CheckStoreEvent)) error {
	if v.liveTruncated {
		return nil
	}
	canonical, err := attrs.Ledger.NewStreamingIter(reader, nil)
	if err != nil {
		return fmt.Errorf("reading canonical ledger policies: %w", err)
	}
	defer func() { _ = canonical.Close() }()
	seen := make(map[string]struct{}, len(v.expected))
	// The streaming iterator excludes the empty canonical key. Read it from
	// the same snapshot so a surplus row at the attribute prefix is visible.
	empty, err := attrs.Ledger.Get(reader, nil)
	if err != nil {
		return fmt.Errorf("reading empty canonical ledger policy: %w", err)
	}
	if empty != nil {
		seen[""] = struct{}{}
		v.compareRow("canonical", "", empty, callback)
	}
	for canonical.Next() {
		row := canonical.Entry()
		name := string(row.CanonicalKey)
		seen[name] = struct{}{}
		v.compareRow("canonical", name, row.Value, callback)
	}
	if err := canonical.Err(); err != nil {
		return fmt.Errorf("scanning canonical ledger policies: %w", err)
	}
	v.compareMissing("canonical", seen, callback)

	iter, err := dal.NewBoundedIter(reader, []byte{dal.ZoneGlobal, dal.SubGlobLedgerInfo}, dal.ZonePrefixUpperBound(dal.ZoneGlobal, dal.SubGlobLedgerInfo))
	if err != nil {
		return fmt.Errorf("reading global ledger policies: %w", err)
	}
	defer func() { _ = iter.Close() }()
	clear(seen)
	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		name := string(bytes.TrimSuffix(key[2:], []byte{0}))
		expectedKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).PutLedgerName(name).Build()
		if !bytes.Equal(key, expectedKey) || strings.ContainsRune(name, 0) {
			v.report("global", name, "key encoding", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LEDGER_INFO_MISMATCH, callback)

			continue
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading global ledger policy: %w", err)
		}
		info := &commonpb.LedgerInfo{}
		if err := info.UnmarshalVT(value); err != nil {
			return fmt.Errorf("decoding global ledger policy: %w", err)
		}
		seen[name] = struct{}{}
		v.compareRow("global", name, info, callback)
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("scanning global ledger policies: %w", err)
	}
	v.compareMissing("global", seen, callback)

	return nil
}

func (v *ledgerPolicyVerifier) compareMissing(projection string, seen map[string]struct{}, callback func(*servicepb.CheckStoreEvent)) {
	for name := range v.expected {
		if _, exists := seen[name]; !exists {
			v.report(projection, name, "missing ledger row", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MISSING_LEDGER, callback)
		}
	}
}

func (v *ledgerPolicyVerifier) compareRow(projection, name string, info *commonpb.LedgerInfo, callback func(*servicepb.CheckStoreEvent)) {
	expected, exists := v.expected[name]
	if !exists {
		v.report(projection, name, "unaudited ledger row", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNAUDITED_LEDGER, callback)

		return
	}
	if info.GetName() != name {
		v.report(projection, name, "name", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LEDGER_INFO_MISMATCH, callback)
	}
	if !expected.GetDeletedAt().EqualVT(info.GetDeletedAt()) {
		v.report(projection, name, "deletion state", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LEDGER_INFO_MISMATCH, callback)
	}
	if !schemaEqual(expected.GetMetadataSchema(), info.GetMetadataSchema()) {
		v.report(projection, name, "metadata schema", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SCHEMA_MISMATCH, callback)
	}
	if !accountTypesEqual(expected.GetAccountTypes(), info.GetAccountTypes()) {
		v.report(projection, name, "account types", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ACCOUNT_TYPE_MISMATCH, callback)
	}
	if expected.GetDefaultEnforcementMode() != info.GetDefaultEnforcementMode() {
		v.report(projection, name, "default enforcement mode", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_DEFAULT_ENFORCEMENT_MODE_MISMATCH, callback)
	}
	if expected.GetMode() != info.GetMode() {
		v.report(projection, name, "mode", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LEDGER_INFO_MISMATCH, callback)
	}
	if !expected.GetMirrorSource().EqualVT(info.GetMirrorSource()) {
		v.report(projection, name, "mirror source", servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LEDGER_INFO_MISMATCH, callback)
	}
}

func (v *ledgerPolicyVerifier) report(projection, name, field string, kind servicepb.CheckStoreErrorType, callback func(*servicepb.CheckStoreEvent)) {
	// Mirror sources can carry credentials; never include configuration values.
	callback(errorEvent(kind, fmt.Sprintf("%s ledger %q %s differs from audit-derived state", projection, name, field), 0, name, "", ""))
}

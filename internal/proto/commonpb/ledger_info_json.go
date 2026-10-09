package commonpb

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// MarshalJSON projects LedgerInfo onto the REST contract. Protobuf defaults,
// enum names and nested timestamps must not determine the public JSON shape.
// Secret removal belongs to the external response boundary, not this codec:
// internal configuration and authoritative records must retain their contents.
func (x *LedgerInfo) MarshalJSON() ([]byte, error) {
	mode := "NORMAL"
	switch x.GetMode() {
	case LedgerMode_LEDGER_MODE_NORMAL:
	case LedgerMode_LEDGER_MODE_MIRROR:
		mode = "MIRROR"
	default:
		return nil, fmt.Errorf("ledger info: unknown mode %v", x.GetMode())
	}
	enforcement := "STRICT"
	switch x.GetDefaultEnforcementMode() {
	case ChartEnforcementMode_CHART_ENFORCEMENT_STRICT:
	case ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT:
		enforcement = "AUDIT"
	default:
		return nil, fmt.Errorf("ledger info: unknown enforcement mode %v", x.GetDefaultEnforcementMode())
	}
	var schema *ledgerMetadataSchemaJSON
	var source json.RawValue
	if x.GetMetadataSchema() != nil {
		schema = &ledgerMetadataSchemaJSON{}
		var err error
		schema.AccountFields, err = ledgerMetadataFieldsJSON(x.GetMetadataSchema().GetAccountFields())
		if err != nil {
			return nil, err
		}
		schema.TransactionFields, err = ledgerMetadataFieldsJSON(x.GetMetadataSchema().GetTransactionFields())
		if err != nil {
			return nil, err
		}
		schema.LedgerFields, err = ledgerMetadataFieldsJSON(x.GetMetadataSchema().GetLedgerFields())
		if err != nil {
			return nil, err
		}
	}
	if x.GetMirrorSource() != nil {
		// This configuration's protojson shape is the documented read shape,
		// including rewrite-rule oneofs; it differs from the flat create input.
		b, err := protojson.Marshal(x.GetMirrorSource())
		if err != nil {
			return nil, fmt.Errorf("ledger info mirror source: %w", err)
		}
		source = b
	}
	var progress *ledgerMirrorProgressJSON
	if p := x.GetMirrorSyncProgress(); p != nil {
		state := "SYNCING"
		switch p.GetState() {
		case MirrorSyncState_MIRROR_SYNC_STATE_SYNCING:
		case MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING:
			state = "FOLLOWING"
		default:
			return nil, fmt.Errorf("ledger info: unknown mirror sync state %v", p.GetState())
		}
		progress = &ledgerMirrorProgressJSON{
			State: state, Cursor: strconv.FormatUint(p.GetCursor(), 10),
			SourceLogCount: strconv.FormatUint(p.GetSourceLogCount(), 10),
			RemainingLogs:  strconv.FormatUint(p.GetRemainingLogs(), 10),
		}
		if e := p.GetError(); e != nil {
			progress.Error = &ledgerMirrorErrorJSON{Message: e.GetMessage(), OccurredAt: e.GetOccurredAt()}
		}
	}
	var accounts map[string]ledgerAccountTypeJSON
	if len(x.GetAccountTypes()) > 0 {
		accounts = make(map[string]ledgerAccountTypeJSON, len(x.GetAccountTypes()))
		for name, at := range x.GetAccountTypes() {
			if _, ok := AccountTypePersistence_name[int32(at.GetPersistence())]; !ok {
				return nil, fmt.Errorf("ledger info: unknown account persistence %v", at.GetPersistence())
			}
			item := ledgerAccountTypeJSON{Name: at.GetName(), Pattern: at.GetPattern(), Persistence: PersistenceToString(at.GetPersistence())}
			if len(at.GetSegmentTypes()) > 0 {
				item.SegmentTypes = make(map[string]*SegmentTypeJSON, len(at.GetSegmentTypes()))
				for variable, constraint := range at.GetSegmentTypes() {
					item.SegmentTypes[variable] = SegmentTypeToJSON(constraint)
				}
			}
			accounts[name] = item
		}
	}

	return json.Marshal(&struct {
		Name                   string                           `json:"name"`
		CreatedAt              *Timestamp                       `json:"createdAt,omitempty"`
		DeletedAt              *Timestamp                       `json:"deletedAt,omitempty"`
		MetadataSchema         *ledgerMetadataSchemaJSON        `json:"metadataSchema,omitempty"`
		Mode                   string                           `json:"mode"`
		MirrorSource           json.RawValue                    `json:"mirrorSource,omitempty"`
		MirrorSyncProgress     *ledgerMirrorProgressJSON        `json:"mirrorSyncProgress,omitempty"`
		AccountTypes           map[string]ledgerAccountTypeJSON `json:"accountTypes,omitempty"`
		DefaultEnforcementMode string                           `json:"defaultEnforcementMode"`
		Metadata               map[string]any                   `json:"metadata,omitempty"`
	}{x.GetName(), x.GetCreatedAt(), x.GetDeletedAt(), schema, mode, source,
		progress, accounts, enforcement, MetadataToAnyMap(x.GetMetadata())})
}

type ledgerMetadataSchemaJSON struct {
	AccountFields     map[string]ledgerMetadataFieldJSON `json:"accountFields,omitempty"`
	TransactionFields map[string]ledgerMetadataFieldJSON `json:"transactionFields,omitempty"`
	LedgerFields      map[string]ledgerMetadataFieldJSON `json:"ledgerFields,omitempty"`
}

type ledgerMetadataFieldJSON struct {
	Type string `json:"type"`
}

func ledgerMetadataFieldsJSON(fields map[string]*MetadataFieldSchema) (map[string]ledgerMetadataFieldJSON, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	result := make(map[string]ledgerMetadataFieldJSON, len(fields))
	for key, field := range fields {
		if _, ok := MetadataType_name[int32(field.GetType())]; !ok {
			return nil, fmt.Errorf("ledger info: unknown metadata type %v", field.GetType())
		}
		result[key] = ledgerMetadataFieldJSON{Type: MetadataTypeToString(field.GetType())}
	}

	return result, nil
}

type ledgerAccountTypeJSON struct {
	Name         string                      `json:"name"`
	Pattern      string                      `json:"pattern"`
	Persistence  string                      `json:"persistence"`
	SegmentTypes map[string]*SegmentTypeJSON `json:"segmentTypes,omitempty"`
}

type ledgerMirrorProgressJSON struct {
	State          string                 `json:"state"`
	Cursor         string                 `json:"cursor"`
	SourceLogCount string                 `json:"sourceLogCount"`
	RemainingLogs  string                 `json:"remainingLogs"`
	Error          *ledgerMirrorErrorJSON `json:"error,omitempty"`
}

type ledgerMirrorErrorJSON struct {
	Message    string     `json:"message"`
	OccurredAt *Timestamp `json:"occurredAt,omitempty"`
}

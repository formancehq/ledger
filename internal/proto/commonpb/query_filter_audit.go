package commonpb

// AuditFieldKind is the value kind of a bare audit filter field.
type AuditFieldKind int

const (
	// AuditFieldKindUint is an unsigned integer field (raw uint64 operands).
	AuditFieldKindUint AuditFieldKind = iota
	// AuditFieldKindString is an exact-match string field.
	AuditFieldKindString
	// AuditFieldKindDatetime is a uint field whose operands additionally accept
	// an RFC3339 timestamp, coerced through CoerceDatetimeMicros to the uint64
	// microseconds the audit index stores. Raw microseconds still parse.
	AuditFieldKindDatetime
)

// AuditFieldSpec describes one bare audit filter field.
type AuditFieldSpec struct {
	Field AuditField
	Kind  AuditFieldKind
}

// auditFieldsByName is the single vocabulary of bare audit filter fields shared
// by the textual filterexpr grammar and the structured JSON DSL: on the AUDIT
// target, both serializations resolve exactly these names to the AuditCondition
// arm (EN-1549). The set is exactly the fields the audit access path can resolve
// efficiently (index lookup or key-range bound) — see AuditField in common.proto.
var auditFieldsByName = map[string]AuditFieldSpec{
	"seq":             {AuditField_AUDIT_FIELD_SEQUENCE, AuditFieldKindUint},
	"proposal_id":     {AuditField_AUDIT_FIELD_PROPOSAL_ID, AuditFieldKindUint},
	"timestamp":       {AuditField_AUDIT_FIELD_TIMESTAMP, AuditFieldKindDatetime},
	"log_seq":         {AuditField_AUDIT_FIELD_LOG_SEQUENCE, AuditFieldKindUint},
	"outcome":         {AuditField_AUDIT_FIELD_OUTCOME, AuditFieldKindString},
	"caller_subject":  {AuditField_AUDIT_FIELD_CALLER_SUBJECT, AuditFieldKindString},
	"ledger":          {AuditField_AUDIT_FIELD_LEDGER, AuditFieldKindString},
	"order_type":      {AuditField_AUDIT_FIELD_ORDER_TYPE, AuditFieldKindString},
	"idempotency_key": {AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, AuditFieldKindString},
}

var auditFieldNames = func() map[AuditField]string {
	out := make(map[AuditField]string, len(auditFieldsByName))
	for name, spec := range auditFieldsByName {
		out[spec.Field] = name
	}

	return out
}()

// LookupAuditField resolves a bare audit filter field name.
func LookupAuditField(name string) (AuditFieldSpec, bool) {
	spec, ok := auditFieldsByName[name]

	return spec, ok
}

// AuditFieldName returns the bare filter field name of an audit field.
func AuditFieldName(field AuditField) (string, bool) {
	name, ok := auditFieldNames[field]

	return name, ok
}

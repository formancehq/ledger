package commonpb

import "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

type AuditFieldKind = ledgerpb.AuditFieldKind
type AuditFieldSpec = ledgerpb.AuditFieldSpec

const (
	AuditFieldKindUint     = ledgerpb.AuditFieldKindUint
	AuditFieldKindString   = ledgerpb.AuditFieldKindString
	AuditFieldKindDatetime = ledgerpb.AuditFieldKindDatetime
)

var LookupAuditField = ledgerpb.LookupAuditField
var AuditFieldName = ledgerpb.AuditFieldName

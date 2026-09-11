// Package readprojection maps authoritative records to structured public views.
// It never rewrites stored audit bytes or signed evidence.
package readprojection

import (
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/domain/connectionconfig"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/publicauditpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Audit exposes typed order details and signing-key identity. The signature
// payload is deliberately never decoded: opaque input bytes, including fields
// shadowed by protobuf merging, cannot reach the public response schema.
func Audit(entry *auditpb.AuditEntry) (*publicauditpb.AuditEntry, error) {
	if entry == nil {
		return nil, errors.New("audit entry is nil")
	}
	result := &publicauditpb.AuditEntry{
		Sequence: entry.GetSequence(), Timestamp: entry.GetTimestamp(), ProposalId: entry.GetProposalId(),
		OrderCount: entry.GetOrderCount(), Ledgers: slices.Clone(entry.GetLedgers()),
		Hash: slices.Clone(entry.GetHash()), HashVersion: entry.GetHashVersion(),
		CallerSnapshot: entry.GetCallerSnapshot(), Idempotency: entry.GetIdempotency(),
	}
	switch outcome := entry.GetOutcome().(type) {
	case *auditpb.AuditEntry_Success:
		if outcome == nil || outcome.Success == nil {
			return nil, fmt.Errorf("audit entry %d has a nil success outcome", entry.GetSequence())
		}
		result.Outcome = &publicauditpb.AuditEntry_Success{Success: outcome.Success}
	case *auditpb.AuditEntry_Failure:
		if outcome == nil || outcome.Failure == nil {
			return nil, fmt.Errorf("audit entry %d has a nil failure outcome", entry.GetSequence())
		}
		result.Outcome = &publicauditpb.AuditEntry_Failure{Failure: &publicauditpb.AuditFailure{Reason: outcome.Failure.GetReason(), Message: outcome.Failure.GetMessage(), Context: outcome.Failure.GetContext()}}
	default:
		return nil, fmt.Errorf("audit entry %d has no recognized outcome", entry.GetSequence())
	}
	if signature := entry.GetSignature(); signature != nil {
		result.Signature = &publicauditpb.SignatureInfo{KeyId: signature.GetKeyId()}
	}
	for _, item := range entry.GetItems() {
		if item == nil {
			return nil, fmt.Errorf("audit entry %d contains a nil item", entry.GetSequence())
		}
		order := &raftcmdpb.Order{}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return nil, fmt.Errorf("decoding audit order %d for public view: %w", item.GetOrderIndex(), err)
		}
		view, err := publicOrder(order)
		if err != nil {
			return nil, err
		}
		result.Items = append(result.Items, &publicauditpb.AuditItem{
			OrderIndex: item.GetOrderIndex(), LogSequence: item.GetLogSequence(), Order: view,
		})
	}

	return sensitive.Clone(result), nil
}

func publicOrder(order *raftcmdpb.Order) (*publicauditpb.Order, error) {
	result := &publicauditpb.Order{}
	switch value := order.GetType().(type) {
	case *raftcmdpb.Order_LedgerScoped:
		source := value.LedgerScoped
		if source == nil {
			return nil, errors.New("audit order contains a nil ledger scope")
		}
		target := &publicauditpb.LedgerScopedOrder{Ledger: source.GetLedger()}
		if create := source.GetCreateLedger(); create != nil {
			config, err := connectionconfig.Mirror(create.GetMirrorSource())
			if err != nil {
				config = nil
			}
			target.Payload = &publicauditpb.LedgerScopedOrder_CreateLedger{CreateLedger: &publicauditpb.CreateLedgerOrder{
				InitialSchema: create.GetInitialSchema(), Mode: create.GetMode(), MirrorSource: config,
				AccountTypes: create.GetAccountTypes(), DefaultEnforcementMode: create.GetDefaultEnforcementMode(),
				ConfigurationUnavailable: err != nil,
			}}
		} else if err := copyOrderPayload(source.ProtoReflect(), target.ProtoReflect()); err != nil {
			return nil, err
		}
		result.Type = &publicauditpb.Order_LedgerScoped{LedgerScoped: target}
	case *raftcmdpb.Order_SystemScoped:
		source := value.SystemScoped
		if source == nil {
			return nil, errors.New("audit order contains a nil system scope")
		}
		target := &publicauditpb.SystemScopedOrder{}
		if add := source.GetAddEventsSink(); add != nil {
			config, err := connectionconfig.Sink(add.GetConfig())
			if err != nil {
				config = nil
			}
			target.Payload = &publicauditpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &publicauditpb.AddEventsSinkOrder{
				Config: config, ConfigurationUnavailable: err != nil,
			}}
		} else if err := copyOrderPayload(source.ProtoReflect(), target.ProtoReflect()); err != nil {
			return nil, err
		}
		result.Type = &publicauditpb.Order_SystemScoped{SystemScoped: target}
	default:
		return nil, errors.New("audit order has no recognized scope")
	}

	return result, nil
}

// All ordinary order arms share their typed message with the authoritative
// schema. Only the two configuration arms require normalization above. Match
// descriptors explicitly so a new or changed arm cannot silently lose detail.
func copyOrderPayload(source, target protoreflect.Message) error {
	field := source.WhichOneof(source.Descriptor().Oneofs().ByName("payload"))
	if field == nil {
		return errors.New("audit order has no recognized payload")
	}
	destination := target.Descriptor().Fields().ByName(field.Name())
	if destination == nil || destination.Message().FullName() != field.Message().FullName() {
		return fmt.Errorf("audit order %s has no public projection", field.Name())
	}
	target.Set(destination, source.Get(field))

	return nil
}

type auditCursor struct {
	source cursor.Cursor[*auditpb.AuditEntry]
}

// NewAuditCursor preserves the underlying read snapshot lifetime and sequence.
func NewAuditCursor(source cursor.Cursor[*auditpb.AuditEntry]) cursor.Cursor[*publicauditpb.AuditEntry] {
	return &auditCursor{source: source}
}

func (c *auditCursor) Next() (*publicauditpb.AuditEntry, error) {
	entry, err := c.source.Next()
	if err != nil {
		return nil, err
	}

	return Audit(entry)
}

func (c *auditCursor) Close() error { return c.source.Close() }

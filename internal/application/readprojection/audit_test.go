package readprojection

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/publicauditpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestAuditStructuredDetailsPreserveOriginalEvidence(t *testing.T) {
	t.Parallel()
	sink := &commonpb.SinkConfigInput{Name: "events", Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://example.test/hook?token=query-secret", Secret: "webhook-secret"}}}
	mirror := &commonpb.MirrorSourceConfigInput{LedgerName: "source", Type: &commonpb.MirrorSourceConfigInput_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfigInput{Dsn: "postgres://reader:postgres-secret@host/ledger"}}}
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: sink}}}}},
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{MirrorSource: mirror, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}}}}},
	}
	// Even malformed or shadowed opaque signed input is never copied into the view.
	payload := []byte("opaque signed-secret and shadowed-secret")
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	entry := &auditpb.AuditEntry{Sequence: 7, OrderCount: 2, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 12, MaxLogSequence: 13}}, Signature: &signaturepb.SignedApplyBatch{KeyId: "client", Signature: ed25519.Sign(key, payload), Payload: payload}}
	for i, order := range orders {
		entry.Items = append(entry.Items, &auditpb.AuditItem{OrderIndex: uint32(i), LogSequence: uint64(i + 12), SerializedOrder: processing.MarshalOrderBusinessIntent(order, nil)})
	}
	compute := func(e *auditpb.AuditEntry) []byte {
		header, err := state.BuildHashedHeaderPayload(e)
		require.NoError(t, err)
		chunks := [][]byte{header}
		for _, item := range e.GetItems() {
			chunks = append(chunks, state.BuildPerItemPayload(item))
		}
		_, hash := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, "fixture").Compute(nil, nil, chunks)

		return hash
	}
	entry.Hash = compute(entry)
	before := proto.Clone(entry)
	view, err := Audit(entry)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, entry))
	require.Equal(t, entry.GetHash(), compute(entry))
	require.True(t, ed25519.Verify(key.Public().(ed25519.PublicKey), entry.GetSignature().GetPayload(), entry.GetSignature().GetSignature()))
	require.Equal(t, entry.GetHash(), view.GetHash())
	require.Equal(t, "client", view.GetSignature().GetKeyId())
	require.Equal(t, "events", view.GetItems()[0].GetOrder().GetSystemScoped().GetAddEventsSink().GetConfig().GetName())
	require.Equal(t, "mirror", view.GetItems()[1].GetOrder().GetLedgerScoped().GetLedger())
	require.False(t, view.GetItems()[0].GetOrder().GetSystemScoped().GetAddEventsSink().GetConfigurationUnavailable())
	require.False(t, view.GetItems()[1].GetOrder().GetLedgerScoped().GetCreateLedger().GetConfigurationUnavailable())
	rendered, err := json.Marshal(view)
	require.NoError(t, err)
	wire, err := proto.Marshal(view)
	require.NoError(t, err)
	for _, secret := range []string{"webhook-secret", "query-secret", "postgres-secret", "signed-secret", "shadowed-secret"} {
		require.NotContains(t, string(rendered), secret)
		require.NotContains(t, string(wire), secret)
	}
	require.Contains(t, string(rendered), `"orderIndex":0`)
	require.Contains(t, string(rendered), `"order":`)
	require.NotContains(t, string(rendered), "serializedOrder")
	require.NotContains(t, string(rendered), `"payload"`)
}

func TestAuditEveryKnownOrderHasTypedPublicDetails(t *testing.T) {
	t.Parallel()
	for _, ledgerScoped := range []bool{false, true} {
		var source proto.Message = &raftcmdpb.SystemScopedOrder{}
		if ledgerScoped {
			source = &raftcmdpb.LedgerScopedOrder{Ledger: "main"}
		}
		fields := source.ProtoReflect().Descriptor().Oneofs().ByName("payload").Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			source.ProtoReflect().Set(field, source.ProtoReflect().NewField(field))
			order := &raftcmdpb.Order{}
			if ledgerScoped {
				order.Type = &raftcmdpb.Order_LedgerScoped{LedgerScoped: source.(*raftcmdpb.LedgerScopedOrder)}
			} else {
				order.Type = &raftcmdpb.Order_SystemScoped{SystemScoped: source.(*raftcmdpb.SystemScopedOrder)}
			}
			view, err := publicOrder(order)
			require.NoError(t, err, "order %s", field.Name())
			var scope proto.Message = view.GetSystemScoped()
			if ledgerScoped {
				scope = view.GetLedgerScoped()
				require.Equal(t, "main", view.GetLedgerScoped().GetLedger())
			}
			destination := scope.ProtoReflect().WhichOneof(scope.ProtoReflect().Descriptor().Oneofs().ByName("payload"))
			require.Equal(t, field.Name(), destination.Name())
		}
	}
}

func TestAuditShadowedWireCredentialsHaveNoPublicCarrier(t *testing.T) {
	t.Parallel()
	fieldBytes := func(message proto.Message, name protoreflect.Name, payload []byte) []byte {
		field := message.ProtoReflect().Descriptor().Fields().ByName(name)

		return protowire.AppendBytes(protowire.AppendTag(nil, field.Number(), protowire.BytesType), payload)
	}
	http := &commonpb.HttpSinkConfigInput{Endpoint: "https://host/events", Secret: "shadowed-scalar-secret"}
	httpWire, err := http.MarshalVT()
	require.NoError(t, err)
	httpWire = append(httpWire, fieldBytes(http, "secret", nil)...)
	config := fieldBytes(&commonpb.SinkConfigInput{}, "http", httpWire)
	add := fieldBytes(&raftcmdpb.AddEventsSinkOrder{}, "config", config)
	system := fieldBytes(&raftcmdpb.SystemScopedOrder{}, "add_events_sink", add)
	orderWire := fieldBytes(&raftcmdpb.Order{}, "system_scoped", system)
	other := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main", Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}}}}}
	otherWire, err := other.MarshalVT()
	require.NoError(t, err)
	for _, raw := range [][]byte{orderWire, append(bytes.Clone(orderWire), otherWire...)} {
		original := bytes.Clone(raw)
		entry := &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}, Items: []*auditpb.AuditItem{{SerializedOrder: raw}}}
		view, err := Audit(entry)
		require.NoError(t, err)
		encoded, err := view.MarshalVT()
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "shadowed-scalar-secret")
		encoded, err = json.Marshal(view)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "shadowed-scalar-secret")
		require.Equal(t, original, entry.GetItems()[0].GetSerializedOrder())
	}
}

func TestAuditInvalidConfigurationRemainsReadable(t *testing.T) {
	t.Parallel()
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: &commonpb.SinkConfigInput{Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://user:%xx@host"}}}}}}}},
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{MirrorSource: &commonpb.MirrorSourceConfigInput{Type: &commonpb.MirrorSourceConfigInput_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfigInput{Dsn: "password='unterminated"}}}}}}}},
	}
	for _, order := range orders {
		raw, err := order.MarshalVT()
		require.NoError(t, err)
		view, err := Audit(&auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}, Items: []*auditpb.AuditItem{{SerializedOrder: raw}}})
		require.NoError(t, err)
		if add := view.GetItems()[0].GetOrder().GetSystemScoped().GetAddEventsSink(); add != nil {
			require.True(t, add.GetConfigurationUnavailable())
			require.Nil(t, add.GetConfig())
		}
		if create := view.GetItems()[0].GetOrder().GetLedgerScoped().GetCreateLedger(); create != nil {
			require.True(t, create.GetConfigurationUnavailable())
			require.Nil(t, create.GetMirrorSource())
			require.Equal(t, "mirror", view.GetItems()[0].GetOrder().GetLedgerScoped().GetLedger())
		}
	}
}

func TestAuditSchemaCannotCarryOpaqueEvidenceOrRawConfiguration(t *testing.T) {
	t.Parallel()
	seen := map[protoreflect.FullName]bool{}
	var walk func(protoreflect.MessageDescriptor)
	walk = func(desc protoreflect.MessageDescriptor) {
		if seen[desc.FullName()] {
			return
		}
		seen[desc.FullName()] = true
		for _, forbidden := range []protoreflect.FullName{"audit.AuditEntry", "audit.AuditItem", "signature.SignedApplyBatch", "signature.SignedLog", "raft.OrderTechnical", "common.SinkConfigInput", "common.MirrorSourceConfigInput"} {
			require.NotEqual(t, forbidden, desc.FullName())
		}
		for i := range desc.Fields().Len() {
			f := desc.Fields().Get(i)
			require.NotEqual(t, protoreflect.Name("serialized_order"), f.Name())
			if f.Message() != nil {
				walk(f.Message())
			}
		}
	}
	walk((&publicauditpb.AuditEntry{}).ProtoReflect().Descriptor())
}

func TestAuditUnknownFieldsAndTechnicalDataAreOmitted(t *testing.T) {
	t.Parallel()
	remove := &raftcmdpb.RemoveEventsSinkOrder{Name: "events"}
	unknown := protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), []byte("unknown-secret"))
	remove.ProtoReflect().SetUnknown(unknown)
	order := &raftcmdpb.Order{Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte("technical-secret")}, Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{RemoveEventsSink: remove}}}}
	raw, err := order.MarshalVT()
	require.NoError(t, err)
	entry := &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}, Items: []*auditpb.AuditItem{{SerializedOrder: raw}}}
	before := proto.Clone(entry)
	view, err := Audit(entry)
	require.NoError(t, err)
	wire, err := proto.Marshal(view)
	require.NoError(t, err)
	require.NotContains(t, string(wire), "unknown-secret")
	require.NotContains(t, string(wire), "technical-secret")
	require.True(t, proto.Equal(before, entry))
	require.Equal(t, "events", view.GetItems()[0].GetOrder().GetSystemScoped().GetRemoveEventsSink().GetName())
}

func TestAuditCursorAndMalformedOrders(t *testing.T) {
	t.Parallel()
	c := NewAuditCursor(cursor.NewSliceCursor([]*auditpb.AuditEntry{{Sequence: 9, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}}, {Sequence: 10, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}}}))
	first, err := c.Next()
	require.NoError(t, err)
	require.EqualValues(t, 9, first.GetSequence())
	second, err := c.Next()
	require.NoError(t, err)
	require.EqualValues(t, 10, second.GetSequence())
	_, err = c.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, c.Close())
	_, err = Audit(&auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}, Items: []*auditpb.AuditItem{{SerializedOrder: []byte{0xff}}}})
	require.ErrorContains(t, err, "decoding audit order")
	nilView, err := Audit(nil)
	require.EqualError(t, err, "audit entry is nil")
	require.Nil(t, nilView)
}

func TestAuditFailureRetainsReasonWithoutHistoricalDiagnostics(t *testing.T) {
	t.Parallel()
	entry := &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{
		Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
		Message: "connection failed for https://user:historical-secret@host",
		Context: map[string]string{"details": "historical-context-secret"},
	}}}
	original := proto.Clone(entry)
	view, err := Audit(entry)
	require.NoError(t, err)
	require.Equal(t, entry.GetFailure().GetReason(), view.GetFailure().GetReason())
	require.Equal(t, "[redacted]", view.GetFailure().GetMessage())
	require.Empty(t, view.GetFailure().GetContext())
	data, err := json.Marshal(view)
	require.NoError(t, err)
	require.NotContains(t, string(data), "historical-secret")
	require.NotContains(t, string(data), "historical-context-secret")
	require.True(t, proto.Equal(original, entry))
}

func TestAuditRejectsMissingOutcome(t *testing.T) {
	t.Parallel()
	for name, entry := range map[string]*auditpb.AuditEntry{
		"missing":             {},
		"nil success wrapper": {Outcome: (*auditpb.AuditEntry_Success)(nil)},
		"nil success payload": {Outcome: &auditpb.AuditEntry_Success{}},
		"nil failure wrapper": {Outcome: (*auditpb.AuditEntry_Failure)(nil)},
		"nil failure payload": {Outcome: &auditpb.AuditEntry_Failure{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var view *publicauditpb.AuditEntry
			var err error
			require.NotPanics(t, func() { view, err = Audit(entry) })
			require.ErrorContains(t, err, "outcome")
			require.Nil(t, view)
		})
	}
}

func TestAuditCursorErrorsIdentifyEntryAndOrder(t *testing.T) {
	t.Parallel()
	for name, data := range map[string][]byte{"undecodable": {0xff}, "missing scope": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := NewAuditCursor(cursor.NewSliceCursor([]*auditpb.AuditEntry{{
				Sequence: 42, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
				Items: []*auditpb.AuditItem{{OrderIndex: 7, SerializedOrder: data}},
			}}))
			_, err := c.Next()
			require.ErrorContains(t, err, "audit entry 42")
			require.ErrorContains(t, err, "order 7")
			require.NoError(t, c.Close())
		})
	}
}

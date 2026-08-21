package servicepb_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestHistoricalBalanceProtoContract protects the additive wire contract.
func TestHistoricalBalanceProtoContract(t *testing.T) {
	t.Parallel()

	assertField := func(
		message protoreflect.MessageDescriptor,
		name protoreflect.Name,
		number protoreflect.FieldNumber,
		kind protoreflect.Kind,
	) protoreflect.FieldDescriptor {
		t.Helper()

		field := message.Fields().ByName(name)
		require.NotNil(t, field, "missing %s.%s", message.FullName(), name)
		require.Equal(t, number, field.Number(), "%s.%s number", message.FullName(), name)
		require.Equal(t, kind, field.Kind(), "%s.%s kind", message.FullName(), name)

		return field
	}

	request := (&servicepb.AggregateVolumesRequest{}).ProtoReflect().Descriptor()
	assertField(request, "ledger", 1, protoreflect.StringKind)
	assertField(request, "filter", 2, protoreflect.MessageKind)
	assertField(request, "min_log_sequence", 3, protoreflect.Fixed64Kind)
	assertField(request, "use_max_precision", 4, protoreflect.BoolKind)
	groupByPrefixes := assertField(request, "group_by_prefixes", 5, protoreflect.StringKind)
	require.True(t, groupByPrefixes.IsList())
	assertField(request, "checkpoint_id", 6, protoreflect.Fixed64Kind)
	assertField(request, "collapse_colors", 7, protoreflect.BoolKind)
	historicalBalance := assertField(request, "historical_balance", 8, protoreflect.MessageKind)
	require.Equal(t, protoreflect.FullName("ledger.HistoricalBalanceSelector"), historicalBalance.Message().FullName())
	require.Equal(t, "historicalBalance", historicalBalance.JSONName())

	selector := (&servicepb.HistoricalBalanceSelector{}).ProtoReflect().Descriptor()
	at := assertField(selector, "at", 1, protoreflect.MessageKind)
	require.Equal(t, protoreflect.FullName("common.Timestamp"), at.Message().FullName())
	temporality := assertField(selector, "temporality", 2, protoreflect.EnumKind)
	require.Equal(t, protoreflect.FullName("ledger.HistoricalBalanceTemporality"), temporality.Enum().FullName())

	view := (&servicepb.HistoricalBalanceView{}).ProtoReflect().Descriptor()
	requestedAt := assertField(view, "requested_at", 1, protoreflect.MessageKind)
	require.Equal(t, protoreflect.FullName("common.Timestamp"), requestedAt.Message().FullName())
	assertField(view, "temporality", 2, protoreflect.EnumKind)
	assertField(view, "ledger", 3, protoreflect.StringKind)
	assertField(view, "audit_watermark", 4, protoreflect.Fixed64Kind)
	assertField(view, "log_watermark", 5, protoreflect.Fixed64Kind)
	assertField(view, "manifest_version", 6, protoreflect.Fixed64Kind)
	assertField(view, "view_token", 7, protoreflect.StringKind)

	temporalityDescriptor := servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE.Descriptor()
	require.Equal(
		t,
		protoreflect.EnumNumber(0),
		temporalityDescriptor.Values().ByName("HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE").Number(),
	)
	require.Equal(
		t,
		protoreflect.EnumNumber(1),
		temporalityDescriptor.Values().ByName("HISTORICAL_BALANCE_TEMPORALITY_INSERTION").Number(),
	)
}

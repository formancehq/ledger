package http

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// The DTOs in this package replaced protojson with hand-written type switches
// over proto oneofs. protojson was descriptor-driven, so a new oneof variant
// rendered automatically; a type switch does not, and none of the three
// switches here has anything tying it back to the descriptor:
//
//	newSinkConfigDTO        SinkConfig.type          (dto_sinks.go)
//	newDatabricksSinkDTO    DatabricksSinkConfig.auth
//	newIndexIDDTO           IndexID.kind             (dto_indexes.go)
//
// Adding an arm to any of those oneofs compiles cleanly and passes the existing
// per-variant table tests, because those enumerate variants by hand rather than
// from the descriptor. openapi.yml would not catch it either: SinkConfig lists
// no discriminator, no minProperties and no oneOf, so a kind-less sink object
// is schema-valid. `exhaustive` is opt-in in .golangci.yaml and every tagged
// site is an enum switch, and scripts/check-repo-invariants.go does not look at
// dto_*.go. The visible result would be GET /_/events-sinks reporting a
// configured sink as having no kind at all, indistinguishable from the
// genuinely kind-less state — invariant #7, an impossible state served as if it
// were real.
//
// These tests close that gap. They are descriptor-driven rather than
// list-driven, so unlike the wrapper_exhaustiveness_test.go /
// request_scope_exhaustiveness_test.go precedents there is no hand-maintained
// set of names to keep in sync: each variant is populated reflectively, pushed
// through the real converter, and the rendered JSON must carry it.

// setOneofField populates f on m with a value that is observable after
// conversion: a fresh submessage for a message field, a non-zero scalar
// otherwise. The DTO branches test for a non-nil submessage, so an empty
// message is enough to make the arm fire.
func setOneofField(t *testing.T, m protoreflect.Message, f protoreflect.FieldDescriptor) {
	t.Helper()

	switch f.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		m.Set(f, protoreflect.ValueOfMessage(m.NewField(f).Message()))
	case protoreflect.StringKind:
		m.Set(f, protoreflect.ValueOfString("set"))
	case protoreflect.EnumKind:
		m.Set(f, protoreflect.ValueOfEnum(0))
	default:
		t.Fatalf("oneof field %s has kind %s, which this helper cannot populate — extend setOneofField", f.FullName(), f.Kind())
	}
}

// oneofFields returns the fields of the single oneof named `name` on msg,
// failing if the message does not have exactly that one oneof. The count
// assertion is deliberate: a second oneof appearing on one of these messages
// is itself a hand-dispatch surface that needs its own gate.
func oneofFields(t *testing.T, msg protoreflect.ProtoMessage, name string) protoreflect.FieldDescriptors {
	t.Helper()

	desc := msg.ProtoReflect().Descriptor()
	oneofs := desc.Oneofs()
	require.Equal(t, 1, oneofs.Len(), "%s must have exactly one oneof", desc.FullName())

	oneof := oneofs.Get(0)
	require.Equal(t, name, string(oneof.Name()), "%s's oneof was renamed", desc.FullName())

	fields := oneof.Fields()
	require.Positive(t, fields.Len())

	return fields
}

// TestNewSinkConfigDTO_OneofExhaustive asserts every SinkConfig.type variant
// the descriptor declares has a branch in newSinkConfigDTO that renders it.
func TestNewSinkConfigDTO_OneofExhaustive(t *testing.T) {
	t.Parallel()

	fields := oneofFields(t, &commonpb.SinkConfig{}, "type")

	for i := range fields.Len() {
		f := fields.Get(i)

		t.Run(string(f.Name()), func(t *testing.T) {
			t.Parallel()

			cfg := &commonpb.SinkConfig{Name: "s"}
			setOneofField(t, cfg.ProtoReflect(), f)

			raw, err := json.Marshal(newSinkConfigDTO(cfg))
			require.NoError(t, err)

			require.Contains(t, string(raw), `"`+f.JSONName()+`"`,
				"SinkConfig.type.%s is declared in the proto but newSinkConfigDTO does not render it: "+
					"the response would report a configured sink as having no kind. Add a case to the "+
					"type switch in dto_sinks.go, a %s field to sinkConfigDTO, and the schema to openapi.yml.",
				f.Name(), f.JSONName())
		})
	}
}

// TestNewDatabricksSinkDTO_AuthOneofExhaustive asserts every
// DatabricksSinkConfig.auth variant sets the authMethod discriminator. The
// variant itself is not asserted on the wire: the PAT is entirely secret and
// deliberately has no DTO field, so authMethod is the only observable.
func TestNewDatabricksSinkDTO_AuthOneofExhaustive(t *testing.T) {
	t.Parallel()

	fields := oneofFields(t, &commonpb.DatabricksSinkConfig{}, "auth")

	for i := range fields.Len() {
		f := fields.Get(i)

		t.Run(string(f.Name()), func(t *testing.T) {
			t.Parallel()

			cfg := &commonpb.DatabricksSinkConfig{ServerHostname: "h"}
			setOneofField(t, cfg.ProtoReflect(), f)

			dto := newDatabricksSinkDTO(cfg)
			require.NotNil(t, dto)

			require.NotEmpty(t, dto.AuthMethod,
				"DatabricksSinkConfig.auth.%s is declared in the proto but newDatabricksSinkDTO leaves "+
					"authMethod empty, so the response reports a sink with credentials configured as "+
					"having none. Add a case to the switch in dto_sinks.go and the value to the "+
					"authMethod enum in openapi.yml.", f.Name())
		})
	}
}

// TestNewIndexIDDTO_KindOneofExhaustive asserts every IndexID.kind variant is
// rendered. Unlike the sinks case this one degrades in-band today (canonicalId
// still reports "unknown"), but a silently empty id object is still wrong.
func TestNewIndexIDDTO_KindOneofExhaustive(t *testing.T) {
	t.Parallel()

	fields := oneofFields(t, &commonpb.IndexID{}, "kind")

	for i := range fields.Len() {
		f := fields.Get(i)

		t.Run(string(f.Name()), func(t *testing.T) {
			t.Parallel()

			id := &commonpb.IndexID{}
			setOneofField(t, id.ProtoReflect(), f)

			raw, err := json.Marshal(newIndexIDDTO(id))
			require.NoError(t, err)

			require.Contains(t, string(raw), `"`+f.JSONName()+`"`,
				"IndexID.kind.%s is declared in the proto but newIndexIDDTO does not render it: the "+
					"id object would be emitted as {}. Add a case to the switch in dto_indexes.go, a %s "+
					"field to indexIDDTO, and the schema to openapi.yml.", f.Name(), f.JSONName())
		})
	}
}

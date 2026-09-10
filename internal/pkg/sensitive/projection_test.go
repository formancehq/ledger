package sensitive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestClonePreservesSourceAndDropsOpaqueEvidence(t *testing.T) {
	t.Parallel()
	source := &commonpb.Log{Sequence: 17, ResponseSignature: &signaturepb.SignedLog{KeyId: "server", Payload: []byte("secret"), Signature: []byte("signature")}}
	source.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 1000, protowire.BytesType), []byte("unknown secret")))
	before := proto.Clone(source)
	view := Clone(source)
	require.Equal(t, uint64(17), view.GetSequence())
	require.Nil(t, view.GetResponseSignature())
	require.Empty(t, view.ProtoReflect().GetUnknown())
	require.True(t, proto.Equal(before, source))
	require.NotSame(t, source, view)
	var absent *commonpb.Log
	require.Nil(t, Clone(absent))
	require.Nil(t, Clone[proto.Message](nil))
}

func TestCloneRecursiveDescriptors(t *testing.T) {
	t.Parallel()
	marked := &descriptorpb.FieldOptions{}
	proto.SetExtension(marked, commonpb.E_Sensitive, true)
	field := func(name string, n int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: new(name), Number: new(n), Type: kind.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()}
	}
	password := field("password", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING)
	password.Options = marked
	child := &descriptorpb.DescriptorProto{Name: new("Child"), Field: []*descriptorpb.FieldDescriptorProto{password, field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING)}}
	single := field("single", 1, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	single.TypeName = new(".test.Child")
	list := field("list", 2, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	list.TypeName = single.TypeName
	list.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	entry := &descriptorpb.DescriptorProto{Name: new("ByNameEntry"), Options: &descriptorpb.MessageOptions{MapEntry: new(true)}, Field: []*descriptorpb.FieldDescriptorProto{field("key", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING), field("value", 2, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)}}
	entry.Field[1].TypeName = single.TypeName
	mapping := field("by_name", 3, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	mapping.TypeName = new(".test.Parent.ByNameEntry")
	mapping.Label = list.Label
	bytes := field("opaque", 4, descriptorpb.FieldDescriptorProto_TYPE_BYTES)
	bytes.Options = marked
	parent := &descriptorpb.DescriptorProto{Name: new("Parent"), NestedType: []*descriptorpb.DescriptorProto{entry}, Field: []*descriptorpb.FieldDescriptorProto{single, list, mapping, bytes}}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: new("projection_test.proto"), Package: new("test"), Syntax: new("proto3"), MessageType: []*descriptorpb.DescriptorProto{child, parent}}, nil)
	require.NoError(t, err)
	desc := file.Messages().ByName("Parent")
	source := dynamicpb.NewMessage(desc)
	makeChild := func() protoreflect.Value {
		m := dynamicpb.NewMessage(file.Messages().ByName("Child"))
		m.Set(m.Descriptor().Fields().ByName("password"), protoreflect.ValueOfString("credential"))
		m.Set(m.Descriptor().Fields().ByName("name"), protoreflect.ValueOfString("visible"))

		return protoreflect.ValueOfMessage(m)
	}
	source.Set(desc.Fields().ByName("single"), makeChild())
	source.Mutable(desc.Fields().ByName("list")).List().Append(makeChild())
	source.Mutable(desc.Fields().ByName("by_name")).Map().Set(protoreflect.ValueOfString("server").MapKey(), makeChild())
	source.Set(desc.Fields().ByName("opaque"), protoreflect.ValueOfBytes([]byte("credential")))
	before := proto.Clone(source)
	view := Clone(source)
	require.True(t, proto.Equal(before, source))
	require.False(t, view.Has(desc.Fields().ByName("opaque")))
	assertChild := func(v protoreflect.Value) {
		m := v.Message()
		require.Equal(t, Marker, m.Get(m.Descriptor().Fields().ByName("password")).String())
		require.Equal(t, "visible", m.Get(m.Descriptor().Fields().ByName("name")).String())
	}
	assertChild(view.Get(desc.Fields().ByName("single")))
	assertChild(view.Get(desc.Fields().ByName("list")).List().Get(0))
	assertChild(view.Get(desc.Fields().ByName("by_name")).Map().Get(protoreflect.ValueOfString("server").MapKey()))
}

func TestClonePreservesSanitizedSinkDiagnostic(t *testing.T) {
	t.Parallel()
	source := &commonpb.SinkError{Message: "sending request to webhook.example: connection refused"}
	view := Clone(source)
	require.Equal(t, source.GetMessage(), view.GetMessage())
	require.NotSame(t, source, view)
}

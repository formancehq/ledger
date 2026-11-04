package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// Exercise the executable protocol as protoc does, including errors in responses.
func TestReaderPluginProcess(t *testing.T) {
	if os.Getenv("READER_PLUGIN_TEST_PROCESS") == "1" {
		os.Args = os.Args[:1]
		main()
		os.Exit(0)
	}
}

func invokePlugin(t *testing.T, req *pluginpb.CodeGeneratorRequest) *pluginpb.CodeGeneratorResponse {
	t.Helper()
	input, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestReaderPluginProcess$")
	cmd.Env = append(os.Environ(), "READER_PLUGIN_TEST_PROCESS=1")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.Output()
	if err != nil {
		if exit, ok := errors.AsType[*exec.ExitError](err); ok {
			t.Fatalf("plugin: %v: %s", err, exit.Stderr)
		}
		t.Fatal(err)
	}
	response := new(pluginpb.CodeGeneratorResponse)
	if err := proto.Unmarshal(output, response); err != nil {
		t.Fatal(err)
	}

	return response
}

func fixtureField(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type, typeName string, group *int32) *descriptorpb.FieldDescriptorProto {
	field := &descriptorpb.FieldDescriptorProto{Name: new(name), Number: new(number), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: kind.Enum(), OneofIndex: group}
	if typeName != "" {
		field.TypeName = new(typeName)
	}

	return field
}

func fixtureRequest() *pluginpb.CodeGeneratorRequest {
	dep := &descriptorpb.FileDescriptorProto{Name: new("dep.proto"), Package: new("dep"), Syntax: new("proto3"), Options: &descriptorpb.FileOptions{GoPackage: new("example.test/fixture/dep")}, MessageType: []*descriptorpb.DescriptorProto{{Name: new("Child"), Field: []*descriptorpb.FieldDescriptorProto{fixtureField("value", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, "", nil)}}}}
	root := &descriptorpb.FileDescriptorProto{Name: new("sample.proto"), Package: new("sample"), Syntax: new("proto3"), Dependency: []string{"dep.proto"}, Options: &descriptorpb.FileOptions{GoPackage: new("example.test/fixture/sample")}, MessageType: []*descriptorpb.DescriptorProto{{Name: new("Envelope"), OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: new("payload")}, {Name: new("_note")}}, Field: []*descriptorpb.FieldDescriptorProto{
		fixtureField("child", 3, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".dep.Child", new(int32(0))),
		fixtureField("raw", 17, descriptorpb.FieldDescriptorProto_TYPE_BYTES, "", new(int32(0))),
		fixtureField("count", 29, descriptorpb.FieldDescriptorProto_TYPE_INT64, "", new(int32(0))),
		fixtureField("nested", 31, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".sample.Envelope.Nested", new(int32(0))),
		fixtureField("note", 42, descriptorpb.FieldDescriptorProto_TYPE_STRING, "", new(int32(1))),
	}, NestedType: []*descriptorpb.DescriptorProto{{Name: new("Nested"), OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: new("choice")}}, Field: []*descriptorpb.FieldDescriptorProto{fixtureField("enabled", 7, descriptorpb.FieldDescriptorProto_TYPE_BOOL, "", new(int32(0)))}}}}}}
	root.GetMessageType()[0].GetField()[4].Proto3Optional = new(true)

	return &pluginpb.CodeGeneratorRequest{ProtoFile: []*descriptorpb.FileDescriptorProto{dep, root}, FileToGenerate: []string{"dep.proto", "sample.proto"}, Parameter: new("module=example.test/fixture")}
}

func TestGeneratedOneofReaders(t *testing.T) {
	t.Parallel()
	req := fixtureRequest()
	response := invokePlugin(t, req)
	if response.GetError() != "" {
		t.Fatal(response.GetError())
	}
	if response.GetSupportedFeatures()&uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL) == 0 {
		t.Fatal("plugin does not advertise optional support")
	}
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range response.GetFile() {
		if strings.Contains(file.GetContent(), "WhichNote()") || strings.Contains(file.GetContent(), "NoteReaderCase") {
			t.Fatal("synthetic optional field exposed as a oneof")
		}
		write(file.GetName(), file.GetContent())
	}
	// Generate actual protoc Go types from the same descriptors, rather than a
	// handwritten approximation of oneof wrappers or nil/default getter behavior.
	protocGo := filepath.Join(dir, "protoc-gen-go")
	build := exec.Command("go", "build", "-o", protocGo, "google.golang.org/protobuf/cmd/protoc-gen-go")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build protoc-gen-go: %v\n%s", err, output)
	}
	input, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(protocGo)
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	pbResponse := new(pluginpb.CodeGeneratorResponse)
	if err := proto.Unmarshal(output, pbResponse); err != nil {
		t.Fatal(err)
	}
	if pbResponse.GetError() != "" {
		t.Fatal(pbResponse.GetError())
	}
	for _, file := range pbResponse.GetFile() {
		write(file.GetName(), file.GetContent())
	}
	write("go.mod", "module example.test/fixture\n\ngo 1.26.0\n\nrequire google.golang.org/protobuf v1.36.11\n")
	sums, err := os.ReadFile("go.sum")
	if err != nil {
		t.Fatal(err)
	}
	write("go.sum", string(sums))
	write("dep/clone.go", `package dep
import "google.golang.org/protobuf/proto"
func (m *Child) CloneVT() *Child { if m == nil { return nil }; return proto.Clone(m).(*Child) }
`)
	write("sample/clone.go", `package sample
import "google.golang.org/protobuf/proto"
func (m *Envelope) CloneVT() *Envelope { if m == nil { return nil }; return proto.Clone(m).(*Envelope) }
func (m *Envelope_Nested) CloneVT() *Envelope_Nested { if m == nil { return nil }; return proto.Clone(m).(*Envelope_Nested) }
`)
	write("sample/reader_test.go", generatedReaderTests)
	run := exec.Command("go", "test", "-race", "./...")
	run.Dir = dir
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("generated reader compile/runtime: %v\n%s", err, output)
	}
}

func TestOneofDeclarationCollisions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, declaration string
		unsetArm          bool
	}{
		{"nested case type", "PayloadReaderCase", false},
		{"nested case constant", "PayloadReaderCase_Child", false},
		{"nested unset constant", "PayloadReaderCase_Unset", false},
		{"unset arm", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := fixtureRequest()
			message := req.GetProtoFile()[1].GetMessageType()[0]
			if tc.unsetArm {
				message.GetField()[2].Name = new("unset")
			} else {
				message.NestedType = append(message.GetNestedType(), &descriptorpb.DescriptorProto{Name: new(tc.declaration)})
			}
			response := invokePlugin(t, req)
			if !strings.Contains(response.GetError(), "oneof reader declaration Envelope_PayloadReaderCase") || !strings.Contains(response.GetError(), "conflicts") {
				t.Fatalf("expected actionable collision error, got %q", response.GetError())
			}
			if len(response.GetFile()) != 0 {
				t.Fatal("collision produced partial generated files")
			}
		})
	}
}

const generatedReaderTests = `package sample
import (
 "testing"
 "example.test/fixture/dep"
)
func TestReaders(t *testing.T) {
 t.Parallel()
 m := &Envelope{}
 r := m.AsReader()
 if r.WhichPayload() != Envelope_PayloadReaderCase_Unset || r.GetChild() != nil || r.GetRaw() != nil || r.GetCount() != 0 || r.GetNote() != "" { t.Fatal("unset defaults") }
 m.Payload = &Envelope_Count{Count: 0}
 if r.WhichPayload() != 29 || r.GetCount() != 0 { t.Fatal("selected zero identity") }
 m.Payload = (*Envelope_Count)(nil)
 if r.WhichPayload() != 29 || r.GetCount() != 0 { t.Fatal("typed nil scalar") }
 m.Payload = &Envelope_Child{}
 if r.WhichPayload() != 3 || r.GetChild() != nil { t.Fatal("selected nil message") }
 m.Payload = (*Envelope_Child)(nil)
 if r.WhichPayload() != 3 || r.GetChild() != nil { t.Fatal("typed nil message") }
 m.Payload = (*Envelope_Raw)(nil)
 if r.WhichPayload() != 17 || r.GetRaw() != nil { t.Fatal("typed nil bytes") }
 m.Payload = &Envelope_Raw{Raw: []byte("safe")}
 got := r.GetRaw(); got[0] = 'X'
 if string(r.GetRaw()) != "safe" { t.Fatal("bytes escaped") }
 m.Payload = &Envelope_Child{Child: &dep.Child{Value: "original"}}
 child := r.GetChild()
 child.Mutate().Value = "changed"
 clone := r.Mutate(); clone.GetChild().Value = "changed"
 if child.GetValue() != "original" || m.GetChild().Value != "original" { t.Fatal("clone isolation") }
 m.Note = new(string); *m.Note = "optional"
 if r.GetNote() != "optional" { t.Fatal("synthetic optional getter") }
 m.Payload = &Envelope_Nested_{Nested: &Envelope_Nested{Choice: &Envelope_Nested_Enabled{Enabled: false}}}
 if r.WhichPayload() != 31 || r.GetNested().WhichChoice() != 7 || r.GetNested().GetEnabled() { t.Fatal("nested discriminator") }
 var absent *Envelope
 if absent.AsReader() != nil { t.Fatal("nil receiver") }
 if _, ok := any(r).(interface{ GetPayload() isEnvelope_Payload }); ok { t.Fatal("mutable oneof leaked") }
}
var readerSink dep.ChildReader
func TestReaderAllocations(t *testing.T) {
 m := &Envelope{Payload: &Envelope_Child{Child: &dep.Child{Value: "value"}}}
 if allocations := testing.AllocsPerRun(100, func() { r := m.AsReader(); readerSink = r.GetChild(); if r.WhichPayload() != 3 { panic("case") } }); allocations != 0 { t.Fatalf("reader chain allocated %v times", allocations) }
}
`

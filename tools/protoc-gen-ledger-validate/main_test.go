package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratorRejectsInvalidAnnotations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	plugin := filepath.Join(dir, "protoc-gen-ledger-validate")
	build := exec.Command("go", "build", "-o", plugin, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %v: %s", err, output)
	}
	definitions := `syntax = "proto3";
package common;
option go_package = "example/commonpb";
import "google/protobuf/descriptor.proto";
extend google.protobuf.FieldOptions { bool required_for_input = 71006; bool non_zero_for_input = 71007; }
enum Mode { MODE_UNSPECIFIED = 0; MODE_ACTIVE = 1; }
message Sample { FIELD }
`
	for _, tc := range []struct{ name, field, want string }{
		{"plain scalar", `string value = 1 [(common.required_for_input) = true];`, "explicit presence"},
		{"repeated", `repeated string value = 1 [(common.required_for_input) = true];`, "singular field"},
		{"false", `optional string value = 1 [(common.required_for_input) = false];`, "remove the annotation"},
		{"oneof", `oneof choice { string value = 1 [(common.required_for_input) = true]; }`, "outside a oneof"},
		{"method collision", `optional string value = 1 [(common.required_for_input) = true]; string validate_input = 2;`, "collides with generated ValidateInput"},
		{"nonzero message", `google.protobuf.FieldOptions value = 1 [(common.non_zero_for_input) = true];`, "requires a scalar field"},
		{"nonzero repeated", `repeated string value = 1 [(common.non_zero_for_input) = true];`, "singular field"},
		{"nonzero false", `string value = 1 [(common.non_zero_for_input) = false];`, "remove the annotation"},
		{"conflicting rules", `optional string value = 1 [(common.required_for_input) = true, (common.non_zero_for_input) = true];`, "cannot be combined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+".proto")
			if err := os.WriteFile(fixture, []byte(strings.Replace(definitions, "FIELD", tc.field, 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("protoc", "-I", dir, "--plugin=protoc-gen-ledger-validate="+plugin, "--ledger-validate_out="+dir, fixture)
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), tc.want) || !strings.Contains(string(output), "common.Sample.") {
				t.Fatalf("expected %q for named field, got %v: %s", tc.want, err, output)
			}
		})
	}
	fixture := filepath.Join(dir, "positive.proto")
	fields := `string name = 1 [(common.non_zero_for_input) = true];
bytes key = 2 [(common.non_zero_for_input) = true];
int32 count = 3 [(common.non_zero_for_input) = true];
bool enabled = 4 [(common.non_zero_for_input) = true];
optional int32 limit = 5 [(common.non_zero_for_input) = true];`
	fields += `
Mode mode = 6 [(common.non_zero_for_input) = true];`
	if err := os.WriteFile(fixture, []byte(strings.Replace(definitions, "FIELD", fields, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("protoc", "-I", dir, "--plugin=protoc-gen-ledger-validate="+plugin, "--ledger-validate_out="+dir, fixture)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generate non-zero checks: %v: %s", err, output)
	}
	generated, err := os.ReadFile(filepath.Join(dir, "example", "commonpb", "positive_ledger_validate.pb.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{`m.Name == ""`, `len(m.Key) == 0`, `m.Count == 0`, `!m.Enabled`, `m.Limit == nil || *m.Limit == 0`, `m.Mode == 0`} {
		if !strings.Contains(string(generated), check) {
			t.Errorf("generated validator missing %q: %s", check, generated)
		}
	}
}

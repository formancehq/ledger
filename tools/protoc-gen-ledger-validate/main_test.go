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
option go_package = "github.com/formancehq/ledger/v3/internal/proto/commonpb";
import "google/protobuf/descriptor.proto";
extend google.protobuf.FieldOptions { bool required_for_input = 71006; bool non_zero_for_input = 71007; uint32 max_bytes_for_input = 71008; uint32 exact_bytes_for_input = 71009; string input_error_message = 71010; string input_error_code = 71011; }
enum Mode { MODE_UNSPECIFIED = 0; MODE_ACTIVE = 1; }
message Sample { FIELD }
`
	for _, tc := range []struct{ name, field, want string }{
		{"plain scalar", `string value = 1 [(common.required_for_input) = true];`, "explicit presence"},
		{"repeated", `repeated string value = 1 [(common.required_for_input) = true];`, "singular field"},
		{"false", `optional string value = 1 [(common.required_for_input) = false];`, "remove the annotation"},
		{"oneof", `oneof choice { string value = 1 [(common.required_for_input) = true]; }`, "outside a oneof"},
		{"method collision", `optional string value = 1 [(common.required_for_input) = true, (common.input_error_message) = "value is required", (common.input_error_code) = "VALUE_IS_REQUIRED"]; string validate_input = 2;`, "collides with generated ValidateInput"},
		{"nonzero message", `google.protobuf.FieldOptions value = 1 [(common.non_zero_for_input) = true];`, "requires a scalar field"},
		{"nonzero repeated", `repeated string value = 1 [(common.non_zero_for_input) = true];`, "singular field"},
		{"nonzero false", `string value = 1 [(common.non_zero_for_input) = false];`, "remove the annotation"},
		{"conflicting rules", `optional string value = 1 [(common.required_for_input) = true, (common.non_zero_for_input) = true];`, "cannot be combined"},
		{"zero max", `string value = 1 [(common.max_bytes_for_input) = 0];`, "must be positive"},
		{"zero exact", `bytes value = 1 [(common.exact_bytes_for_input) = 0];`, "must be positive"},
		{"numeric max", `uint64 value = 1 [(common.max_bytes_for_input) = 8];`, "string or bytes"},
		{"numeric exact", `int32 value = 1 [(common.exact_bytes_for_input) = 8];`, "string or bytes"},
		{"conflicting limits", `string value = 1 [(common.max_bytes_for_input) = 8, (common.exact_bytes_for_input) = 8];`, "cannot be combined"},
		{"conflicting presence and limit", `optional string value = 1 [(common.required_for_input) = true, (common.max_bytes_for_input) = 8];`, "cannot be combined"},
		{"conflicting nonzero and limit", `string value = 1 [(common.non_zero_for_input) = true, (common.max_bytes_for_input) = 8];`, "cannot be combined"},
		{"missing public message", `string value = 1 [(common.non_zero_for_input) = true];`, "requires a non-empty input_error_message"},
		{"missing error code", `string value = 1 [(common.non_zero_for_input) = true, (common.input_error_message) = "value is required"];`, "requires a non-empty input_error_code"},
		{"invalid error code", `string value = 1 [(common.non_zero_for_input) = true, (common.input_error_message) = "value is required", (common.input_error_code) = "bad_code"];`, "UPPER_SNAKE_CASE"},
		{"orphan public message", `string value = 1 [(common.input_error_message) = "invalid", (common.input_error_code) = "INVALID"];`, "requires an input validation rule"},
		{"orphan error code", `string value = 1 [(common.input_error_code) = "INVALID"];`, "requires an input validation rule"},
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
	fields := `string name = 1 [(common.non_zero_for_input) = true, (common.input_error_message) = "name is required", (common.input_error_code) = "NAME_REQUIRED"];
bytes key = 2 [(common.non_zero_for_input) = true, (common.input_error_message) = "key is required", (common.input_error_code) = "KEY_IS_REQUIRED"];
int32 count = 3 [(common.non_zero_for_input) = true, (common.input_error_message) = "count is required", (common.input_error_code) = "COUNT_IS_REQUIRED"];
bool enabled = 4 [(common.non_zero_for_input) = true, (common.input_error_message) = "enabled is required", (common.input_error_code) = "ENABLED_IS_REQUIRED"];
optional int32 limit = 5 [(common.non_zero_for_input) = true, (common.input_error_message) = "limit is required", (common.input_error_code) = "LIMIT_IS_REQUIRED"];`
	fields += `
Mode mode = 6 [(common.non_zero_for_input) = true, (common.input_error_message) = "mode is required", (common.input_error_code) = "MODE_IS_REQUIRED"];`
	fields += `
string reference = 7 [(common.max_bytes_for_input) = 4096, (common.input_error_message) = "reference too long", (common.input_error_code) = "REFERENCE_TOO_LONG"];
bytes public_key = 8 [(common.exact_bytes_for_input) = 32, (common.input_error_message) = "invalid key length", (common.input_error_code) = "INVALID_KEY_LENGTH"];`
	fields += `
optional string optional_max = 9 [(common.max_bytes_for_input) = 4, (common.input_error_message) = "optional_max too long", (common.input_error_code) = "OPTIONAL_MAX_TOO_LONG"];
optional string optional_exact = 10 [(common.exact_bytes_for_input) = 3, (common.input_error_message) = "invalid optional_exact length", (common.input_error_code) = "INVALID_OPTIONAL_EXACT_LENGTH"];
optional Mode required_mode = 11 [(common.required_for_input) = true, (common.input_error_message) = "required_mode is required", (common.input_error_code) = "REQUIRED_MODE_IS_REQUIRED"];`
	if err := os.WriteFile(fixture, []byte(strings.Replace(definitions, "FIELD", fields, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("protoc", "-I", dir, "--plugin=protoc-gen-ledger-validate="+plugin, "--go_out="+dir, "--go_opt=module=github.com/formancehq/ledger/v3", "--ledger-validate_out="+dir, "--ledger-validate_opt=module=github.com/formancehq/ledger/v3", fixture)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generate non-zero checks: %v: %s", err, output)
	}
	generated, err := os.ReadFile(filepath.Join(dir, "internal", "proto", "commonpb", "positive_ledger_validate.pb.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{`m.Name == ""`, `len(m.Key) == 0`, `m.Count == 0`, `!m.Enabled`, `m.Limit == nil || *m.Limit == 0`, `m.Mode == 0`, `len(m.Reference) > 4096`, `len(m.PublicKey) != 32`, `len(m.GetOptionalMax()) > 4`, `len(m.GetOptionalExact()) != 3`} {
		if !strings.Contains(string(generated), check) {
			t.Errorf("generated validator missing %q: %s", check, generated)
		}
	}
	for _, check := range []string{
		`Message: "common.Sample", Path: "name", Rule: "non_zero"`,
		`Message: "common.Sample", Path: "reference", Rule: "max_bytes"`,
		`Message: "common.Sample", Path: "public_key", Rule: "exact_bytes"`,
		`Message: "common.Sample", Path: "required_mode", Rule: "required"`,
		`PublicMessage: "name is required"`,
		`PublicMessage: "reference too long"`,
	} {
		if !strings.Contains(string(generated), check) {
			t.Errorf("generated error missing %q: %s", check, generated)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module github.com/formancehq/ledger/v3\n\ngo 1.26.0\n\nrequire google.golang.org/protobuf v1.36.11\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	errorSource, err := os.ReadFile("../../internal/proto/input_validation.go")
	if err != nil {
		t.Fatal(err)
	}
	errorDir := filepath.Join(dir, "internal", "proto")
	if err := os.MkdirAll(errorDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(errorDir, "input_validation.go"), errorSource, 0o600); err != nil {
		t.Fatal(err)
	}
	kindSource, err := os.ReadFile("../../internal/proto/error_kind.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(errorDir, "error_kind.go"), kindSource, 0o600); err != nil {
		t.Fatal(err)
	}
	compile := exec.Command("go", "test", "-mod=mod", "./...")
	compile.Dir = dir
	compile.Env = append(os.Environ(), "GOWORK=off")
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile generated validators: %v: %s", err, output)
	}
}

package antithesis_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLaunchWorkflowRecipeCompatibility(t *testing.T) {
	t.Parallel()
	workflow, err := os.ReadFile("../../.github/workflows/antithesis.yml")
	if err != nil {
		t.Fatal(err)
	}
	stepBody := func(name string) string {
		t.Helper()
		_, step, ok := strings.Cut(string(workflow), "      - name: "+name+"\n")
		if !ok {
			t.Fatalf("missing step %s", name)
		}
		step, _, _ = strings.Cut(step, "\n      - name:")
		_, body, ok := strings.Cut(step, "        run: |\n")
		if !ok {
			t.Fatalf("missing shell body for %s", name)
		}
		var script strings.Builder
		for line := range strings.SplitSeq(strings.TrimSuffix(body, "\n"), "\n") {
			if line == "" {
				continue
			}
			if !strings.HasPrefix(line, "          ") {
				t.Fatalf("unexpected shell line %q", line)
			}
			script.WriteString(strings.TrimPrefix(line, "          ") + "\n")
		}

		return strings.ReplaceAll(script.String(), "${{ steps.image.outputs.tag }}", "fixture-tag")
	}
	validate, launch := stepBody("Validate configuration"), stepBody("Launch Antithesis run")
	for _, modern := range []bool{false, true} {
		for _, templates := range []string{"", "main,model"} {
			name := "legacy/" + templates
			if modern {
				name = "modern/" + templates
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				harness := filepath.Join(dir, "tests", "antithesis")
				if err := os.MkdirAll(harness, 0o755); err != nil {
					t.Fatal(err)
				}
				signature := "duration description"
				recorded := "all"
				if modern {
					signature += " test_templates=''"
					recorded = "{{quote(test_templates)}}"
				}
				recipe := "tag := \"fixture\"\nk8s-launch-minutes " + signature + ":\n    printf '%s\\n' \"{{duration}}\" {{quote(description)}} " + recorded + " > \"$RECORDED_ARGS\"\n    echo '{}'\n"
				if err := os.WriteFile(filepath.Join(harness, "Justfile"), []byte(recipe), 0o600); err != nil {
					t.Fatal(err)
				}
				// Run the real Just argument parser without starting another Nix shell.
				shim := "#!/usr/bin/env bash\nset -euo pipefail\n[[ $1 == develop && $2 == .#ci && $3 == --command ]]\nshift 3\nexec \"$@\"\n"
				if err := os.WriteFile(filepath.Join(dir, "nix"), []byte(shim), 0o700); err != nil {
					t.Fatal(err)
				}
				argsFile := filepath.Join(dir, "args")
				env := []string{"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
					"ANTITHESIS_API_KEY=fixture", "ANTITHESIS_REGISTRY_KEY_JSON=fixture", "ANTITHESIS_REPOSITORY=fixture",
					"ANTITHESIS_REPORT_RECIPIENT=fixture@example.invalid", "ANTITHESIS_TENANT=fixture",
					"RUN_DURATION_MINUTES=20", "RUN_DESCRIPTION=Fixture's run", "TEST_TEMPLATES=" + templates,
					"SOURCE_REF=fixture-ref", "RUNNER_TEMP=" + dir, "GITHUB_OUTPUT=" + filepath.Join(dir, "output"), "RECORDED_ARGS=" + argsFile}
				run := func(body, cwd string) ([]byte, error) {
					cmd := exec.CommandContext(t.Context(), "bash", "-euo", "pipefail", "-c", body)
					cmd.Dir, cmd.Env = cwd, env

					return cmd.CombinedOutput()
				}
				output, err := run(validate, dir)
				if !modern && templates != "" {
					if err == nil || !strings.Contains(string(output), "does not support test_templates") {
						t.Fatalf("expected early compatibility rejection, got %v: %s", err, output)
					}

					return
				}
				if err != nil {
					t.Fatalf("validation failed: %v: %s", err, output)
				}
				if _, err := os.Stat(argsFile); !os.IsNotExist(err) {
					t.Fatalf("validation executed launch: %v", err)
				}
				output, err = run(launch, harness)
				if err != nil {
					t.Fatalf("launch failed: %v: %s", err, output)
				}
				args, err := os.ReadFile(argsFile)
				if err != nil {
					t.Fatal(err)
				}
				want := templates
				if !modern {
					want = "all"
				}
				if string(args) != "20\nFixture's run\n"+want+"\n" {
					t.Fatalf("unexpected launch arguments: %q", args)
				}
			})
		}
	}
}

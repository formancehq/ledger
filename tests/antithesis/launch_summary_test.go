package antithesis_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the actual always-run workflow body, without launching a cloud run.
func TestLaunchSummaryRequiresSuccessfulLaunch(t *testing.T) {
	t.Parallel()
	workflow, err := os.ReadFile("../../.github/workflows/antithesis.yml")
	if err != nil {
		t.Fatal(err)
	}
	_, summary, ok := strings.Cut(string(workflow), "      - name: Summarize launch\n")
	if !ok {
		t.Fatal("launch summary step not found")
	}
	if !strings.Contains(summary, "        if: always()\n") ||
		!strings.Contains(summary, "          LAUNCH_OUTCOME: ${{ steps.launch.outcome }}\n") {
		t.Fatal("summary must always run and receive the launch step outcome")
	}
	_, body, ok := strings.Cut(summary, "        run: |\n")
	if !ok {
		t.Fatal("launch summary shell body not found")
	}
	var script strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
		if !strings.HasPrefix(line, "          ") {
			t.Fatalf("unexpected summary body line: %q", line)
		}
		script.WriteString(strings.TrimPrefix(line, "          ") + "\n")
	}

	for _, outcome := range []string{"success", "failure", "skipped", "cancelled", ""} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			for _, reportURL := range []string{"", "https://example.invalid/report"} {
				output := filepath.Join(t.TempDir(), "summary")
				cmd := exec.Command("bash", "-euo", "pipefail", "-c", script.String())
				cmd.Env = []string{
					"PATH=" + os.Getenv("PATH"), "GITHUB_STEP_SUMMARY=" + output,
					"LAUNCH_OUTCOME=" + outcome, "REPORT_URL=" + reportURL, "RUN_ID=fixture-run",
					"RUN_DESCRIPTION=Fixture", "SOURCE_REF=main", "SOURCE_SHA=fixture-sha",
					"IMAGE_TAG=fixture-tag", "RUN_DURATION_MINUTES=20",
					"ANTITHESIS_REPORT_RECIPIENT=fixture@example.invalid",
					"ANTITHESIS_API_KEY=fixture-secret-api-key",
					"ANTITHESIS_REGISTRY_KEY_JSON=fixture-secret-registry-key",
				}
				combined, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("summary failed: %v\n%s", err, combined)
				}
				data, err := os.ReadFile(output)
				if err != nil {
					t.Fatal(err)
				}
				text := string(data)
				wantOutcome := outcome
				if wantOutcome == "" {
					wantOutcome = "unknown"
				}
				if !strings.Contains(text, "| Launch outcome | "+wantOutcome+" |") {
					t.Fatalf("summary missing launch outcome %q:\n%s", wantOutcome, text)
				}
				for _, secret := range []string{"fixture-secret-api-key", "fixture-secret-registry-key"} {
					if strings.Contains(text+string(combined), secret) {
						t.Error("summary or command output contains a secret")
					}
				}
				if outcome == "success" {
					if !strings.Contains(text, "| Run ID | `fixture-run` |") {
						t.Fatalf("successful launch missing run ID:\n%s", text)
					}
					if !strings.Contains(text, "launch request is accepted") {
						t.Fatalf("successful launch lost confirmation:\n%s", text)
					}
					want := "Sent to fixture@example.invalid when ready"
					if reportURL != "" {
						want = "[Open in Antithesis](" + reportURL + ")"
					}
					if !strings.Contains(text, want) {
						t.Fatalf("successful launch missing report destination %q:\n%s", want, text)
					}
				} else {
					if !strings.Contains(text, "Launch acceptance was not confirmed") {
						t.Fatalf("unsuccessful launch must be explicit:\n%s", text)
					}
					for _, forbidden := range []string{"launch request is accepted", "Sent to", "Open in Antithesis", "fixture-run"} {
						if strings.Contains(text, forbidden) {
							t.Errorf("unsuccessful launch claims %q:\n%s", forbidden, text)
						}
					}
				}
			}
		})
	}
}

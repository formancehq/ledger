package ledgerv3

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func TestCatalogueReproducesTheCurrentProductCommands(t *testing.T) {
	t.Parallel()

	commands := Plugin{}.Commands()
	got := make([]string, 0, len(commands))
	for _, command := range commands {
		got = append(got, strings.Join(command.Path, " "))
	}
	sort.Strings(got)

	want := currentLedgerctlProductCommands(t)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("catalogue has %d commands, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("catalogue command %d = %q, want %q", index, got[index], want[index])
		}
	}
}

func currentLedgerctlProductCommands(t *testing.T) []string {
	t.Helper()

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve catalogue test source path")
	}
	pluginRoot := filepath.Dir(sourceFile)
	repositoryRoot := filepath.Clean(filepath.Join(pluginRoot, "../../.."))
	helper := filepath.Join(pluginRoot, "scripts", "ledgerctl-product-command-paths.go")

	command := exec.Command("go", "run", helper)
	command.Dir = repositoryRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("derive product commands from ledgerctl: %v\n%s", err, output)
	}

	var paths []string
	if err := json.Unmarshal(output, &paths); err != nil {
		t.Fatalf("decode ledgerctl product commands: %v\n%s", err, output)
	}
	return paths
}

func TestCataloguePassesHostAdmission(t *testing.T) {
	t.Parallel()

	plugin := Plugin{}
	if err := sdk.ValidateCatalogue(plugin.Commands(), plugin.DocumentationResources()); err != nil {
		t.Fatalf("ValidateCatalogue: %v", err)
	}
}

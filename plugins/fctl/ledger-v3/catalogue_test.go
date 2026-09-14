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

func TestCatalogueAuditsCriticalLedgerctlSemantics(t *testing.T) {
	t.Parallel()

	semantics := currentLedgerctlCriticalSemantics(t)
	create := commandByPath(t, "ledgers", "create")
	for _, name := range semantics.CreateMirrorFlags {
		if commandFlag(create, name) == nil {
			t.Fatalf("ledgers create does not translate ledgerctl flag %q", name)
		}
	}

	apply := commandByPath(t, "ledgers", "configuration", "apply")
	if !semantics.ConfigurationDryRun || commandFlag(apply, flagDryRun) == nil {
		t.Fatal("configuration apply does not preserve dry-run semantics")
	}

	inspect := commandByPath(t, "indexes", "inspect")
	for name, want := range semantics.InspectDefaults {
		got := commandFlag(inspect, name)
		if got == nil || !got.HasDefault || got.DefaultValue != want {
			t.Fatalf("indexes inspect flag %q default = %#v, ledgerctl = %q", name, got, want)
		}
	}

	execute := commandByPath(t, "queries", "execute")
	if !semantics.QueriesAllPages || !execute.Pagination.Supported {
		t.Fatal("queries execute does not translate ledgerctl all-pages semantics")
	}
}

type ledgerctlCriticalSemantics struct {
	CreateMirrorFlags   []string          `json:"createMirrorFlags"`
	ConfigurationDryRun bool              `json:"configurationDryRun"`
	InspectDefaults     map[string]string `json:"inspectDefaults"`
	QueriesAllPages     bool              `json:"queriesAllPages"`
}

func currentLedgerctlCriticalSemantics(t *testing.T) ledgerctlCriticalSemantics {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve catalogue test source path")
	}
	pluginRoot := filepath.Dir(sourceFile)
	repositoryRoot := filepath.Clean(filepath.Join(pluginRoot, "../../.."))
	helper := filepath.Join(pluginRoot, "scripts", "ledgerctl-product-command-paths.go")
	command := exec.Command("go", "run", helper, "--semantics")
	command.Dir = repositoryRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("derive ledgerctl semantics: %v\n%s", err, output)
	}
	var semantics ledgerctlCriticalSemantics
	if err := json.Unmarshal(output, &semantics); err != nil {
		t.Fatalf("decode ledgerctl semantics: %v\n%s", err, output)
	}
	return semantics
}

func commandFlag(command sdk.Command, name string) *sdk.Flag {
	for index := range command.Flags {
		if command.Flags[index].Name == name {
			return &command.Flags[index]
		}
	}
	return nil
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

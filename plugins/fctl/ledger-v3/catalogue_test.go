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

func TestCataloguePreservesOptionalProductInputsAndDefaults(t *testing.T) {
	t.Parallel()

	transactionCreate := commandByPath(t, "transactions", "create")
	assertOptionalArtifact(t, transactionCreate, flagScript)

	ledgerCreate := commandByPath(t, "ledgers", "create")
	assertOptionalArtifact(t, ledgerCreate, flagMirrorRewriteFile)

	queryCreate := commandByPath(t, "queries", "create")
	assertOptionalFlag(t, queryCreate, flagFilter)
	assertDefaultedFlag(t, queryCreate, flagQueryTarget, "accounts")

	indexInspect := commandByPath(t, "indexes", "inspect")
	assertDefaultedFlag(t, indexInspect, flagTargetType, "account")
	decoded, err := decode(indexInspect, sdk.ExecuteRequest{CommandID: indexInspect.ID, Arguments: []string{"main"}, Flags: []sdk.FlagOccurrence{{Name: flagMetadataKey, Value: "category"}}})
	if err != nil || decoded.text(flagTargetType) != "account" {
		t.Fatalf("indexes inspect default decode = %q, %v", decoded.text(flagTargetType), err)
	}
}

func TestDocumentationResourcesExposeThePinnedLedgerV3CLIReference(t *testing.T) {
	t.Parallel()

	resources := (Plugin{}).DocumentationResources()
	if len(resources) != 1 {
		t.Fatalf("DocumentationResources() has %d entries, want 1", len(resources))
	}
	resource := resources[0]
	if resource.ID != "ledger-v3-cli-reference" || resource.Kind != sdk.DocumentationAPIReference {
		t.Fatalf("documentation resource identity = %#v", resource)
	}
	if resource.URL != "https://raw.githubusercontent.com/formancehq/ledger/9a6fa7d0308e2d33d57fed63c5d1b7f0411194e0/docs/ops/cli.md" {
		t.Fatalf("documentation resource URL = %q", resource.URL)
	}
	if resource.MediaType != "text/markdown" || resource.Locale != "en" || len(resource.SupportedMajors) != 1 || resource.SupportedMajors[0] != productMajor {
		t.Fatalf("documentation resource compatibility = %#v", resource)
	}
	for _, command := range (Plugin{}).Commands() {
		if len(command.DocumentationIDs) != 1 || command.DocumentationIDs[0] != resource.ID {
			t.Fatalf("%s documentation IDs = %v, want [%s]", command.ID, command.DocumentationIDs, resource.ID)
		}
	}
}

func assertOptionalArtifact(t *testing.T, command sdk.Command, flagName string) {
	t.Helper()
	for _, artifact := range command.InputArtifacts {
		if artifact.FlagName == flagName {
			if !artifact.Optional {
				t.Fatalf("%s artifact %q is required, want optional", command.ID, flagName)
			}
			return
		}
	}
	t.Fatalf("%s has no artifact for flag %q", command.ID, flagName)
}

func assertOptionalFlag(t *testing.T, command sdk.Command, flagName string) {
	t.Helper()
	flag := commandFlag(command, flagName)
	if flag == nil || flag.Required {
		t.Fatalf("%s flag %q = %#v, want optional", command.ID, flagName, flag)
	}
}

func assertDefaultedFlag(t *testing.T, command sdk.Command, flagName, want string) {
	t.Helper()
	flag := commandFlag(command, flagName)
	if flag == nil || flag.Required || !flag.HasDefault || flag.DefaultValue != want {
		t.Fatalf("%s flag %q = %#v, want optional default %q", command.ID, flagName, flag, want)
	}
}

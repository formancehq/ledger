package ledgerv2

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// inventoryCommand mirrors the fields of inventory.json this plugin is bound
// to. The committed inventory is the oracle: a descriptor that disagrees with
// it is a preparation violation, not a style difference.
type inventoryCommand struct {
	Aliases     []string `json:"aliases"`
	APIMajor    string   `json:"api_major"`
	Destructive bool     `json:"destructive"`
	HTTPMethod  string   `json:"http_method"`
	HTTPPath    string   `json:"http_path"`
	Idempotency bool     `json:"idempotency_key"`
	Included    bool     `json:"included"`
	Mutation    bool     `json:"mutation"`
	OperationID string   `json:"operation_id"`
	Paginated   bool     `json:"paginated"`
	Path        string   `json:"path"`
	Scopes      []string `json:"scopes"`
	SDKMethod   string   `json:"sdk_method"`
	Use         string   `json:"use"`
}

type inventoryDocument struct {
	Commands     []inventoryCommand `json:"commands"`
	Plugin       string             `json:"plugin"`
	ProductMajor uint32             `json:"product_major"`
}

func loadInventory(t *testing.T) inventoryDocument {
	t.Helper()
	raw, err := os.ReadFile("inventory.json")
	if err != nil {
		t.Fatalf("read inventory.json: %v", err)
	}
	var document inventoryDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode inventory.json: %v", err)
	}
	return document
}

func includedInventory(t *testing.T) map[string]inventoryCommand {
	t.Helper()
	byPath := make(map[string]inventoryCommand)
	for _, command := range loadInventory(t).Commands {
		if command.Included {
			byPath[command.Path] = command
		}
	}
	return byPath
}

func commandsByPath(t *testing.T) map[string]sdk.Command {
	t.Helper()
	byPath := make(map[string]sdk.Command)
	for _, command := range Commands() {
		byPath[strings.Join(command.Path, " ")] = command
	}
	return byPath
}

func TestCommandsCoverExactlyTheIncludedInventory(t *testing.T) {
	t.Parallel()

	want := includedInventory(t)
	if len(want) != 22 {
		t.Fatalf("included inventory commands = %d, want 22", len(want))
	}
	got := commandsByPath(t)
	if len(got) != len(want) {
		t.Fatalf("Commands() = %d descriptors, want %d", len(got), len(want))
	}
	for path := range want {
		if _, ok := got[path]; !ok {
			t.Errorf("Commands() omits the included command %q", path)
		}
	}
	for path := range got {
		if _, ok := want[path]; !ok {
			t.Errorf("Commands() publishes %q, which the inventory does not include", path)
		}
	}
}

func TestIdempotencyFlagExposureMatchesInventoryExactly(t *testing.T) {
	t.Parallel()
	inventory := includedInventory(t)
	for path, command := range commandsByPath(t) {
		declared := false
		for _, flag := range command.Flags {
			if flag.Name == "idempotency-key" {
				if declared {
					t.Fatalf("%s declares idempotency-key more than once", path)
				}
				declared = true
			}
		}
		if want := inventory[path].Idempotency; declared != want {
			t.Errorf("%s idempotency-key declared = %t, inventory = %t", path, declared, want)
		}
	}
}

func TestCommandsPreserveInventoryGrammarAndBinding(t *testing.T) {
	t.Parallel()

	got := commandsByPath(t)
	for path, want := range includedInventory(t) {
		command, ok := got[path]
		if !ok {
			continue
		}
		if last := command.PathAliases; len(last) != len(command.Path) {
			t.Errorf("%s: path aliases length = %d, want %d", path, len(last), len(command.Path))
		} else {
			leaf := last[len(last)-1]
			if !slices.Equal(leaf, want.Aliases) {
				t.Errorf("%s: leaf aliases = %v, want %v", path, leaf, want.Aliases)
			}
			for index, aliases := range last[:len(last)-1] {
				if len(aliases) != 0 {
					t.Errorf("%s: segment %d declares aliases %v, want none", path, index, aliases)
				}
			}
		}
		if len(command.Operations) == 0 {
			t.Fatalf("%s: declares no operation", path)
		}
		primary := command.Operations[0]
		if primary.ID != want.OperationID {
			t.Errorf("%s: primary operation = %q, want %q", path, primary.ID, want.OperationID)
		}
		if !slices.Equal(primary.Scopes, want.Scopes) {
			t.Errorf("%s: scopes = %v, want %v", path, primary.Scopes, want.Scopes)
		}
		if primary.HTTP == nil {
			t.Fatalf("%s: primary operation declares no HTTP policy", path)
		}
		if primary.HTTP.Method != want.HTTPMethod {
			t.Errorf("%s: method = %q, want %q", path, primary.HTTP.Method, want.HTTPMethod)
		}
		template := primary.HTTP.Path
		if template == "" && primary.HTTP.GeneratedClient != nil {
			template = primary.HTTP.GeneratedClient.PathTemplate
		}
		if template != want.HTTPPath {
			t.Errorf("%s: path = %q, want %q", path, template, want.HTTPPath)
		}
		if command.Pagination.Supported != want.Paginated {
			t.Errorf("%s: pagination = %t, want %t", path, command.Pagination.Supported, want.Paginated)
		}
		wantRisk := sdk.RiskRead
		if want.Mutation {
			wantRisk = sdk.RiskMutation
		}
		if command.Risk != wantRisk {
			t.Errorf("%s: risk = %q, want %q", path, command.Risk, wantRisk)
		}
	}
}

func TestCatalogueIsAdmissible(t *testing.T) {
	t.Parallel()

	commands := Commands()
	if err := sdk.ValidateCatalogue(commands, nil); err != nil {
		t.Fatalf("ValidateCatalogue() = %v, want nil", err)
	}
	if err := sdk.ValidateCommandFacetHostRequirements(Metadata().Facets, commands); err != nil {
		t.Fatalf("ValidateCommandFacetHostRequirements() = %v, want nil", err)
	}
}

func TestCatalogueDeclaresProductMajorTwoOnly(t *testing.T) {
	t.Parallel()

	for _, command := range Commands() {
		path := strings.Join(command.Path, " ")
		if len(command.Compatibility) != 1 {
			t.Fatalf("%s: compatibility entries = %d, want 1", path, len(command.Compatibility))
		}
		entry := command.Compatibility[0]
		if entry.Service != sdk.ServiceLedger || !slices.Equal(entry.Majors, []uint32{2}) {
			t.Errorf("%s: compatibility = %v %v, want ledger [2]", path, entry.Service, entry.Majors)
		}
	}
}

func TestCatalogueNamesNoSigningCapability(t *testing.T) {
	t.Parallel()

	for _, command := range Commands() {
		path := strings.Join(command.Path, " ")
		if command.RequestSigning != nil {
			t.Errorf("%s: declares request signing, which is major-3 only", path)
		}
		for _, requirement := range command.Requires {
			if requirement.ProviderCapability == sdk.CapabilitySignLedgerApplyBatch {
				t.Errorf("%s: requires %q", path, sdk.CapabilitySignLedgerApplyBatch)
			}
		}
	}
	for _, facet := range Metadata().Facets {
		if facet.Kind == sdk.FacetSignerProvider {
			t.Error("metadata declares a signer facet")
		}
		if slices.Contains(facet.Capabilities, sdk.CapabilitySignLedgerApplyBatch) {
			t.Errorf("facet %q declares %q", facet.Kind, sdk.CapabilitySignLedgerApplyBatch)
		}
	}
}

func TestCatalogueBindsNoV1Operation(t *testing.T) {
	t.Parallel()

	for _, command := range Commands() {
		path := strings.Join(command.Path, " ")
		for _, operation := range command.Operations {
			if strings.HasPrefix(operation.ID, "v1") {
				t.Errorf("%s: binds the V1 operation %q", path, operation.ID)
			}
			if operation.HTTP == nil {
				continue
			}
			template := operation.HTTP.Path
			if template == "" && operation.HTTP.GeneratedClient != nil {
				template = operation.HTTP.GeneratedClient.PathTemplate
			}
			if strings.HasPrefix(template, "/v1") {
				t.Errorf("%s: binds the V1 route %q", path, template)
			}
		}
	}
}

func TestCatalogueExcludesTheHostOwnedInfoProbe(t *testing.T) {
	t.Parallel()

	for _, command := range Commands() {
		path := strings.Join(command.Path, " ")
		if path == "ledger server-infos" {
			t.Error("publishes the host-owned /_/info probe as a product command")
		}
		for _, operation := range command.Operations {
			if operation.ID == "v2GetInfo" {
				t.Errorf("%s: binds the host-owned v2GetInfo operation", path)
			}
		}
	}
}

func TestFilterListOperationsDeclareTheirJSONRequestBodies(t *testing.T) {
	t.Parallel()

	for _, id := range []string{"ledger.v2.accounts.list", "ledger.v2.transactions.list", "ledger.v2.volumes.list"} {
		command, ok := commandByID(id)
		if !ok {
			t.Fatalf("command %q missing", id)
		}
		generated := command.Operations[0].HTTP.GeneratedClient
		if generated.MaxRequestBytes != requestJSONBytes || !slices.Equal(generated.RequestContentTypes, []string{"application/json"}) {
			t.Errorf("%s generated request policy = %#v", id, generated)
		}
	}
}

func TestImportDeclaresItsBoundedChunkingAndOptionalResumeProbe(t *testing.T) {
	t.Parallel()

	command, ok := commandByID("ledger.v2.import")
	if !ok {
		t.Fatal("import command missing")
	}
	if len(command.Operations) != 2 || command.Operations[0].ID != "v2ImportLogs" || command.Operations[1].ID != "v2ListLogs" {
		t.Fatalf("import operations = %#v, want import followed by list-logs", command.Operations)
	}
	if command.ExecutionPolicy == nil || command.ExecutionPolicy.MaxHostRequests != sdk.PortableMaxHostRequests {
		t.Fatalf("import execution policy = %#v, want bounded chunking budget", command.ExecutionPolicy)
	}
}

func TestRelativeTransactionCommandsDeclareTheReadLookup(t *testing.T) {
	t.Parallel()

	for _, id := range []string{
		"ledger.v2.transactions.show",
		"ledger.v2.transactions.set-metadata",
		"ledger.v2.transactions.delete-metadata",
		"ledger.v2.transactions.revert",
	} {
		command, ok := commandByID(id)
		if !ok {
			t.Fatalf("command %q missing", id)
		}
		if len(command.Operations) != 2 || command.Operations[1].ID != "v2ListTransactions" || !slices.Equal(command.Operations[1].Scopes, []string{"ledger:read"}) {
			t.Errorf("%s operations = %#v", id, command.Operations)
		}
		if command.ExecutionPolicy == nil || command.ExecutionPolicy.MaxHostRequests != 2 {
			t.Errorf("%s execution policy = %#v", id, command.ExecutionPolicy)
		}
	}
}

func TestCatalogueDeclaresOnlyTheThreeHostOwnedInputArtifacts(t *testing.T) {
	t.Parallel()

	var declared []string
	for _, command := range Commands() {
		for _, artifact := range command.InputArtifacts {
			declared = append(declared, command.ID+":"+artifact.ArgumentName)
			switch command.ID {
			case "ledger.v2.import":
				if artifact.ArgumentName != "file-path" || !artifact.AllowFile || artifact.AllowStdin || artifact.MaxBytes != inputArtifactBytes || !slices.Equal(artifact.MediaTypes, contentTypesBinary) {
					t.Errorf("import artifact = %#v", artifact)
				}
			case "ledger.v2.schemas.insert":
				if artifact.ArgumentName != "source" || !artifact.AllowFile || artifact.AllowStdin || artifact.MaxBytes != requestJSONBytes || !slices.Equal(artifact.MediaTypes, contentTypesBinary) {
					t.Errorf("schema artifact = %#v", artifact)
				}
			case "ledger.v2.transactions.num":
				if artifact.ArgumentName != "file" || !artifact.AllowFile || !artifact.AllowStdin || artifact.MaxBytes != requestJSONBytes || !slices.Equal(artifact.MediaTypes, []string{"text/plain"}) {
					t.Errorf("numscript artifact = %#v", artifact)
				}
			default:
				t.Errorf("%s unexpectedly declares artifact %#v", command.ID, artifact)
			}
		}
	}
	slices.Sort(declared)
	if !slices.Equal(declared, []string{"ledger.v2.import:file-path", "ledger.v2.schemas.insert:source", "ledger.v2.transactions.num:file"}) {
		t.Fatalf("declared artifacts = %v", declared)
	}
}

func TestEveryCommandRequiresTheStackAuthCapability(t *testing.T) {
	t.Parallel()

	for _, command := range Commands() {
		path := strings.Join(command.Path, " ")
		if command.AuthMode != sdk.AuthModeCapability {
			t.Errorf("%s: auth mode = %q, want %q", path, command.AuthMode, sdk.AuthModeCapability)
		}
		if len(command.Auth) != 1 || command.Auth[0].Capability != "auth.stack" || command.Auth[0].Optional {
			t.Errorf("%s: auth = %v, want one non-optional auth.stack", path, command.Auth)
		}
		if command.Target.Kind != sdk.TargetStack {
			t.Errorf("%s: target = %q, want %q", path, command.Target.Kind, sdk.TargetStack)
		}
	}
}

func TestMappingNamesTheHistoricalTransactionTimeFlags(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("mapping.md")
	if err != nil {
		t.Fatalf("read mapping.md: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "`account`, `source`, `destination`, `reference`, `start`, `end`, `metadata`") {
		t.Fatal("mapping does not name the historical transaction filters as start and end")
	}
}

// Package audit verifies the fctl Ledger plugin preparation artifacts.
//
// It is internal audit tooling shared by the ledger-v2 and ledger-v3
// preparation directories. It deliberately carries no plugin contract and is
// not packaged into either plugin: sharing it must not couple the two
// plugins' catalogues, manifests or command surfaces.
package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Classification buckets used by the ledger-v3 inventory. Every executable
// ledgerctl command carries exactly one, so the four tallies must sum to the
// full executable total with no silent omission.
const (
	ClassProduct          = "product"
	ClassOperator         = "operator"
	ClassHostLocal        = "host_local"
	ClassSigningEventSink = "signing_event_sink"
)

// SigningCapability is the only request-signing capability the programme
// grants. It is bound to Ledger product major 3; ledger-v2 must never name it.
const SigningCapability = "sign.ledger.apply-batch"

// Command is one executable command row. The ledger-v2 inventory keys
// inclusion off Included/ExclusionReason; the ledger-v3 inventory keys it off
// Classification. Both shapes share this struct so one audit covers both.
type Command struct {
	Path            string   `json:"path"`
	Use             string   `json:"use,omitempty"`
	Aliases         []string `json:"aliases"`
	Group           string   `json:"group,omitempty"`
	Classification  string   `json:"classification,omitempty"`
	APIMajor        string   `json:"api_major,omitempty"`
	SDKMethod       string   `json:"sdk_method,omitempty"`
	OperationID     *string  `json:"operation_id,omitempty"`
	HTTPMethod      *string  `json:"http_method,omitempty"`
	HTTPPath        *string  `json:"http_path,omitempty"`
	SourceFile      *string  `json:"source_file,omitempty"`
	RPCs            []string `json:"rpcs,omitempty"`
	StreamingRPCs   []string `json:"streaming_rpcs,omitempty"`
	RPCNote         string   `json:"rpc_note,omitempty"`
	Scopes          []string `json:"scopes,omitempty"`
	Mutation        bool     `json:"mutation"`
	Destructive     bool     `json:"destructive"`
	Paginated       bool     `json:"paginated"`
	IdempotencyKey  bool     `json:"idempotency_key,omitempty"`
	Included        *bool    `json:"included,omitempty"`
	ExclusionReason *string  `json:"exclusion_reason,omitempty"`
}

// Counts is the recorded ledger-v3 classification tally. The audit recomputes
// it from Commands rather than trusting it.
type Counts struct {
	ExecutableTotal  int `json:"executable_total"`
	Product          int `json:"product"`
	Operator         int `json:"operator"`
	HostLocal        int `json:"host_local"`
	SigningEventSink int `json:"signing_event_sink"`
}

// Inventory is one plugin's command inventory.
type Inventory struct {
	SchemaVersion     int               `json:"schema_version"`
	Plugin            string            `json:"plugin"`
	ProductMajor      int               `json:"product_major"`
	SigningCapability *string           `json:"signing_capability"`
	Source            map[string]string `json:"source"`
	Counts            *Counts           `json:"counts,omitempty"`
	Commands          []Command         `json:"commands"`
}

// Manifest is one plugin's logical manifest. Only the fields the audit
// constrains are modelled; unknown fields are ignored on purpose so the
// manifest can grow without breaking the audit.
type Manifest struct {
	SchemaVersion        int             `json:"schema_version"`
	Kind                 string          `json:"kind"`
	Status               string          `json:"status"`
	Name                 string          `json:"name"`
	Module               string          `json:"module"`
	ProductMajors        []int           `json:"product_majors"`
	CapabilitiesDeclared []string        `json:"capabilities_declared"`
	SigningCapability    json.RawMessage `json:"signing_capability"`
	PluginModulePath     string          `json:"plugin_module_path"`
	CoverageDenominator  int             `json:"coverage_denominator"`
	Invariants           []string        `json:"invariants"`
	RuntimeArtifact      *string         `json:"runtime_artifact"`
}

// IncludedCommands returns the commands this plugin publishes.
//
// ledger-v2 marks inclusion explicitly. ledger-v3 has no Included field, so a
// command is published exactly when it is classified as an ordinary product
// command.
func (inv Inventory) IncludedCommands() []Command {
	out := make([]Command, 0, len(inv.Commands))

	for _, c := range inv.Commands {
		switch {
		case c.Included != nil:
			if *c.Included {
				out = append(out, c)
			}
		case c.Classification == ClassProduct:
			out = append(out, c)
		}
	}

	return out
}

// Tally recomputes the classification counts from Commands.
func (inv Inventory) Tally() Counts {
	t := Counts{ExecutableTotal: len(inv.Commands)}

	for _, c := range inv.Commands {
		switch c.Classification {
		case ClassProduct:
			t.Product++
		case ClassOperator:
			t.Operator++
		case ClassHostLocal:
			t.HostLocal++
		case ClassSigningEventSink:
			t.SigningEventSink++
		}
	}

	return t
}

// LoadInventory reads and decodes an inventory document.
func LoadInventory(path string) (Inventory, []byte, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return Inventory{}, nil, fmt.Errorf("read inventory %s: %w", path, err)
	}

	var inv Inventory
	if err := json.Unmarshal(raw, &inv); err != nil {
		return Inventory{}, nil, fmt.Errorf("decode inventory %s: %w", path, err)
	}

	return inv, raw, nil
}

// LoadManifest reads and decodes a manifest document.
func LoadManifest(path string) (Manifest, []byte, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("read manifest %s: %w", path, err)
	}

	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, nil, fmt.Errorf("decode manifest %s: %w", path, err)
	}

	return m, raw, nil
}

// Package audit verifies the fctl Ledger plugin inventory and manifest artifacts.
//
// It is internal audit tooling shared by ledger-v2 and ledger-v3. It
// deliberately carries no plugin contract and is
// not packaged into either plugin: sharing it must not couple the two
// plugins' catalogues, manifests or command surfaces.
package audit

import (
	"bytes"
	"encoding/json"
	"errors"
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
//
// The Baseline* fields record what the historical old-fctl command actually
// called; APIMajor/SDKMethod record what the plugin binds. The two differ for
// every command converted from a V1 call to its V2 equivalent, so keeping them
// apart is what makes a conversion auditable instead of a rewritten fact.
type Command struct {
	Path              string   `json:"path"`
	Use               string   `json:"use,omitempty"`
	Aliases           []string `json:"aliases"`
	Group             string   `json:"group,omitempty"`
	Classification    string   `json:"classification,omitempty"`
	APIMajor          string   `json:"api_major,omitempty"`
	SDKMethod         string   `json:"sdk_method,omitempty"`
	BaselineAPIMajor  string   `json:"baseline_api_major,omitempty"`
	BaselineSDKMethod string   `json:"baseline_sdk_method,omitempty"`
	ConversionNote    string   `json:"conversion_note,omitempty"`
	OperationID       *string  `json:"operation_id,omitempty"`
	HTTPMethod        *string  `json:"http_method,omitempty"`
	HTTPPath          *string  `json:"http_path,omitempty"`
	SourceFile        *string  `json:"source_file,omitempty"`
	RPCs              []string `json:"rpcs,omitempty"`
	StreamingRPCs     []string `json:"streaming_rpcs,omitempty"`
	RPCNote           string   `json:"rpc_note,omitempty"`
	Scopes            []string `json:"scopes,omitempty"`
	Mutation          bool     `json:"mutation"`
	Destructive       bool     `json:"destructive"`
	Paginated         bool     `json:"paginated"`
	IdempotencyKey    bool     `json:"idempotency_key,omitempty"`
	Included          *bool    `json:"included,omitempty"`
	ExclusionReason   *string  `json:"exclusion_reason,omitempty"`
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

// V2Counts is the recorded ledger-v2 tally. The audit recomputes it from
// Commands rather than trusting it.
type V2Counts struct {
	BaselineExecutable        int `json:"baseline_executable"`
	Included                  int `json:"included"`
	ExcludedHostOwned         int `json:"excluded_host_owned"`
	BaselineV1Only            int `json:"baseline_v1_only"`
	ConvertedV1ToV2           int `json:"converted_v1_to_v2"`
	DistinctPrimaryOperations int `json:"distinct_primary_operations"`
}

// Inventory is one plugin's command inventory.
//
// Counts stays raw because the two plugins record different tallies under the
// same key. Decoding it into the wrong shape would silently yield zeros, so
// each side asks for its own type through V2Counts/V3Counts, which reject
// unknown fields.
type Inventory struct {
	SchemaVersion     int               `json:"schema_version"`
	Plugin            string            `json:"plugin"`
	ProductMajor      int               `json:"product_major"`
	SigningCapability *string           `json:"signing_capability"`
	Source            map[string]string `json:"source"`
	Counts            json.RawMessage   `json:"counts,omitempty"`
	Commands          []Command         `json:"commands"`
}

// V2Counts decodes the recorded ledger-v2 tally.
func (inv Inventory) V2Counts() (V2Counts, error) {
	var c V2Counts
	return c, decodeCounts(inv.Counts, &c)
}

// V3Counts decodes the recorded ledger-v3 classification tally.
func (inv Inventory) V3Counts() (Counts, error) {
	var c Counts
	return c, decodeCounts(inv.Counts, &c)
}

func decodeCounts(raw []byte, into any) error {
	if len(raw) == 0 {
		return errNoCounts
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("decode counts: %w", err)
	}

	return nil
}

var errNoCounts = errors.New("inventory records no counts block")

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

// generate-json-schemas extracts OpenAPI v3 schemas from generated CRD manifests
// and writes standalone JSON Schema files for IDE validation and external tooling.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const (
	jsonSchemaDraft04 = "http://json-schema.org/draft-04/schema#"
	jsonSchemaDraft07 = "http://json-schema.org/draft-07/schema#"
)

// crdInfo identifies a generated kind schema for the dispatcher schema below.
type crdInfo struct {
	group   string
	version string
	kind    string
}

func main() {
	if err := run("config/crd/bases", "config/crd/schemas"); err != nil {
		fmt.Fprintf(os.Stderr, "generate-json-schemas: %v\n", err)
		os.Exit(1)
	}
}

func run(crdDir, outDir string) error {
	entries, err := os.ReadDir(crdDir)
	if err != nil {
		return fmt.Errorf("reading CRD directory %q: %w", crdDir, err)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating output directory %q: %w", outDir, err)
	}

	if err := clearJSONFiles(outDir); err != nil {
		return fmt.Errorf("clearing output directory %q: %w", outDir, err)
	}

	written := 0
	var crds []crdInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		crdPath := filepath.Join(crdDir, entry.Name())
		n, info, err := writeSchemasForCRD(crdPath, outDir)
		if err != nil {
			return fmt.Errorf("%s: %w", crdPath, err)
		}
		written += n
		crds = append(crds, info)
	}

	if written == 0 {
		return fmt.Errorf("no JSON schemas written from %q", crdDir)
	}

	if err := writeDispatcherSchemas(crds, outDir); err != nil {
		return fmt.Errorf("writing dispatcher schemas: %w", err)
	}

	return nil
}

func clearJSONFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

func writeSchemasForCRD(crdPath, outDir string) (int, crdInfo, error) {
	data, err := os.ReadFile(crdPath)
	if err != nil {
		return 0, crdInfo{}, fmt.Errorf("reading CRD: %w", err)
	}

	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		return 0, crdInfo{}, fmt.Errorf("decoding CRD: %w", err)
	}

	schema, version, err := crdOpenAPISchema(&crd)
	if err != nil {
		return 0, crdInfo{}, err
	}

	kind := crd.Spec.Names.Kind
	baseName := fmt.Sprintf("%s_%s", version, strings.ToLower(kind))

	apiVersion := fmt.Sprintf("%s/%s", crd.Spec.Group, version)

	fullPath := filepath.Join(outDir, baseName+".json")
	if err := writeSchemaFile(fullPath, schema, requireResourceIdentity(apiVersion, kind)); err != nil {
		return 0, crdInfo{}, fmt.Errorf("writing full schema: %w", err)
	}

	specSchema, ok := schema.Properties["spec"]
	if !ok {
		return 0, crdInfo{}, fmt.Errorf("CRD %q has no spec property", crd.Name)
	}

	specPath := filepath.Join(outDir, baseName+".spec.json")
	if err := writeSchemaFile(specPath, &specSchema, nil); err != nil {
		return 0, crdInfo{}, fmt.Errorf("writing spec schema: %w", err)
	}

	return 2, crdInfo{group: crd.Spec.Group, version: version, kind: kind}, nil
}

// writeDispatcherSchemas writes one draft-07 schema per CRD group that routes
// a manifest to the kind schema matching its apiVersion/kind via "if"/"then".
// This lets a single $schema mapping cover a directory containing manifests
// of several kinds instead of requiring one mapping per kind.
func writeDispatcherSchemas(crds []crdInfo, outDir string) error {
	var groups []string
	byGroup := map[string][]crdInfo{}
	for _, c := range crds {
		if _, seen := byGroup[c.group]; !seen {
			groups = append(groups, c.group)
		}
		byGroup[c.group] = append(byGroup[c.group], c)
	}

	for _, group := range groups {
		allOf := make([]any, 0, len(byGroup[group]))
		for _, c := range byGroup[group] {
			baseName := fmt.Sprintf("%s_%s", c.version, strings.ToLower(c.kind))
			allOf = append(allOf, map[string]any{
				"if": map[string]any{
					"properties": map[string]any{
						"apiVersion": map[string]any{"const": fmt.Sprintf("%s/%s", c.group, c.version)},
						"kind":       map[string]any{"const": c.kind},
					},
					"required": []string{"apiVersion", "kind"},
				},
				"then": map[string]any{"$ref": baseName + ".json"},
			})
		}

		doc := map[string]any{
			"$schema":     jsonSchemaDraft07,
			"description": fmt.Sprintf("Dispatches %s documents to the schema matching their kind. Documents of any other kind are left unconstrained.", group),
			"allOf":       allOf,
		}

		payload, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding dispatcher schema for group %q: %w", group, err)
		}
		payload = append(payload, '\n')

		path := filepath.Join(outDir, group+".json")
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			return fmt.Errorf("writing %q: %w", path, err)
		}
	}

	return nil
}

func crdOpenAPISchema(crd *apiextv1.CustomResourceDefinition) (*apiextv1.JSONSchemaProps, string, error) {
	if len(crd.Spec.Versions) == 0 {
		return nil, "", fmt.Errorf("CRD %q has no versions", crd.Name)
	}

	var chosen *apiextv1.CustomResourceDefinitionVersion
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Storage {
			chosen = &crd.Spec.Versions[i]

			break
		}
	}
	if chosen == nil {
		return nil, "", fmt.Errorf("CRD %q has no version marked as storage version", crd.Name)
	}

	if chosen.Schema == nil || chosen.Schema.OpenAPIV3Schema == nil {
		return nil, "", fmt.Errorf("CRD %q version %q has no OpenAPI v3 schema", crd.Name, chosen.Name)
	}

	return chosen.Schema.OpenAPIV3Schema, chosen.Name, nil
}

// writeSchemaFile writes schema as JSON Schema. mutate, when non-nil, can
// further adjust the converted document (e.g. constraining resource
// identity) before it is encoded.
func writeSchemaFile(path string, schema *apiextv1.JSONSchemaProps, mutate func(doc map[string]any) error) error {
	doc := withJSONSchemaMeta(schema)

	if mutate != nil {
		if err := mutate(doc); err != nil {
			return err
		}
	}

	payload, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding schema: %w", err)
	}
	payload = append(payload, '\n')

	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("writing %q: %w", path, err)
	}

	return nil
}

// requireResourceIdentity returns a writeSchemaFile mutator that constrains a
// full-resource schema to the exact apiVersion/kind it describes and requires
// both. Without this, {} or a manifest for an unrelated kind passes the
// schema's structural checks, even though this file is advertised for full
// Kubernetes manifest validation.
func requireResourceIdentity(apiVersion, kind string) func(doc map[string]any) error {
	return func(doc map[string]any) error {
		properties, ok := doc["properties"].(map[string]any)
		if !ok {
			return errors.New("full resource schema has no properties")
		}

		for key, value := range map[string]string{"apiVersion": apiVersion, "kind": kind} {
			prop, ok := properties[key].(map[string]any)
			if !ok {
				return fmt.Errorf("full resource schema has no %q property", key)
			}
			prop["enum"] = []string{value}
		}

		required, _ := doc["required"].([]any)
		for _, key := range []string{"apiVersion", "kind"} {
			if !containsString(required, key) {
				required = append(required, key)
			}
		}
		doc["required"] = required

		return nil
	}
}

func containsString(list []any, s string) bool {
	for _, v := range list {
		if str, ok := v.(string); ok && str == s {
			return true
		}
	}

	return false
}

func withJSONSchemaMeta(schema *apiextv1.JSONSchemaProps) map[string]any {
	raw, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Sprintf("marshal schema: %v", err))
	}

	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		panic(fmt.Sprintf("unmarshal schema: %v", err))
	}

	doc["$schema"] = jsonSchemaDraft04
	enforceAdditionalPropertiesFalse(doc)

	return doc
}

// enforceAdditionalPropertiesFalse makes the schema reject unknown properties
// on every object node that declares a fixed property set, so editor/IDE
// validation catches typos (e.g. "replicass") that the Kubernetes API server
// would otherwise silently prune rather than reject. It leaves map-type nodes
// (which declare "additionalProperties" as a value schema, not a boolean)
// and nodes explicitly marked x-kubernetes-preserve-unknown-fields untouched.
func enforceAdditionalPropertiesFalse(node any) {
	switch n := node.(type) {
	case map[string]any:
		if _, hasProperties := n["properties"]; hasProperties {
			_, hasAdditionalProperties := n["additionalProperties"]
			preserveUnknown, _ := n["x-kubernetes-preserve-unknown-fields"].(bool)
			if !hasAdditionalProperties && !preserveUnknown {
				n["additionalProperties"] = false
			}
		}

		for _, value := range n {
			enforceAdditionalPropertiesFalse(value)
		}
	case []any:
		for _, item := range n {
			enforceAdditionalPropertiesFalse(item)
		}
	}
}

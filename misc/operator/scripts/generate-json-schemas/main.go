// generate-json-schemas extracts OpenAPI v3 schemas from generated CRD manifests
// and writes standalone JSON Schema files for IDE validation and external tooling.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const jsonSchemaDraft04 = "http://json-schema.org/draft-04/schema#"

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
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		crdPath := filepath.Join(crdDir, entry.Name())
		n, err := writeSchemasForCRD(crdPath, outDir)
		if err != nil {
			return fmt.Errorf("%s: %w", crdPath, err)
		}
		written += n
	}

	if written == 0 {
		return fmt.Errorf("no JSON schemas written from %q", crdDir)
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

func writeSchemasForCRD(crdPath, outDir string) (int, error) {
	data, err := os.ReadFile(crdPath)
	if err != nil {
		return 0, fmt.Errorf("reading CRD: %w", err)
	}

	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		return 0, fmt.Errorf("decoding CRD: %w", err)
	}

	schema, version, err := crdOpenAPISchema(&crd)
	if err != nil {
		return 0, err
	}

	kind := strings.ToLower(crd.Spec.Names.Kind)
	baseName := fmt.Sprintf("%s_%s", version, kind)

	fullPath := filepath.Join(outDir, baseName+".json")
	if err := writeSchemaFile(fullPath, schema); err != nil {
		return 0, fmt.Errorf("writing full schema: %w", err)
	}

	specSchema, ok := schema.Properties["spec"]
	if !ok {
		return 0, fmt.Errorf("CRD %q has no spec property", crd.Name)
	}

	specPath := filepath.Join(outDir, baseName+".spec.json")
	if err := writeSchemaFile(specPath, &specSchema); err != nil {
		return 0, fmt.Errorf("writing spec schema: %w", err)
	}

	return 2, nil
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

func writeSchemaFile(path string, schema *apiextv1.JSONSchemaProps) error {
	payload, err := json.MarshalIndent(withJSONSchemaMeta(schema), "", "  ")
	if err != nil {
		return fmt.Errorf("encoding schema: %w", err)
	}
	payload = append(payload, '\n')

	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("writing %q: %w", path, err)
	}

	return nil
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

	return doc
}

package explain

import (
	"context"
	"fmt"
	"strings"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const clusterCRDName = "clusters.ledger.formance.com"

// FetchSpecFields fetches the Cluster CRD from the cluster and builds
// the field tree from its OpenAPI v3 schema. Returns descriptions, defaults,
// and validation info that reflection alone cannot provide.
func FetchSpecFields(ctx context.Context, cfg *rest.Config) ([]Field, error) {
	client, err := apiextensionsclientset.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating apiextensions client: %w", err)
	}

	crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, clusterCRDName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("fetching CRD %s: %w", clusterCRDName, err)
	}

	if len(crd.Spec.Versions) == 0 {
		return nil, fmt.Errorf("CRD %s has no versions", clusterCRDName)
	}

	schema := crd.Spec.Versions[0].Schema
	if schema == nil || schema.OpenAPIV3Schema == nil {
		return nil, fmt.Errorf("CRD %s has no OpenAPI v3 schema", clusterCRDName)
	}

	specProp, ok := schema.OpenAPIV3Schema.Properties["spec"]
	if !ok {
		return nil, fmt.Errorf("CRD %s has no spec property", clusterCRDName)
	}

	return fieldsFromOpenAPI(specProp.Properties), nil
}

// FetchStatusFields fetches status fields from the cluster CRD.
func FetchStatusFields(ctx context.Context, cfg *rest.Config) ([]Field, error) {
	client, err := apiextensionsclientset.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating apiextensions client: %w", err)
	}

	crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, clusterCRDName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("fetching CRD %s: %w", clusterCRDName, err)
	}

	if len(crd.Spec.Versions) == 0 {
		return nil, fmt.Errorf("CRD %s has no versions", clusterCRDName)
	}

	schema := crd.Spec.Versions[0].Schema
	if schema == nil || schema.OpenAPIV3Schema == nil {
		return nil, fmt.Errorf("CRD %s has no OpenAPI v3 schema", clusterCRDName)
	}

	statusProp, ok := schema.OpenAPIV3Schema.Properties["status"]
	if !ok {
		return nil, fmt.Errorf("CRD %s has no status property", clusterCRDName)
	}

	return fieldsFromOpenAPI(statusProp.Properties), nil
}

// fieldsFromOpenAPI converts an OpenAPI v3 properties map to []Field.
func fieldsFromOpenAPI(props map[string]apiextv1.JSONSchemaProps) []Field {
	fields := make([]Field, 0, len(props))

	for name, prop := range props {
		f := Field{
			Name:        name,
			Description: prop.Description,
		}

		if prop.Default != nil {
			f.Default = string(prop.Default.Raw)
			// Strip JSON quotes from string defaults.
			if len(f.Default) >= 2 && f.Default[0] == '"' {
				f.Default = f.Default[1 : len(f.Default)-1]
			}
		}

		// Detect immutability from XValidation rules containing "oldSelf".
		for _, rule := range prop.XValidations {
			if strings.Contains(rule.Rule, "oldSelf") {
				f.Immutable = true

				break
			}
		}

		// Extract enum values.
		if len(prop.Enum) > 0 {
			for _, e := range prop.Enum {
				val := string(e.Raw)
				if len(val) >= 2 && val[0] == '"' {
					val = val[1 : len(val)-1]
				}
				f.Enum = append(f.Enum, val)
			}
		}

		switch prop.Type {
		case "object":
			if len(prop.Properties) > 0 {
				f.Type = "object"
				f.Children = fieldsFromOpenAPI(prop.Properties)
			} else {
				// Opaque object (e.g. map, RawExtension).
				f.Type = "object"
			}
		case "array":
			if prop.Items != nil && prop.Items.Schema != nil {
				itemSchema := prop.Items.Schema
				switch itemSchema.Type {
				case "string":
					f.Type = "[]string"
				case "integer":
					f.Type = "[]int32"
				case "object":
					f.Type = "[]object"
					if len(itemSchema.Properties) > 0 {
						f.Children = fieldsFromOpenAPI(itemSchema.Properties)
					}
				default:
					f.Type = "[]" + itemSchema.Type
				}
			} else {
				f.Type = "[]object"
			}
		case "integer":
			if prop.Format == "int64" {
				f.Type = "int64"
			} else {
				f.Type = "int32"
			}
		case "boolean":
			f.Type = "bool"
		case "string":
			f.Type = "string"
		default:
			f.Type = prop.Type
		}

		fields = append(fields, f)
	}

	return fields
}

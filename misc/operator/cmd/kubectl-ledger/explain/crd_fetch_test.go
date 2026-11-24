package explain

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestResolveFields(t *testing.T) {
	t.Parallel()

	for _, scenario := range []string{"live", "no versions", "no schema", "no OpenAPI", "no spec", "no status", "no properties", "fetch error", "config error", "client error"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()

			properties := map[string]apiextv1.JSONSchemaProps{
				"spec":   {Properties: map[string]apiextv1.JSONSchemaProps{"liveSpec": {Type: "string", Description: "Live spec description"}}},
				"status": {Properties: map[string]apiextv1.JSONSchemaProps{"liveStatus": {Type: "string", Description: "Live status description"}}},
			}
			schema := &apiextv1.CustomResourceValidation{OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Properties: properties}}
			versions := []apiextv1.CustomResourceDefinitionVersion{{Name: "v1alpha1", Schema: schema}}
			switch scenario {
			case "no versions":
				versions = nil
			case "no schema":
				versions[0].Schema = nil
			case "no OpenAPI":
				schema.OpenAPIV3Schema = nil
			case "no spec":
				delete(properties, "spec")
			case "no status":
				delete(properties, "status")
			case "no properties":
				clear(properties)
			}
			crd := apiextv1.CustomResourceDefinition{
				TypeMeta:   metav1.TypeMeta{APIVersion: "apiextensions.k8s.io/v1", Kind: "CustomResourceDefinition"},
				ObjectMeta: metav1.ObjectMeta{Name: clusterCRDName},
				Spec:       apiextv1.CustomResourceDefinitionSpec{Versions: versions},
			}
			body, err := json.Marshal(crd)
			require.NoError(t, err)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				if !assert.Equal(t, http.MethodGet, request.Method) ||
					!assert.Equal(t, "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/"+clusterCRDName, request.URL.Path) {
					writer.WriteHeader(http.StatusBadRequest)

					return
				}
				writer.Header().Set("Content-Type", "application/json")
				if scenario == "fetch error" {
					writer.WriteHeader(http.StatusForbidden)

					return
				}
				_, writeErr := writer.Write(body)
				assert.NoError(t, writeErr)
			}))
			t.Cleanup(server.Close)
			configFailure := errors.New("config failure")
			spec, status, err := resolveFields(t.Context(), func() (*rest.Config, error) {
				if scenario == "config error" {
					return nil, configFailure
				}
				if scenario == "client error" {
					return &rest.Config{Host: "://invalid"}, nil
				}

				return &rest.Config{Host: server.URL}, nil
			})
			if scenario == "live" || scenario == "no status" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if scenario == "config error" {
				require.ErrorIs(t, err, configFailure)
			}
			if scenario == "live" || scenario == "no status" {
				require.Equal(t, []Field{{Name: "liveSpec", Type: "string", Description: "Live spec description"}}, spec)
			} else {
				require.Equal(t, SpecFields(), spec)
			}
			if scenario == "live" || scenario == "no spec" {
				require.Equal(t, []Field{{Name: "liveStatus", Type: "string", Description: "Live status description"}}, status)
			} else {
				require.Equal(t, fieldsFromType(reflect.TypeFor[ledgerv1alpha1.ClusterStatus]()), status)
			}
			expectedRequests := int32(1)
			if scenario == "config error" || scenario == "client error" {
				expectedRequests = 0
			}
			require.Equal(t, expectedRequests, requests.Load())
		})
	}
}

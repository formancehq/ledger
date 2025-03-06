package provision

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
)

func createConfigMap(ctx *pulumi.Context, cmp *Component, args ComponentArgs) (*corev1.ConfigMap, error) {
	connectors := make(map[string]any)
	if args.Connectors != nil && args.Connectors.Connectors != nil {
		for connectorName, connectorComponent := range args.Connectors.Connectors {
			config, err := internals.UnsafeAwaitOutput(ctx.Context(), connectorComponent.Component.GetConfig())
			if err != nil {
				return nil, err
			}
			connectors[connectorName] = map[string]any{
				"driver": connectorComponent.Driver,
				"config": config.Value,
			}
		}
	}

	marshalledConfig, err := json.Marshal(struct {
		Ledgers    map[string]LedgerConfigArgs `json:"ledgers"`
		Connectors map[string]any              `json:"connectors"`
	}{
		Ledgers:    args.Ledgers,
		Connectors: connectors,
	})
	if err != nil {
		return nil, err
	}

	digest := sha256.New()
	_, err = digest.Write(marshalledConfig)
	if err != nil {
		return nil, err
	}

	return corev1.NewConfigMap(ctx, "provisioner", &corev1.ConfigMapArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Annotations: pulumi.StringMap{
				"config-hash": pulumi.String(fmt.Sprintf("%x", digest.Sum(nil))),
			},
		},
		Data: pulumi.StringMap{
			"config.yaml": pulumi.String(marshalledConfig),
		},
	}, pulumi.Parent(cmp))
}
